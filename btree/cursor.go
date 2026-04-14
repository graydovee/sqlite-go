package btree

import (
	"encoding/binary"
	"fmt"
)

type pathEntry struct {
	pageNum  PageNumber
	childIdx int
}

type CursorImpl struct {
	bt          *BTreeImpl
	rootPage    PageNumber
	currentPage PageNumber
	cellIndex   int
	valid       bool
	write       bool
	key         []byte
	data        []byte
	rowid       RowID
	path        []pathEntry
}

func (c *CursorImpl) Close() error {
	c.valid = false
	c.path = c.path[:0]
	return nil
}

func (c *CursorImpl) First() (bool, error) {
	c.currentPage = c.rootPage
	c.path = c.path[:0]
	c.cellIndex = 0
	return c.descendToLeftmost()
}

func (c *CursorImpl) descendToLeftmost() (bool, error) {
	for {
		page, err := c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return false, err
		}
		hdr := readPageHeader(page.Data, c.currentPage)
		if !isInteriorPage(hdr.pageType) {
			c.bt.pgr.ReleasePage(page)
			c.cellIndex = 0
			c.valid = int(hdr.numCells) > 0
			if c.valid {
				return c.readCurrentCell()
			}
			return false, nil
		}
		hs := hdrSizeInterior(c.currentPage)
		leftChild := PageNumber(0)
		if hdr.numCells > 0 {
			cellPtr := int(binary.BigEndian.Uint16(page.Data[hs : hs+2]))
			if cellPtr > 0 && cellPtr+4 <= len(page.Data) {
				leftChild = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
			}
		}
		if leftChild == 0 && hdr.rightChild > 0 {
			leftChild = PageNumber(hdr.rightChild)
		}
		c.bt.pgr.ReleasePage(page)
		if leftChild == 0 {
			c.valid = false
			return false, fmt.Errorf("no left child in interior page %d", c.currentPage)
		}
		c.path = append(c.path, pathEntry{pageNum: c.currentPage, childIdx: 0})
		c.currentPage = leftChild
	}
}

func (c *CursorImpl) Last() (bool, error) {
	c.currentPage = c.rootPage
	c.path = c.path[:0]
	return c.descendToRightmost()
}

func (c *CursorImpl) descendToRightmost() (bool, error) {
	for {
		page, err := c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return false, err
		}
		hdr := readPageHeader(page.Data, c.currentPage)
		if !isInteriorPage(hdr.pageType) {
			c.cellIndex = int(hdr.numCells) - 1
			c.bt.pgr.ReleasePage(page)
			c.valid = c.cellIndex >= 0
			if c.valid {
				return c.readCurrentCell()
			}
			return false, nil
		}
		off := pageOffset(c.currentPage)
		rightChild := PageNumber(binary.BigEndian.Uint32(page.Data[off+8 : off+12]))
		c.bt.pgr.ReleasePage(page)
		if rightChild == 0 {
			c.valid = false
			return false, nil
		}
		c.path = append(c.path, pathEntry{pageNum: c.currentPage, childIdx: int(hdr.numCells)})
		c.currentPage = rightChild
	}
}

func (c *CursorImpl) Next() (bool, error) {
	if !c.valid {
		return false, nil
	}
	c.cellIndex++
	page, err := c.bt.pgr.GetPage(c.currentPage)
	if err != nil {
		return false, err
	}
	hdr := readPageHeader(page.Data, c.currentPage)
	c.bt.pgr.ReleasePage(page)
	if c.cellIndex < int(hdr.numCells) {
		return c.readCurrentCell()
	}
	return c.moveToNextLeaf()
}

func (c *CursorImpl) moveToNextLeaf() (bool, error) {
	for len(c.path) > 0 {
		parent := c.path[len(c.path)-1]
		page, err := c.bt.pgr.GetPage(parent.pageNum)
		if err != nil {
			return false, err
		}
		hdr := readPageHeader(page.Data, parent.pageNum)
		numCells := int(hdr.numCells)
		hs := hdrSizeInterior(parent.pageNum)
		nextChildIdx := parent.childIdx + 1
		var nextChild PageNumber
		if nextChildIdx < numCells {
			cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+nextChildIdx*2 : hs+nextChildIdx*2+2]))
			if cellPtr > 0 && cellPtr+4 <= len(page.Data) {
				nextChild = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
			}
		} else if nextChildIdx == numCells {
			off := pageOffset(parent.pageNum)
			nextChild = PageNumber(binary.BigEndian.Uint32(page.Data[off+8 : off+12]))
		}
		c.bt.pgr.ReleasePage(page)
		c.path[len(c.path)-1].childIdx = nextChildIdx
		if nextChild > 0 {
			c.currentPage = nextChild
			hasRow, err := c.descendToLeftmost()
			if err != nil {
				return false, err
			}
			if hasRow {
				return true, nil
			}
			// Empty leaf page — continue to next sibling
			continue
		}
		c.path = c.path[:len(c.path)-1]
	}
	c.valid = false
	return false, nil
}

func (c *CursorImpl) Prev() (bool, error) {
	if !c.valid {
		return false, nil
	}
	c.cellIndex--
	if c.cellIndex >= 0 {
		return c.readCurrentCell()
	}
	return c.moveToPrevLeaf()
}

func (c *CursorImpl) moveToPrevLeaf() (bool, error) {
	for len(c.path) > 0 {
		parent := c.path[len(c.path)-1]
		prevChildIdx := parent.childIdx - 1
		if prevChildIdx < 0 {
			c.path = c.path[:len(c.path)-1]
			continue
		}
		page, err := c.bt.pgr.GetPage(parent.pageNum)
		if err != nil {
			return false, err
		}
		hs := hdrSizeInterior(parent.pageNum)
		var prevChild PageNumber
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+prevChildIdx*2 : hs+prevChildIdx*2+2]))
		if cellPtr > 0 && cellPtr+4 <= len(page.Data) {
			prevChild = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
		}
		c.bt.pgr.ReleasePage(page)
		c.path[len(c.path)-1].childIdx = prevChildIdx
		if prevChild > 0 {
			c.currentPage = prevChild
			hasRow, err := c.descendToRightmost()
			if err != nil {
				return false, err
			}
			if hasRow {
				return true, nil
			}
			// Empty leaf page — continue to previous sibling
			continue
		}
		c.path = c.path[:len(c.path)-1]
	}
	c.valid = false
	return false, nil
}

func (c *CursorImpl) Seek(key []byte) (SeekResult, error) {
	c.currentPage = c.rootPage
	c.path = c.path[:0]
	return c.seekInPage(key)
}

func (c *CursorImpl) seekInPage(target []byte) (SeekResult, error) {
	page, err := c.bt.pgr.GetPage(c.currentPage)
	if err != nil {
		return SeekInvalid, err
	}
	defer c.bt.pgr.ReleasePage(page)
	hdr := readPageHeader(page.Data, c.currentPage)
	hs := hdrSize(c.currentPage)

	if isInteriorPage(hdr.pageType) {
		hsInt := hdrSizeInterior(c.currentPage)
		childIdx := int(hdr.numCells)
		childPage := PageNumber(hdr.rightChild)
		for i := 0; i < int(hdr.numCells); i++ {
			cellPtr := int(binary.BigEndian.Uint16(page.Data[hsInt+i*2 : hsInt+i*2+2]))
			if cellPtr == 0 || cellPtr+4 >= len(page.Data) {
				continue
			}
			pos := cellPtr + 4
			if hdr.pageType == PageTypeInteriorIndex {
				payloadLen, n := ReadVarint(page.Data[pos:])
				endPos := pos + n + int(payloadLen)
				if endPos > len(page.Data) {
					endPos = len(page.Data)
				}
				c.key = page.Data[pos+n : endPos]
			}
			cmp := compareKeys(c.key, target)
			if cmp >= 0 {
				childIdx = i
				childPage = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
				break
			}
		}
		c.path = append(c.path, pathEntry{pageNum: c.currentPage, childIdx: childIdx})
		c.currentPage = childPage
		return c.seekInPage(target)
	}

	for i := 0; i < int(hdr.numCells); i++ {
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
		if cellPtr == 0 || cellPtr >= c.bt.pageSize {
			continue
		}
		pos := cellPtr
		if hdr.pageType == PageTypeLeafTable {
			_, n := ReadVarint(page.Data[pos:])
			pos += n
			rowid, _ := ReadVarint(page.Data[pos:])
			c.rowid = RowID(rowid)
			c.key = nil
		} else if hdr.pageType == PageTypeLeafIndex {
			payloadLen, n := ReadVarint(page.Data[pos:])
			pos += n
			endPos := pos + int(payloadLen)
			if endPos > len(page.Data) {
				endPos = len(page.Data)
			}
			c.key = page.Data[pos:endPos]
		}
		cmp := compareKeys(c.key, target)
		if cmp == 0 {
			c.cellIndex = i
			c.valid = true
			c.readCurrentCell()
			return SeekFound, nil
		}
		if cmp > 0 {
			c.cellIndex = i
			c.valid = true
			c.readCurrentCell()
			return SeekNotFound, nil
		}
	}
	c.cellIndex = int(hdr.numCells) - 1
	c.valid = int(hdr.numCells) > 0
	if c.valid {
		c.readCurrentCell()
	}
	return SeekNotFound, nil
}

func (c *CursorImpl) SeekRowid(rowid RowID) (SeekResult, error) {
	c.currentPage = c.rootPage
	c.path = c.path[:0]
	return c.seekRowidInPage(rowid)
}

func (c *CursorImpl) seekRowidInPage(target RowID) (SeekResult, error) {
	page, err := c.bt.pgr.GetPage(c.currentPage)
	if err != nil {
		return SeekInvalid, err
	}
	defer c.bt.pgr.ReleasePage(page)
	hdr := readPageHeader(page.Data, c.currentPage)
	hs := hdrSize(c.currentPage)

	if hdr.pageType == PageTypeInteriorTable {
		hsInt := hdrSizeInterior(c.currentPage)
		childIdx := int(hdr.numCells)
		childPage := PageNumber(hdr.rightChild)
		lo, hi := 0, int(hdr.numCells)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			cellPtr := int(binary.BigEndian.Uint16(page.Data[hsInt+mid*2 : hsInt+mid*2+2]))
			if cellPtr == 0 || cellPtr+4 >= len(page.Data) {
				break
			}
			cellRowid, _ := ReadVarint(page.Data[cellPtr+4:])
			if RowID(cellRowid) < target {
				lo = mid + 1
			} else {
				hi = mid - 1
				childIdx = mid
				childPage = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
			}
		}
		c.path = append(c.path, pathEntry{pageNum: c.currentPage, childIdx: childIdx})
		c.currentPage = childPage
		return c.seekRowidInPage(target)
	}

	if hdr.pageType != PageTypeLeafTable {
		c.valid = false
		return SeekNotFound, nil
	}

	for i := 0; i < int(hdr.numCells); i++ {
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
		if cellPtr == 0 || cellPtr >= c.bt.pageSize {
			continue
		}
		pos := cellPtr
		_, n := ReadVarint(page.Data[pos:])
		pos += n
		rowid, _ := ReadVarint(page.Data[pos:])
		if RowID(rowid) == target {
			c.cellIndex = i
			c.rowid = target
			c.valid = true
			c.readCurrentCell()
			return SeekFound, nil
		}
		if RowID(rowid) > target {
			c.cellIndex = i
			c.valid = true
			c.readCurrentCell()
			return SeekNotFound, nil
		}
	}
	c.valid = false
	return SeekNotFound, nil
}

func (c *CursorImpl) SeekNear(key []byte) (SeekResult, error) {
	return c.Seek(key)
}

func (c *CursorImpl) Key() []byte          { return c.key }
func (c *CursorImpl) Data() ([]byte, error) { return c.data, nil }
func (c *CursorImpl) RowID() RowID          { return c.rowid }
func (c *CursorImpl) IsValid() bool         { return c.valid }
func (c *CursorImpl) SetRowID(rowid RowID) error {
	c.rowid = rowid
	return nil
}

func (c *CursorImpl) readCurrentCell() (bool, error) {
	page, err := c.bt.pgr.GetPage(c.currentPage)
	if err != nil {
		return false, err
	}
	defer c.bt.pgr.ReleasePage(page)
	hdr := readPageHeader(page.Data, c.currentPage)
	if c.cellIndex < 0 || c.cellIndex >= int(hdr.numCells) {
		c.valid = false
		return false, nil
	}
	hs := hdrSize(c.currentPage)
	if isInteriorPage(hdr.pageType) {
		hs = hdrSizeInterior(c.currentPage)
	}
	cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+c.cellIndex*2 : hs+c.cellIndex*2+2]))
	if cellPtr == 0 || cellPtr >= len(page.Data) {
		c.valid = false
		return false, nil
	}
	pos := cellPtr
	switch hdr.pageType {
	case PageTypeLeafTable:
		payloadLen, n := ReadVarint(page.Data[pos:])
		pos += n
		rowid, n := ReadVarint(page.Data[pos:])
		pos += n
		c.rowid = RowID(rowid)
		local := maxLocalSize(int(payloadLen))
		if local > int(payloadLen) {
			local = int(payloadLen)
		}
		endPos := pos + local
		if endPos > len(page.Data) {
			endPos = len(page.Data)
		}
		if int(payloadLen) <= local {
			c.data = make([]byte, int(payloadLen))
			copy(c.data, page.Data[pos:endPos])
		} else {
			ovflPos := pos + local
			ovflPage := PageNumber(0)
			if ovflPos+4 <= len(page.Data) {
				ovflPage = PageNumber(binary.BigEndian.Uint32(page.Data[ovflPos : ovflPos+4]))
			}
			c.data, err = c.bt.readOverflowPayload(page.Data[pos:endPos], ovflPage, int(payloadLen))
			if err != nil {
				return false, err
			}
		}
		c.key = nil
	case PageTypeInteriorTable:
		if pos+4 <= len(page.Data) {
			rid, _ := ReadVarint(page.Data[pos+4:])
			c.rowid = RowID(rid)
		}
		c.data = nil
		c.key = nil
	case PageTypeLeafIndex:
		payloadLen, n := ReadVarint(page.Data[pos:])
		pos += n
		local := maxLocalSize(int(payloadLen))
		if local > int(payloadLen) {
			local = int(payloadLen)
		}
		endPos := pos + local
		if endPos > len(page.Data) {
			endPos = len(page.Data)
		}
		if int(payloadLen) <= local {
			c.key = make([]byte, int(payloadLen))
			copy(c.key, page.Data[pos:endPos])
		} else {
			ovflPos := pos + local
			ovflPage := PageNumber(0)
			if ovflPos+4 <= len(page.Data) {
				ovflPage = PageNumber(binary.BigEndian.Uint32(page.Data[ovflPos : ovflPos+4]))
			}
			c.key, err = c.bt.readOverflowPayload(page.Data[pos:endPos], ovflPage, int(payloadLen))
			if err != nil {
				return false, err
			}
		}
		c.data = nil
	default:
		c.valid = false
		return false, nil
	}
	c.valid = true
	return true, nil
}

func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// RestoreAfterDelete fixes cursor state after a delete operation.
// After deleting the current cell, cells shift down so the next cell
// is now at the current index. We restore validity and decrement
// cellIndex so that Next() will re-read the correct cell.
func (c *CursorImpl) RestoreAfterDelete() {
	c.valid = true
	if c.cellIndex > 0 {
		c.cellIndex--
	} else {
		c.cellIndex = -1
	}
}
