package btree

import (
	"encoding/binary"
	"fmt"
)

// CursorImpl implements the BTCursor interface.
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
}

func (c *CursorImpl) Close() error {
	c.valid = false
	return nil
}

func (c *CursorImpl) First() (bool, error) {
	c.currentPage = c.rootPage
	c.cellIndex = 0
	return c.moveToFirst()
}

func (c *CursorImpl) moveToLeftmostLeaf() error {
	for {
		page, err := c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return err
		}
		hdr := readPageHeader(page.Data, c.currentPage)
		c.bt.pgr.ReleasePage(page)

		if hdr.pageType == PageTypeLeafTable || hdr.pageType == PageTypeLeafIndex {
			return nil
		}

		// Interior page: follow leftmost child
		hs := hdrSize(c.currentPage)
		page, err = c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return err
		}
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs : hs+2]))
		if cellPtr == 0 || cellPtr+4 > len(page.Data) {
			c.bt.pgr.ReleasePage(page)
			return fmt.Errorf("invalid cell pointer in interior page")
		}
		leftChild := PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
		c.bt.pgr.ReleasePage(page)
		c.currentPage = leftChild
	}
}

func (c *CursorImpl) moveToFirst() (bool, error) {
	if err := c.moveToLeftmostLeaf(); err != nil {
		return false, err
	}
	c.cellIndex = 0
	c.valid = true
	return c.readCurrentCell()
}

func (c *CursorImpl) Last() (bool, error) {
	c.currentPage = c.rootPage
	return c.moveToLast()
}

func (c *CursorImpl) moveToLast() (bool, error) {
	for {
		page, err := c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return false, err
		}
		hdr := readPageHeader(page.Data, c.currentPage)
		c.bt.pgr.ReleasePage(page)

		if hdr.pageType == PageTypeLeafTable || hdr.pageType == PageTypeLeafIndex {
			c.cellIndex = int(hdr.numCells) - 1
			c.valid = c.cellIndex >= 0
			if c.valid {
				return c.readCurrentCell()
			}
			return false, nil
		}

		// Interior: follow rightmost child
		off := pageOffset(c.currentPage)
		page, err = c.bt.pgr.GetPage(c.currentPage)
		if err != nil {
			return false, err
		}
		rightChild := PageNumber(binary.BigEndian.Uint32(page.Data[off+8 : off+12]))
		c.bt.pgr.ReleasePage(page)
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
	c.valid = false
	return false, nil
}

func (c *CursorImpl) Seek(key []byte) (SeekResult, error) {
	c.currentPage = c.rootPage
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

	for i := 0; i < int(hdr.numCells); i++ {
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
		if cellPtr == 0 || cellPtr >= c.bt.pageSize {
			continue
		}

		pos := cellPtr
		if hdr.pageType == PageTypeLeafTable {
			_, n := ReadVarint(page.Data[pos:]) // payload size
			pos += n
			rowid, _ := ReadVarint(page.Data[pos:])
			c.rowid = RowID(rowid)
			c.key = nil
		} else if hdr.pageType == PageTypeLeafIndex || hdr.pageType == PageTypeInteriorIndex {
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
	}
	c.valid = false
	return SeekNotFound, nil
}

func (c *CursorImpl) SeekNear(key []byte) (SeekResult, error) {
	return c.Seek(key)
}

func (c *CursorImpl) Key() []byte         { return c.key }
func (c *CursorImpl) Data() ([]byte, error) { return c.data, nil }
func (c *CursorImpl) RowID() RowID         { return c.rowid }
func (c *CursorImpl) IsValid() bool        { return c.valid }
func (c *CursorImpl) SetRowID(rowid RowID) error {
	c.rowid = rowid
	return nil
}

// readCurrentCell reads the cell data at the current cursor position.
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
		endPos := pos + int(payloadLen)
		if endPos > len(page.Data) {
			endPos = len(page.Data)
		}
		c.data = page.Data[pos:endPos]
		c.key = nil

	case PageTypeLeafIndex:
		payloadLen, n := ReadVarint(page.Data[pos:])
		pos += n
		endPos := pos + int(payloadLen)
		if endPos > len(page.Data) {
			endPos = len(page.Data)
		}
		c.key = page.Data[pos:endPos]
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
