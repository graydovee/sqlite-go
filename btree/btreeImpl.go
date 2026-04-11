package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/sqlite-go/sqlite-go/pager"
)

const (
	DBHeaderSize = 100
)

func pageOffset(pageNum PageNumber) int {
	if pageNum == 1 {
		return DBHeaderSize
	}
	return 0
}

type BTreeImpl struct {
	pgr       pager.Pager
	pageSize  int
	pageCount int
}

type BTreeConnImpl struct {
	pgr pager.Pager
}

func OpenBTreeConn(pgr pager.Pager) BTreeConn {
	return &BTreeConnImpl{pgr: pgr}
}

func (c *BTreeConnImpl) Open(pgr pager.Pager) (BTree, error) {
	b := &BTreeImpl{
		pgr:      pgr,
		pageSize: pgr.PageSize(),
	}
	b.pageCount = pgr.PageCount()
	SetPageSize(b.pageSize)
	return b, nil
}

func (c *BTreeConnImpl) GetPager() pager.Pager { return c.pgr }

func (c *BTreeConnImpl) GetMeta(idx int) (int32, error) {
	if idx < 0 || idx > 15 {
		return 0, fmt.Errorf("meta index out of range: %d", idx)
	}
	page, err := c.pgr.GetPage(1)
	if err != nil {
		return 0, err
	}
	defer c.pgr.ReleasePage(page)
	offset := 40 + idx*4
	val := int32(binary.BigEndian.Uint32(page.Data[offset : offset+4]))
	return val, nil
}

func (c *BTreeConnImpl) SetMeta(idx int, value int32) error {
	if idx < 0 || idx > 15 {
		return fmt.Errorf("meta index out of range: %d", idx)
	}
	page, err := c.pgr.GetPage(1)
	if err != nil {
		return err
	}
	defer c.pgr.ReleasePage(page)
	offset := 40 + idx*4
	binary.BigEndian.PutUint32(page.Data[offset:offset+4], uint32(value))
	return c.pgr.MarkDirty(page)
}

func (c *BTreeConnImpl) SchemaVersion() (int32, error)  { return c.GetMeta(1) }
func (c *BTreeConnImpl) SetSchemaVersion(v int32) error { return c.SetMeta(1, v) }

func (b *BTreeImpl) Close() error                                        { return nil }
func (b *BTreeImpl) Begin(write bool) error                              { return b.pgr.Begin(write) }
func (b *BTreeImpl) Commit() error                                       { return b.pgr.Commit() }
func (b *BTreeImpl) Rollback() error                                     { return b.pgr.Rollback() }

func hdrSize(pageNum PageNumber) int {
	return pageOffset(pageNum) + 8
}

func hdrSizeInterior(pageNum PageNumber) int {
	return pageOffset(pageNum) + 12
}

func (b *BTreeImpl) CreateBTree(flags CreateFlags) (PageNumber, error) {
	page, err := b.pgr.GetNewPage()
	if err != nil {
		return 0, err
	}
	pageNum := page.PageNum
	pageType := PageTypeLeafTable
	if flags == CreateIndex {
		pageType = PageTypeLeafIndex
	}
	initPage(page.Data, pageNum, pageType, b.pageSize)
	b.pgr.MarkDirty(page)
	b.pgr.WritePage(page)
	b.pgr.ReleasePage(page)
	return pageNum, nil
}

func (b *BTreeImpl) Drop(rootPage PageNumber) error {
	return b.freePageRecursive(rootPage)
}

func (b *BTreeImpl) freePageRecursive(pageNum PageNumber) error {
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		return err
	}
	hdr := readPageHeader(page.Data, pageNum)
	b.pgr.ReleasePage(page)
	if isInteriorPage(hdr.pageType) {
		hs := hdrSizeInterior(pageNum)
		pg, err := b.pgr.GetPage(pageNum)
		if err != nil {
			return err
		}
		for i := 0; i < int(hdr.numCells); i++ {
			cellPtr := int(binary.BigEndian.Uint16(pg.Data[hs+i*2 : hs+i*2+2]))
			if cellPtr > 0 && cellPtr+4 <= len(pg.Data) {
				leftChild := PageNumber(binary.BigEndian.Uint32(pg.Data[cellPtr : cellPtr+4]))
				if leftChild > 0 {
					b.freePageRecursive(leftChild)
				}
			}
		}
		if hdr.rightChild > 0 {
			b.freePageRecursive(PageNumber(hdr.rightChild))
		}
		b.pgr.ReleasePage(pg)
	}
	return b.pgr.FreePage(pageNum)
}

func (b *BTreeImpl) Clear(rootPage PageNumber) error {
	_ = b.Drop(rootPage)
	page, err := b.pgr.GetPage(rootPage)
	if err != nil {
		return err
	}
	initPage(page.Data, rootPage, PageTypeLeafTable, b.pageSize)
	b.pgr.MarkDirty(page)
	b.pgr.WritePage(page)
	b.pgr.ReleasePage(page)
	return nil
}

func (b *BTreeImpl) Cursor(rootPage PageNumber, write bool) (BTCursor, error) {
	return &CursorImpl{
		bt:       b,
		rootPage: rootPage,
		write:    write,
	}, nil
}

type pageHeader struct {
	pageType   byte
	firstFree  uint16
	numCells   uint16
	contentOff uint16
	fragBytes  byte
	rightChild uint32
}

func readPageHeader(data []byte, pageNum PageNumber) pageHeader {
	off := pageOffset(pageNum)
	h := pageHeader{
		pageType:   data[off],
		firstFree:  binary.BigEndian.Uint16(data[off+1 : off+3]),
		numCells:   binary.BigEndian.Uint16(data[off+3 : off+5]),
		contentOff: binary.BigEndian.Uint16(data[off+5 : off+7]),
		fragBytes:  data[off+7],
	}
	if isInteriorPage(h.pageType) {
		h.rightChild = binary.BigEndian.Uint32(data[off+8 : off+12])
	}
	return h
}

// --- Insert with split support ---

func (b *BTreeImpl) Insert(cursor BTCursor, key []byte, data []byte, rowid RowID, seekResult SeekResult) error {
	cur := cursor.(*CursorImpl)

	if seekResult == SeekFound {
		if err := b.findLeafForInsert(cur, rowid); err != nil {
			return err
		}
		page, err := b.pgr.GetPage(cur.currentPage)
		if err != nil {
			return err
		}
		hdr := readPageHeader(page.Data, cur.currentPage)
		hs := hdrSize(cur.currentPage)
		for i := 0; i < int(hdr.numCells); i++ {
			ptr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
			if ptr == 0 || ptr >= len(page.Data) {
				continue
			}
			if readCellRowid(page.Data[ptr:], hdr.pageType) == int64(rowid) {
				b.deleteCellPtr(page, cur.currentPage, i)
				break
			}
		}
		b.pgr.MarkDirty(page)
		b.pgr.WritePage(page)
		b.pgr.ReleasePage(page)
		cur.currentPage = cur.rootPage
		cur.path = cur.path[:0]
	}

	if err := b.findLeafForInsert(cur, rowid); err != nil {
		return err
	}

	cell, err := b.buildCell(cur.currentPage, key, data, rowid)
	if err != nil {
		return err
	}

	err = b.insertCellIntoPage(cur.currentPage, cell)
	if err == nil {
		return nil
	}
	if err.Error() != "page full" {
		return err
	}
	return b.splitAndInsert(cur, cell, rowid)
}

func (b *BTreeImpl) findLeafForInsert(cur *CursorImpl, rowid RowID) error {
	cur.currentPage = cur.rootPage
	cur.path = cur.path[:0]
	for {
		page, err := b.pgr.GetPage(cur.currentPage)
		if err != nil {
			return err
		}
		hdr := readPageHeader(page.Data, cur.currentPage)
		b.pgr.ReleasePage(page)
		if !isInteriorPage(hdr.pageType) {
			return nil
		}
		childPage, childIdx, err := b.findChildForRowid(cur.currentPage, rowid)
		if err != nil {
			return err
		}
		cur.path = append(cur.path, pathEntry{pageNum: cur.currentPage, childIdx: childIdx})
		cur.currentPage = childPage
	}
}

func (b *BTreeImpl) findChildForRowid(pageNum PageNumber, rowid RowID) (PageNumber, int, error) {
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		return 0, 0, err
	}
	defer b.pgr.ReleasePage(page)
	hdr := readPageHeader(page.Data, pageNum)
	hs := hdrSizeInterior(pageNum)
	childIdx := int(hdr.numCells)
	childPage := PageNumber(hdr.rightChild)
	lo, hi := 0, int(hdr.numCells)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+mid*2 : hs+mid*2+2]))
		if cellPtr == 0 || cellPtr+4 >= len(page.Data) {
			break
		}
		cellRowid, _ := ReadVarint(page.Data[cellPtr+4:])
		if RowID(cellRowid) < rowid {
			lo = mid + 1
		} else {
			hi = mid - 1
			childIdx = mid
			childPage = PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
		}
	}
	return childPage, childIdx, nil
}

func (b *BTreeImpl) buildCell(pageNum PageNumber, key []byte, data []byte, rowid RowID) ([]byte, error) {
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		return nil, err
	}
	hdr := readPageHeader(page.Data, pageNum)
	b.pgr.ReleasePage(page)

	switch hdr.pageType {
	case PageTypeLeafTable:
		payload := data
		if payload == nil {
			payload = []byte{}
		}
		local := maxLocalSize(len(payload))
		if local > len(payload) {
			local = len(payload)
		}
		pl := VarintLen(int64(len(payload)))
		rl := VarintLen(int64(rowid))
		cellSize := pl + rl + local
		if len(payload) > local {
			cellSize += 4
		}
		buf := make([]byte, cellSize)
		pos := PutVarint(buf, int64(len(payload)))
		pos += PutVarint(buf[pos:], int64(rowid))
		copy(buf[pos:], payload[:local])
		pos += local
		if len(payload) > local {
			ovfl, err := b.writeOverflowPages(payload[local:])
			if err != nil {
				return nil, err
			}
			binary.BigEndian.PutUint32(buf[pos:], uint32(ovfl))
		}
		return buf, nil

	case PageTypeLeafIndex:
		payload := key
		if payload == nil {
			payload = []byte{}
		}
		local := maxLocalSize(len(payload))
		if local > len(payload) {
			local = len(payload)
		}
		pl := VarintLen(int64(len(payload)))
		cellSize := pl + local
		if len(payload) > local {
			cellSize += 4
		}
		buf := make([]byte, cellSize)
		pos := PutVarint(buf, int64(len(payload)))
		copy(buf[pos:], payload[:local])
		pos += local
		if len(payload) > local {
			ovfl, err := b.writeOverflowPages(payload[local:])
			if err != nil {
				return nil, err
			}
			binary.BigEndian.PutUint32(buf[pos:], uint32(ovfl))
		}
		return buf, nil

	default:
		return nil, fmt.Errorf("cannot build cell for page type %d", hdr.pageType)
	}
}

func (b *BTreeImpl) writeOverflowPages(payload []byte) (PageNumber, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	usable := pageSize - 4
	var firstPage, prevPage PageNumber
	offset := 0
	for offset < len(payload) {
		oPage, err := b.pgr.GetNewPage()
		if err != nil {
			return 0, err
		}
		pNum := oPage.PageNum
		toWrite := len(payload) - offset
		if toWrite > usable {
			toWrite = usable
		}
		copy(oPage.Data[4:], payload[offset:offset+toWrite])
		binary.BigEndian.PutUint32(oPage.Data[0:4], 0)
		if prevPage > 0 {
			prev, err := b.pgr.GetPage(prevPage)
			if err != nil {
				b.pgr.ReleasePage(oPage)
				return 0, err
			}
			binary.BigEndian.PutUint32(prev.Data[0:4], uint32(pNum))
			b.pgr.MarkDirty(prev)
			b.pgr.WritePage(prev)
			b.pgr.ReleasePage(prev)
		}
		if firstPage == 0 {
			firstPage = pNum
		}
		prevPage = pNum
		b.pgr.MarkDirty(oPage)
		b.pgr.WritePage(oPage)
		b.pgr.ReleasePage(oPage)
		offset += toWrite
	}
	return firstPage, nil
}

func (b *BTreeImpl) readOverflowPayload(localData []byte, overflowPage PageNumber, totalLen int) ([]byte, error) {
	result := make([]byte, totalLen)
	copy(result, localData)
	written := len(localData)
	cur := overflowPage
	for written < totalLen && cur > 0 {
		page, err := b.pgr.GetPage(cur)
		if err != nil {
			return nil, fmt.Errorf("read overflow page %d: %w", cur, err)
		}
		next := PageNumber(0)
		if len(page.Data) >= 4 {
			next = PageNumber(binary.BigEndian.Uint32(page.Data[0:4]))
		}
		usable := pageSize - 4
		remaining := totalLen - written
		toRead := remaining
		if toRead > usable {
			toRead = usable
		}
		if written+toRead <= len(result) && 4+toRead <= len(page.Data) {
			copy(result[written:], page.Data[4:4+toRead])
			written += toRead
		}
		b.pgr.ReleasePage(page)
		cur = next
	}
	return result, nil
}

func (b *BTreeImpl) insertCellIntoPage(pageNum PageNumber, cell []byte) error {
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		return err
	}
	defer b.pgr.ReleasePage(page)

	hdr := readPageHeader(page.Data, pageNum)
	numCells := int(hdr.numCells)
	isInt := isInteriorPage(hdr.pageType)
	hs := hdrSize(pageNum)
	if isInt {
		hs = hdrSizeInterior(pageNum)
	}

	cellContentStart := int(hdr.contentOff)
	if cellContentStart == 0 || cellContentStart > b.pageSize {
		cellContentStart = b.pageSize
	}

	cellPtrAreaEnd := hs + numCells*2
	newContentStart := cellContentStart - len(cell)

	if newContentStart < cellPtrAreaEnd+2 {
		return fmt.Errorf("page full")
	}

	insertIdx := numCells
	cellRowid := readCellRowid(cell, hdr.pageType)
	for i := 0; i < numCells; i++ {
		ptr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
		if ptr == 0 || ptr >= len(page.Data) {
			continue
		}
		if readCellRowid(page.Data[ptr:], hdr.pageType) >= cellRowid {
			insertIdx = i
			break
		}
	}

	copy(page.Data[newContentStart:], cell)
	for i := numCells; i > insertIdx; i-- {
		ptr := binary.BigEndian.Uint16(page.Data[hs+(i-1)*2 : hs+(i-1)*2+2])
		binary.BigEndian.PutUint16(page.Data[hs+i*2:], ptr)
	}
	binary.BigEndian.PutUint16(page.Data[hs+insertIdx*2:], uint16(newContentStart))

	off := pageOffset(pageNum)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(numCells+1))
	binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(newContentStart))

	b.pgr.MarkDirty(page)
	return b.pgr.WritePage(page)
}

func (b *BTreeImpl) splitAndInsert(cur *CursorImpl, newCell []byte, rowid RowID) error {
	page, err := b.pgr.GetPage(cur.currentPage)
	if err != nil {
		return err
	}
	hdr := readPageHeader(page.Data, cur.currentPage)
	b.pgr.ReleasePage(page)

	if len(cur.path) == 0 {
		return b.splitRoot(cur, newCell, hdr)
	}
	return b.splitNonRoot(cur, newCell, hdr)
}

func (b *BTreeImpl) splitRoot(cur *CursorImpl, newCell []byte, hdr pageHeader) error {
	leftPage, err := b.pgr.GetNewPage()
	if err != nil {
		return err
	}
	rightPage, err := b.pgr.GetNewPage()
	if err != nil {
		return err
	}
	leftNum := leftPage.PageNum
	rightNum := rightPage.PageNum

	rootPage, err := b.pgr.GetPage(cur.rootPage)
	if err != nil {
		return err
	}
	rootHdr := readPageHeader(rootPage.Data, cur.rootPage)

	allCells := make([][]byte, 0, int(rootHdr.numCells)+1)
	for i := 0; i < int(rootHdr.numCells); i++ {
		cd := extractCellData(rootPage.Data, cur.rootPage, i)
		if cd != nil {
			allCells = append(allCells, cd)
		}
	}
	b.pgr.ReleasePage(rootPage)

	newRowid := readCellRowid(newCell, rootHdr.pageType)
	inserted := false
	for i, c := range allCells {
		if readCellRowid(c, rootHdr.pageType) >= newRowid {
			allCells = append(allCells, nil)
			copy(allCells[i+1:], allCells[i:])
			allCells[i] = newCell
			inserted = true
			break
		}
	}
	if !inserted {
		allCells = append(allCells, newCell)
	}

	mid := b.findSplitPoint(allCells, rootHdr.pageType)
	leftCells := allCells[:mid]
	rightCells := allCells[mid:]

	initPage(leftPage.Data, leftNum, rootHdr.pageType, b.pageSize)
	b.writeCellsToPage(leftPage, leftNum, leftCells, rootHdr.pageType)
	b.pgr.MarkDirty(leftPage)
	b.pgr.WritePage(leftPage)
	b.pgr.ReleasePage(leftPage)

	initPage(rightPage.Data, rightNum, rootHdr.pageType, b.pageSize)
	b.writeCellsToPage(rightPage, rightNum, rightCells, rootHdr.pageType)
	b.pgr.MarkDirty(rightPage)
	b.pgr.WritePage(rightPage)
	b.pgr.ReleasePage(rightPage)

	rootPage, err = b.pgr.GetPage(cur.rootPage)
	if err != nil {
		return err
	}

	intType := PageTypeInteriorTable
	if rootHdr.pageType == PageTypeLeafIndex || rootHdr.pageType == PageTypeInteriorIndex {
		intType = PageTypeInteriorIndex
	}

	dividerRowid := int64(0)
	if len(leftCells) > 0 {
		dividerRowid = readCellRowid(leftCells[len(leftCells)-1], rootHdr.pageType)
	}

	rl := VarintLen(dividerRowid)
	intCell := make([]byte, 4+rl)
	binary.BigEndian.PutUint32(intCell[0:4], uint32(leftNum))
	PutVarint(intCell[4:], dividerRowid)

	initPage(rootPage.Data, cur.rootPage, intType, b.pageSize)
	hsInt := hdrSizeInterior(cur.rootPage)
	contentStart := b.pageSize - len(intCell)
	copy(rootPage.Data[contentStart:], intCell)
	binary.BigEndian.PutUint16(rootPage.Data[hsInt:], uint16(contentStart))

	off := pageOffset(cur.rootPage)
	binary.BigEndian.PutUint16(rootPage.Data[off+3:off+5], 1)
	binary.BigEndian.PutUint16(rootPage.Data[off+5:off+7], uint16(contentStart))
	binary.BigEndian.PutUint32(rootPage.Data[off+8:off+12], uint32(rightNum))

	b.pgr.MarkDirty(rootPage)
	b.pgr.WritePage(rootPage)
	b.pgr.ReleasePage(rootPage)

	if newRowid <= dividerRowid {
		cur.currentPage = leftNum
		cur.path = []pathEntry{{pageNum: cur.rootPage, childIdx: 0}}
	} else {
		cur.currentPage = rightNum
		cur.path = []pathEntry{{pageNum: cur.rootPage, childIdx: 1}}
	}
	return nil
}

func (b *BTreeImpl) splitNonRoot(cur *CursorImpl, newCell []byte, hdr pageHeader) error {
	page, err := b.pgr.GetPage(cur.currentPage)
	if err != nil {
		return err
	}
	allCells := make([][]byte, 0, int(hdr.numCells)+1)
	for i := 0; i < int(hdr.numCells); i++ {
		cd := extractCellData(page.Data, cur.currentPage, i)
		if cd != nil {
			allCells = append(allCells, cd)
		}
	}
	b.pgr.ReleasePage(page)

	newRowid := readCellRowid(newCell, hdr.pageType)
	inserted := false
	for i, c := range allCells {
		if readCellRowid(c, hdr.pageType) >= newRowid {
			allCells = append(allCells, nil)
			copy(allCells[i+1:], allCells[i:])
			allCells[i] = newCell
			inserted = true
			break
		}
	}
	if !inserted {
		allCells = append(allCells, newCell)
	}

	mid := b.findSplitPoint(allCells, hdr.pageType)
	leftCells := allCells[:mid]
	rightCells := allCells[mid:]

	p, _ := b.pgr.GetPage(cur.currentPage)
	initPage(p.Data, cur.currentPage, hdr.pageType, b.pageSize)
	b.pgr.MarkDirty(p)
	b.pgr.WritePage(p)
	b.pgr.ReleasePage(p)

	p, _ = b.pgr.GetPage(cur.currentPage)
	b.writeCellsToPage(p, cur.currentPage, leftCells, hdr.pageType)
	b.pgr.ReleasePage(p)

	sibPage, err := b.pgr.GetNewPage()
	if err != nil {
		return err
	}
	sibNum := sibPage.PageNum
	initPage(sibPage.Data, sibNum, hdr.pageType, b.pageSize)
	b.writeCellsToPage(sibPage, sibNum, rightCells, hdr.pageType)
	b.pgr.MarkDirty(sibPage)
	b.pgr.WritePage(sibPage)
	b.pgr.ReleasePage(sibPage)

	dividerRowid := readCellRowid(leftCells[len(leftCells)-1], hdr.pageType)

	parent := cur.path[len(cur.path)-1]
	cur.path = cur.path[:len(cur.path)-1]

	if err := b.insertDividerIntoParent(parent.pageNum, parent.childIdx, cur.currentPage, dividerRowid, sibNum); err != nil {
		return err
	}

	if newRowid > dividerRowid {
		cur.currentPage = sibNum
	}
	return nil
}

func (b *BTreeImpl) insertDividerIntoParent(parentPage PageNumber, childIdx int, leftChild PageNumber, dividerRowid int64, rightChild PageNumber) error {
	rl := VarintLen(dividerRowid)
	dividerCell := make([]byte, 4+rl)
	binary.BigEndian.PutUint32(dividerCell[0:4], uint32(leftChild))
	PutVarint(dividerCell[4:], dividerRowid)

	page, err := b.pgr.GetPage(parentPage)
	if err != nil {
		return err
	}
	hdr := readPageHeader(page.Data, parentPage)
	b.pgr.ReleasePage(page)

	numCells := int(hdr.numCells)
	hsInt := hdrSizeInterior(parentPage)
	cellContentStart := int(hdr.contentOff)
	if cellContentStart == 0 || cellContentStart > b.pageSize {
		cellContentStart = b.pageSize
	}

	newContentStart := cellContentStart - len(dividerCell)
	if newContentStart < hsInt+numCells*2+2 {
		return b.splitInteriorPage(parentPage, childIdx, dividerCell, rightChild)
	}

	page, err = b.pgr.GetPage(parentPage)
	if err != nil {
		return err
	}
	defer b.pgr.ReleasePage(page)

	copy(page.Data[newContentStart:], dividerCell)
	for i := numCells; i > childIdx; i-- {
		ptr := binary.BigEndian.Uint16(page.Data[hsInt+(i-1)*2 : hsInt+(i-1)*2+2])
		binary.BigEndian.PutUint16(page.Data[hsInt+i*2:], ptr)
	}
	binary.BigEndian.PutUint16(page.Data[hsInt+childIdx*2:], uint16(newContentStart))

	off := pageOffset(parentPage)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(numCells+1))
	binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(newContentStart))
	if childIdx >= numCells {
		binary.BigEndian.PutUint32(page.Data[off+8:off+12], uint32(rightChild))
	} else {
		// The cell that was at childIdx is now at childIdx+1 (shifted right).
		// Its leftChild still points to the old child; update it to the sibling.
		nextPtr := int(binary.BigEndian.Uint16(page.Data[hsInt+(childIdx+1)*2 : hsInt+(childIdx+1)*2+2]))
		if nextPtr > 0 && nextPtr+4 <= len(page.Data) {
			binary.BigEndian.PutUint32(page.Data[nextPtr:nextPtr+4], uint32(rightChild))
		}
	}

	b.pgr.MarkDirty(page)
	return b.pgr.WritePage(page)
}

func (b *BTreeImpl) splitInteriorPage(parentPage PageNumber, newChildIdx int, newDividerCell []byte, newRightChild PageNumber) error {
	page, err := b.pgr.GetPage(parentPage)
	if err != nil {
		return err
	}
	hdr := readPageHeader(page.Data, parentPage)
	_ = hdrSizeInterior(parentPage)

	allCells := make([][]byte, 0, int(hdr.numCells)+1)
	for i := 0; i < int(hdr.numCells); i++ {
		cd := extractCellData(page.Data, parentPage, i)
		if cd != nil {
			allCells = append(allCells, cd)
		}
	}
	oldRightChild := hdr.rightChild
	b.pgr.ReleasePage(page)

	if newChildIdx <= len(allCells) {
		allCells = append(allCells, nil)
		copy(allCells[newChildIdx+1:], allCells[newChildIdx:])
		allCells[newChildIdx] = newDividerCell
	} else {
		allCells = append(allCells, newDividerCell)
	}

	// The cell that was at newChildIdx is now at newChildIdx+1 (shifted right).
	// Its leftChild still points to the old child page; update it to the sibling.
	if newChildIdx < len(allCells)-1 {
		oldCell := allCells[newChildIdx+1]
		if len(oldCell) >= 4 {
			binary.BigEndian.PutUint32(oldCell[0:4], uint32(newRightChild))
		}
	}

	mid := b.findSplitPoint(allCells, PageTypeInteriorTable)
	leftCells := allCells[:mid]
	rightCells := allCells[mid:]
	dividerRowid := readCellRowid(leftCells[len(leftCells)-1], PageTypeInteriorTable)

	p, _ := b.pgr.GetPage(parentPage)
	initPage(p.Data, parentPage, PageTypeInteriorTable, b.pageSize)
	b.writeCellsToPage(p, parentPage, leftCells, PageTypeInteriorTable)
	off := pageOffset(parentPage)
	binary.BigEndian.PutUint32(p.Data[off+8:off+12], uint32(oldRightChild))
	b.pgr.MarkDirty(p)
	b.pgr.WritePage(p)
	b.pgr.ReleasePage(p)

	sibPage, err := b.pgr.GetNewPage()
	if err != nil {
		return err
	}
	sibNum := sibPage.PageNum
	initPage(sibPage.Data, sibNum, PageTypeInteriorTable, b.pageSize)
	off = pageOffset(sibNum)
	binary.BigEndian.PutUint32(sibPage.Data[off+8:off+12], uint32(newRightChild))
	b.writeCellsToPage(sibPage, sibNum, rightCells, PageTypeInteriorTable)
	b.pgr.MarkDirty(sibPage)
	b.pgr.WritePage(sibPage)
	b.pgr.ReleasePage(sibPage)

	newRoot, err := b.pgr.GetNewPage()
	if err != nil {
		return err
	}
	newRootNum := newRoot.PageNum
	initPage(newRoot.Data, newRootNum, PageTypeInteriorTable, b.pageSize)

	rl := VarintLen(dividerRowid)
	dividerCell := make([]byte, 4+rl)
	binary.BigEndian.PutUint32(dividerCell[0:4], uint32(parentPage))
	PutVarint(dividerCell[4:], dividerRowid)

	hsNew := hdrSizeInterior(newRootNum)
	contentStart := b.pageSize - len(dividerCell)
	copy(newRoot.Data[contentStart:], dividerCell)
	binary.BigEndian.PutUint16(newRoot.Data[hsNew:], uint16(contentStart))

	off = pageOffset(newRootNum)
	binary.BigEndian.PutUint16(newRoot.Data[off+3:off+5], 1)
	binary.BigEndian.PutUint16(newRoot.Data[off+5:off+7], uint16(contentStart))
	binary.BigEndian.PutUint32(newRoot.Data[off+8:off+12], uint32(sibNum))

	b.pgr.MarkDirty(newRoot)
	b.pgr.WritePage(newRoot)
	b.pgr.ReleasePage(newRoot)
	return nil
}

func (b *BTreeImpl) writeCellsToPage(page *pager.Page, pageNum PageNumber, cells [][]byte, pageType byte) error {
	if page == nil {
		var err error
		page, err = b.pgr.GetPage(pageNum)
		if err != nil {
			return err
		}
		defer b.pgr.ReleasePage(page)
	}
	hs := hdrSize(pageNum)
	if isInteriorPage(pageType) {
		hs = hdrSizeInterior(pageNum)
	}
	totalCellSize := 0
	for _, c := range cells {
		totalCellSize += len(c)
	}
	cellPtrArea := len(cells) * 2
	if totalCellSize+cellPtrArea > b.pageSize-hs {
		return fmt.Errorf("writeCellsToPage: %d bytes of cells exceed available space (%d)", totalCellSize, b.pageSize-hs-cellPtrArea)
	}
	contentStart := b.pageSize
	for i := len(cells) - 1; i >= 0; i-- {
		contentStart -= len(cells[i])
		copy(page.Data[contentStart:], cells[i])
	}
	for i := range cells {
		cellOff := contentStart
		for j := 0; j < i; j++ {
			cellOff += len(cells[j])
		}
		binary.BigEndian.PutUint16(page.Data[hs+i*2:], uint16(cellOff))
	}
	off := pageOffset(pageNum)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(len(cells)))
	if len(cells) > 0 {
		binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(contentStart))
	} else {
		binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(b.pageSize))
	}
	b.pgr.MarkDirty(page)
	return b.pgr.WritePage(page)
}

// findSplitPoint chooses a split index within allCells such that both
// allCells[:mid] and allCells[mid:] fit on a fresh page of the given type.
// It starts near the size-weighted median and adjusts until both halves fit.
func (b *BTreeImpl) findSplitPoint(allCells [][]byte, pageType byte) int {
	if len(allCells) <= 1 {
		return len(allCells)
	}

	hs := 8
	if isInteriorPage(pageType) {
		hs = 12
	}

	totalSize := 0
	for _, c := range allCells {
		totalSize += len(c)
	}

	// Start at the size-weighted median.
	half := totalSize / 2
	cumulative := 0
	mid := 1
	for i, c := range allCells {
		cumulative += len(c)
		if cumulative >= half {
			mid = i + 1
			break
		}
	}
	if mid >= len(allCells) {
		mid = len(allCells) - 1
	}

	// Shrink left side until it fits on a page.
	leftSize := 0
	for i := 0; i < mid; i++ {
		leftSize += len(allCells[i])
	}
	for mid > 1 {
		avail := b.pageSize - hs - mid*2
		if leftSize <= avail {
			break
		}
		mid--
		leftSize -= len(allCells[mid])
	}

	// Grow left side until right side fits on a page.
	rightSize := totalSize - leftSize
	rightCount := len(allCells) - mid
	for mid < len(allCells)-1 {
		avail := b.pageSize - hs - rightCount*2
		if rightSize <= avail {
			break
		}
		leftSize += len(allCells[mid])
		mid++
		rightSize = totalSize - leftSize
		rightCount = len(allCells) - mid
	}

	return mid
}

// --- Delete ---

func (b *BTreeImpl) Delete(cursor BTCursor) error {
	cur := cursor.(*CursorImpl)
	if !cur.valid {
		return fmt.Errorf("cursor not positioned")
	}
	page, err := b.pgr.GetPage(cur.currentPage)
	if err != nil {
		return err
	}
	defer b.pgr.ReleasePage(page)

	hdr := readPageHeader(page.Data, cur.currentPage)
	numCells := int(hdr.numCells)
	if cur.cellIndex < 0 || cur.cellIndex >= numCells {
		return fmt.Errorf("cell index out of range: %d", cur.cellIndex)
	}
	b.deleteCellPtr(page, cur.currentPage, cur.cellIndex)
	b.pgr.MarkDirty(page)
	cur.valid = false
	return b.pgr.WritePage(page)
}

func (b *BTreeImpl) deleteCellPtr(page *pager.Page, pageNum PageNumber, cellIndex int) {
	hdr := readPageHeader(page.Data, pageNum)
	numCells := int(hdr.numCells)
	hs := hdrSize(pageNum)
	if isInteriorPage(hdr.pageType) {
		hs = hdrSizeInterior(pageNum)
	}
	if cellIndex < 0 || cellIndex >= numCells {
		return
	}
	for i := cellIndex; i < numCells-1; i++ {
		ptr := binary.BigEndian.Uint16(page.Data[hs+(i+1)*2 : hs+(i+1)*2+2])
		binary.BigEndian.PutUint16(page.Data[hs+i*2:], ptr)
	}
	off := pageOffset(pageNum)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(numCells-1))
}

// --- Count ---

func (b *BTreeImpl) Count(rootPage PageNumber) (int64, error) {
	return b.countPages(rootPage)
}

func (b *BTreeImpl) countPages(pageNum PageNumber) (int64, error) {
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		return 0, err
	}
	defer b.pgr.ReleasePage(page)
	hdr := readPageHeader(page.Data, pageNum)
	if isInteriorPage(hdr.pageType) {
		hs := hdrSizeInterior(pageNum)
		var total int64
		for i := 0; i < int(hdr.numCells); i++ {
			cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
			if cellPtr == 0 || cellPtr >= b.pageSize {
				continue
			}
			leftChild := PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
			if leftChild > 0 {
				cnt, err := b.countPages(leftChild)
				if err != nil {
					return 0, err
				}
				total += cnt
			}
		}
		if hdr.rightChild > 0 {
			cnt, err := b.countPages(PageNumber(hdr.rightChild))
			if err != nil {
				return 0, err
			}
			total += cnt
		}
		return total, nil
	}
	return int64(hdr.numCells), nil
}

func (b *BTreeImpl) IntegrityCheck(rootPage PageNumber, depth int, errDest *[]string) {
	b.integrityCheckPage(rootPage, 0, errDest)
}

func (b *BTreeImpl) integrityCheckPage(pageNum PageNumber, depth int, errDest *[]string) {
	if depth > 100 {
		*errDest = append(*errDest, fmt.Sprintf("page %d: tree too deep", pageNum))
		return
	}
	page, err := b.pgr.GetPage(pageNum)
	if err != nil {
		*errDest = append(*errDest, fmt.Sprintf("page %d: %v", pageNum, err))
		return
	}
	defer b.pgr.ReleasePage(page)

	hdr := readPageHeader(page.Data, pageNum)
	hs := hdrSize(pageNum)
	if isInteriorPage(hdr.pageType) {
		hs = hdrSizeInterior(pageNum)
	}

	var lastRowid int64
	for i := 0; i < int(hdr.numCells); i++ {
		cellPtr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
		if cellPtr == 0 || cellPtr >= b.pageSize {
			*errDest = append(*errDest, fmt.Sprintf("page %d cell %d: invalid ptr %d", pageNum, i, cellPtr))
			continue
		}
		if hdr.pageType == PageTypeLeafTable || hdr.pageType == PageTypeInteriorTable {
			rowid := readCellRowid(page.Data[cellPtr:], hdr.pageType)
			if i > 0 && rowid <= lastRowid {
				*errDest = append(*errDest, fmt.Sprintf("page %d: rowids out of order at cell %d (%d <= %d)", pageNum, i, rowid, lastRowid))
			}
			lastRowid = rowid
		}
		if isInteriorPage(hdr.pageType) && cellPtr+4 <= len(page.Data) {
			leftChild := PageNumber(binary.BigEndian.Uint32(page.Data[cellPtr : cellPtr+4]))
			if leftChild > 0 {
				b.integrityCheckPage(leftChild, depth+1, errDest)
			}
		}
	}
	if isInteriorPage(hdr.pageType) && hdr.rightChild > 0 {
		b.integrityCheckPage(PageNumber(hdr.rightChild), depth+1, errDest)
	}
}

func (b *BTreeImpl) PageCount() int { return b.pgr.PageCount() }
