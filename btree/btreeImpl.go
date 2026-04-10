package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/sqlite-go/sqlite-go/pager"
)

// Page header offsets for the B-Tree page header.
// For page 1, the B-Tree header starts at offset 100 (after DB header).
const (
	DBHeaderSize = 100 // SQLite database file header size (page 1 only)
)

// pageOffset returns the offset of the B-Tree header within the page.
// Page 1 has a 100-byte database file header before the B-Tree header.
func pageOffset(pageNum PageNumber) int {
	if pageNum == 1 {
		return DBHeaderSize
	}
	return 0
}

// BTreeImpl implements the BTree interface.
type BTreeImpl struct {
	pgr       pager.Pager
	pageSize  int
	pageCount int
}

// BTreeConnImpl implements the BTreeConn interface.
type BTreeConnImpl struct {
	pgr pager.Pager
}

// OpenBTreeConn opens a B-Tree connection.
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

func (b *BTreeImpl) Close() error  { return nil }
func (b *BTreeImpl) Begin(write bool) error { return b.pgr.Begin(write) }
func (b *BTreeImpl) Commit() error  { return b.pgr.Commit() }
func (b *BTreeImpl) Rollback() error { return b.pgr.Rollback() }

// hdrSize returns the total header size (page offset + 8-byte B-Tree header).
func hdrSize(pageNum PageNumber) int {
	return pageOffset(pageNum) + 8
}

// CreateBTree creates a new B-Tree root page.
func (b *BTreeImpl) CreateBTree(flags CreateFlags) (PageNumber, error) {
	page, err := b.pgr.GetNewPage()
	if err != nil {
		return 0, err
	}
	pageNum := page.PageNum
	off := pageOffset(pageNum)

	// Page type
	switch flags {
	case CreateTable:
		page.Data[off] = PageTypeLeafTable
	case CreateIndex:
		page.Data[off] = PageTypeLeafIndex
	default:
		page.Data[off] = PageTypeLeafTable
	}

	// First free block = 0
	binary.BigEndian.PutUint16(page.Data[off+1:off+3], 0)
	// Number of cells = 0
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], 0)
	// Cell content offset = pageSize (empty, meaning full page available)
	binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(b.pageSize))
	// Fragmented free bytes
	page.Data[off+7] = 0

	b.pgr.MarkDirty(page)
	b.pgr.WritePage(page)
	b.pgr.ReleasePage(page)
	return pageNum, nil
}

func (b *BTreeImpl) Drop(rootPage PageNumber) error { return nil }

func (b *BTreeImpl) Clear(rootPage PageNumber) error {
	page, err := b.pgr.GetPage(rootPage)
	if err != nil {
		return err
	}
	defer b.pgr.ReleasePage(page)
	off := pageOffset(rootPage)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], 0)
	b.pgr.MarkDirty(page)
	return b.pgr.WritePage(page)
}

func (b *BTreeImpl) Cursor(rootPage PageNumber, write bool) (BTCursor, error) {
	return &CursorImpl{
		bt:       b,
		rootPage: rootPage,
		write:    write,
	}, nil
}

// pageHeader reads B-Tree page header fields.
type pageHeader struct {
	pageType    byte
	firstFree   uint16
	numCells    uint16
	contentOff  uint16
	fragBytes   byte
	rightChild  uint32 // Interior pages only
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
	if h.pageType == PageTypeInteriorTable || h.pageType == PageTypeInteriorIndex {
		h.rightChild = binary.BigEndian.Uint32(data[off+8 : off+12])
	}
	return h
}

// cellPtrOffset returns the byte offset of the i-th cell pointer.
func cellPtrOffset(pageNum PageNumber, i int) int {
	return hdrSize(pageNum) + i*2
}

// Insert inserts a key/value pair.
func (b *BTreeImpl) Insert(cursor BTCursor, key []byte, data []byte, rowid RowID, seekResult SeekResult) error {
	cur := cursor.(*CursorImpl)

	// For updates (SeekFound), delete the old entry first to free space.
	if seekResult == SeekFound {
		page, err := b.pgr.GetPage(cur.rootPage)
		if err != nil {
			return err
		}
		hdr := readPageHeader(page.Data, cur.rootPage)
		hs := hdrSize(cur.rootPage)
		for i := 0; i < int(hdr.numCells); i++ {
			ptr := int(binary.BigEndian.Uint16(page.Data[hs+i*2 : hs+i*2+2]))
			if ptr == 0 || ptr >= len(page.Data) {
				continue
			}
			// Read rowid from cell data
			existingRowid := readCellRowidFromPage(page.Data, ptr, hdr.pageType)
			if RowID(existingRowid) == rowid {
				// Remove this cell by shifting pointers
				for j := i; j < int(hdr.numCells)-1; j++ {
					nextPtr := binary.BigEndian.Uint16(page.Data[hs+(j+1)*2 : hs+(j+1)*2+2])
					binary.BigEndian.PutUint16(page.Data[hs+j*2:], nextPtr)
				}
				off := pageOffset(cur.rootPage)
				binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(int(hdr.numCells)-1))
				break
			}
		}
		b.pgr.MarkDirty(page)
		b.pgr.WritePage(page)
		b.pgr.ReleasePage(page)
	}

	page, err := b.pgr.GetPage(cur.rootPage)
	if err != nil {
		return err
	}
	defer b.pgr.ReleasePage(page)

	hdr := readPageHeader(page.Data, cur.rootPage)
	pageType := hdr.pageType
	numCells := int(hdr.numCells)
	hs := hdrSize(cur.rootPage)

	// Build cell data
	var cellData []byte
	if pageType == PageTypeLeafTable {
		payload := data
		if payload == nil {
			payload = []byte{}
		}
		pl := VarintLen(int64(len(payload)))
		rl := VarintLen(int64(rowid))
		cellData = make([]byte, pl+rl+len(payload))
		pos := PutVarint(cellData, int64(len(payload)))
		pos += PutVarint(cellData[pos:], int64(rowid))
		copy(cellData[pos:], payload)
	} else if pageType == PageTypeLeafIndex {
		payload := key
		pl := VarintLen(int64(len(payload)))
		cellData = make([]byte, pl+len(payload))
		pos := PutVarint(cellData, int64(len(payload)))
		copy(cellData[pos:], payload)
	} else {
		return fmt.Errorf("unsupported page type for insert: %d", pageType)
	}

	cellSize := len(cellData)
	cellContentStart := int(hdr.contentOff)
	if cellContentStart == 0 || cellContentStart > b.pageSize {
		cellContentStart = b.pageSize
	}

	// Cell pointer area: from hs to hs + numCells*2
	cellPtrAreaEnd := hs + numCells*2
	newContentStart := cellContentStart - cellSize

	if newContentStart < cellPtrAreaEnd+2 {
		return fmt.Errorf("page full: need split (not yet implemented)")
	}

	// Write cell data from bottom of page
	copy(page.Data[newContentStart:], cellData)

	// Append cell pointer
	binary.BigEndian.PutUint16(page.Data[hs+numCells*2:], uint16(newContentStart))

	// Update header: numCells++ and contentOff
	off := pageOffset(cur.rootPage)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(numCells+1))
	binary.BigEndian.PutUint16(page.Data[off+5:off+7], uint16(newContentStart))

	b.pgr.MarkDirty(page)
	return b.pgr.WritePage(page)
}

// Delete deletes the entry at the cursor position.
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
	hs := hdrSize(cur.currentPage)

	if cur.cellIndex < 0 || cur.cellIndex >= numCells {
		return fmt.Errorf("cell index out of range: %d", cur.cellIndex)
	}

	// Shift cell pointers to remove the deleted one
	for i := cur.cellIndex; i < numCells-1; i++ {
		ptr := binary.BigEndian.Uint16(page.Data[hs+(i+1)*2 : hs+(i+1)*2+2])
		binary.BigEndian.PutUint16(page.Data[hs+i*2:], ptr)
	}

	// Update cell count
	off := pageOffset(cur.currentPage)
	binary.BigEndian.PutUint16(page.Data[off+3:off+5], uint16(numCells-1))

	b.pgr.MarkDirty(page)
	cur.valid = false
	return b.pgr.WritePage(page)
}

// Count returns the number of entries in the B-Tree.
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
	numCells := int64(hdr.numCells)

	if hdr.pageType == PageTypeInteriorTable || hdr.pageType == PageTypeInteriorIndex {
		hs := hdrSize(pageNum)
		total := numCells
		for i := 0; i < int(numCells); i++ {
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
	return numCells, nil
}

func (b *BTreeImpl) IntegrityCheck(rootPage PageNumber, depth int, errDest *[]string) {}
func (b *BTreeImpl) PageCount() int { return b.pgr.PageCount() }
