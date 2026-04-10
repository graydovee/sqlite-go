package btree

import (
	"encoding/binary"
)

// B-Tree page types
const (
	PageTypeInteriorIndex byte = 2
	PageTypeInteriorTable byte = 5
	PageTypeLeafIndex     byte = 10
	PageTypeLeafTable     byte = 13
)

// PageSize is the database page size (set during initialization).
var pageSize = 4096

// SetPageSize sets the page size for B-Tree operations.
func SetPageSize(sz int) {
	if sz >= 512 && sz <= 65536 {
		pageSize = sz
	}
}

// CellHeader describes a cell in a B-Tree page.
type CellHeader struct {
	LeftChild  PageNumber // Interior pages only
	Key        int64      // For table leaf/interior: rowid
	PayloadSize int64     // For index cells: total payload size
	Overflow   PageNumber // First overflow page (0 if none)
	LocalSize  int        // Bytes stored locally (not on overflow)
}

// maxLocalSize returns the maximum local payload for a leaf page.
// Formulas from SQLite docs:
//   U = usable page size (pageSize - reserved = pageSize)
//   X = U - 35 (max local for leaf table/index)
//   M = ((U - 12) * 32 / 255) - 23 (min local)
//   K = M + ((payloadLen - M) % (U - 4))
func maxLocalSize(payloadLen int) int {
	U := pageSize
	X := U - 35
	M := ((U-12)*32/255 - 23)
	if payloadLen <= X {
		return payloadLen
	}
	K := M + ((payloadLen - M) % (U - 4))
	if K <= X {
		return K
	}
	return M
}

// maxLocalSizeInterior returns the maximum local payload for an interior page.
// X = ((U - 12) * 64 / 255) - 23
func maxLocalSizeInterior(payloadLen int) int {
	U := pageSize
	X := ((U-12)*64/255 - 23)
	M := ((U-12)*32/255 - 23)
	if payloadLen <= X {
		return payloadLen
	}
	K := M + ((payloadLen - M) % (U - 4))
	if K <= X {
		return K
	}
	return M
}

// isInteriorPage returns true for interior page types.
func isInteriorPage(pageType byte) bool {
	return pageType == PageTypeInteriorTable || pageType == PageTypeInteriorIndex
}

// interiorHdrSize returns header size for interior pages (12 bytes + page offset).
func interiorHdrSize(pageNum PageNumber) int {
	return pageOffset(pageNum) + 12
}

// leafHdrSize returns header size for leaf pages (8 bytes + page offset).
func leafHdrSize(pageNum PageNumber) int {
	return pageOffset(pageNum) + 8
}

// cellTotalSize returns the total on-page size of a cell including any overflow pointer.
func cellTotalSize(data []byte, pos int, pageType byte) int {
	switch pageType {
	case PageTypeLeafTable:
		pl, n1 := ReadVarint(data[pos:])
		pos += n1
		_, n2 := ReadVarint(data[pos:])
		local := maxLocalSize(int(pl))
		if local > int(pl) {
			local = int(pl)
		}
		size := n1 + n2 + local
		if int(pl) > local {
			size += 4 // overflow page pointer
		}
		return size

	case PageTypeInteriorTable:
		// 4-byte left child + varint rowid
		_, n := ReadVarint(data[pos+4:])
		return 4 + n

	case PageTypeLeafIndex:
		pl, n1 := ReadVarint(data[pos:])
		local := maxLocalSize(int(pl))
		if local > int(pl) {
			local = int(pl)
		}
		size := n1 + local
		if int(pl) > local {
			size += 4
		}
		return size

	case PageTypeInteriorIndex:
		pl, n1 := ReadVarint(data[pos+4:])
		local := maxLocalSizeInterior(int(pl))
		if local > int(pl) {
			local = int(pl)
		}
		size := 4 + n1 + local
		if int(pl) > local {
			size += 4
		}
		return size
	}
	return 0
}

// cellData extracts the raw bytes of cell i from a page.
func cellData(pageData []byte, pageNum PageNumber, i int) []byte {
	hs := hdrSize(pageNum)
	cellPtr := int(binary.BigEndian.Uint16(pageData[hs+i*2 : hs+i*2+2]))
	if cellPtr == 0 || cellPtr >= len(pageData) {
		return nil
	}
	pageType := pageData[pageOffset(pageNum)]
	sz := cellTotalSize(pageData, cellPtr, pageType)
	if cellPtr+sz > len(pageData) {
		sz = len(pageData) - cellPtr
	}
	buf := make([]byte, sz)
	copy(buf, pageData[cellPtr:cellPtr+sz])
	return buf
}

// readCellRowid reads the rowid from a table cell (leaf or interior).
func readCellRowid(data []byte, pageType byte) int64 {
	switch pageType {
	case PageTypeLeafTable:
		_, n := ReadVarint(data) // payload size
		rowid, _ := ReadVarint(data[n:])
		return rowid
	case PageTypeInteriorTable:
		// 4-byte left child, then rowid
		rowid, _ := ReadVarint(data[4:])
		return rowid
	}
	return 0
}

// initPage writes a fresh page header.
func initPage(data []byte, pageNum PageNumber, pageType byte, ps int) {
	off := pageOffset(pageNum)
	data[off] = pageType
	binary.BigEndian.PutUint16(data[off+1:off+3], 0) // first free block
	binary.BigEndian.PutUint16(data[off+3:off+5], 0) // num cells = 0
	binary.BigEndian.PutUint16(data[off+5:off+7], uint16(ps)) // cell content offset = page size (empty)
	data[off+7] = 0 // fragmented free bytes
	if isInteriorPage(pageType) {
		binary.BigEndian.PutUint32(data[off+8:off+12], 0) // right child = 0
	}
}
