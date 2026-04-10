package btree

import (
	"encoding/binary"
)

const (
	PageTypeInteriorIndex byte = 2
	PageTypeInteriorTable byte = 5
	PageTypeLeafIndex     byte = 10
	PageTypeLeafTable     byte = 13
)

var pageSize = 4096

func SetPageSize(sz int) {
	if sz >= 512 && sz <= 65536 {
		pageSize = sz
	}
}

func isInteriorPage(pageType byte) bool {
	return pageType == PageTypeInteriorTable || pageType == PageTypeInteriorIndex
}

func maxLocalSize(payloadLen int) int {
	U := pageSize
	X := U - 35
	M := ((U - 12) * 32 / 255) - 23
	if payloadLen <= X {
		return payloadLen
	}
	K := M + ((payloadLen - M) % (U - 4))
	if K <= X {
		return K
	}
	return M
}

func maxLocalSizeInterior(payloadLen int) int {
	U := pageSize
	X := ((U-12)*64/255 - 23)
	M := ((U - 12) * 32 / 255) - 23
	if payloadLen <= X {
		return payloadLen
	}
	K := M + ((payloadLen - M) % (U - 4))
	if K <= X {
		return K
	}
	return M
}

func initPage(data []byte, pageNum PageNumber, pageType byte, ps int) {
	off := pageOffset(pageNum)
	data[off] = pageType
	binary.BigEndian.PutUint16(data[off+1:off+3], 0)
	binary.BigEndian.PutUint16(data[off+3:off+5], 0)
	binary.BigEndian.PutUint16(data[off+5:off+7], uint16(ps))
	data[off+7] = 0
	if isInteriorPage(pageType) {
		binary.BigEndian.PutUint32(data[off+8:off+12], 0)
	}
}

// readCellRowidFromPage reads the rowid from a cell at a given offset in page data.
func readCellRowidFromPage(pageData []byte, cellOffset int, pageType byte) int64 {
	if cellOffset < 0 || cellOffset >= len(pageData) {
		return 0
	}
	return readCellRowid(pageData[cellOffset:], pageType)
}

func readCellRowid(data []byte, pageType byte) int64 {
	switch pageType {
	case PageTypeLeafTable:
		_, n := ReadVarint(data)
		rowid, _ := ReadVarint(data[n:])
		return rowid
	case PageTypeInteriorTable:
		rowid, _ := ReadVarint(data[4:])
		return rowid
	}
	return 0
}

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
			size += 4
		}
		return size
	case PageTypeInteriorTable:
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

func extractCellData(pageData []byte, pageNum PageNumber, i int) []byte {
	hdr := readPageHeader(pageData, pageNum)
	hs := hdrSize(pageNum)
	if isInteriorPage(hdr.pageType) {
		hs = hdrSizeInterior(pageNum)
	}
	if hs+i*2+2 > len(pageData) {
		return nil
	}
	cellPtr := int(binary.BigEndian.Uint16(pageData[hs+i*2 : hs+i*2+2]))
	if cellPtr == 0 || cellPtr >= len(pageData) {
		return nil
	}
	sz := cellTotalSize(pageData, cellPtr, hdr.pageType)
	end := cellPtr + sz
	if end > len(pageData) {
		end = len(pageData)
	}
	buf := make([]byte, end-cellPtr)
	copy(buf, pageData[cellPtr:end])
	return buf
}
