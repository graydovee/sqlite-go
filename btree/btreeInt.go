package btree

// B-Tree page types
const (
	PageTypeInteriorIndex = 2
	PageTypeInteriorTable = 5
	PageTypeLeafIndex     = 10
	PageTypeLeafTable     = 13
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

// MaxLocalSize returns the maximum local payload for a leaf page.
func maxLocalSize(payloadLen int) int {
	U := pageSize
	X := ((U - 12) * 64 / 255) - 23
	M := ((U - 12) * 32 / 255) - 23
	K := M + ((payloadLen - M) % (U - 4))
	if payloadLen <= X {
		return payloadLen
	}
	if K <= X {
		return K
	}
	return M
}

// MaxLocalSizeInterior returns the maximum local payload for an interior page.
func maxLocalSizeInterior(payloadLen int) int {
	U := pageSize
	X := ((U - 12) * 64 / 255) - 23
	M := ((U - 12) * 32 / 255) - 23
	K := M + ((payloadLen - M) % (U - 4))
	if payloadLen <= X {
		return payloadLen
	}
	if K <= X {
		return K
	}
	return M
}
