package pager

// MemJournal implements an in-memory journal for :memory: databases.
type MemJournal struct {
	records []journalRecord
	pageSize int
}

// NewMemJournal creates a new in-memory journal.
func NewMemJournal(pageSize int) *MemJournal {
	return &MemJournal{pageSize: pageSize}
}

// Begin initializes the journal.
func (j *MemJournal) Begin() error {
	j.records = j.records[:0]
	return nil
}

// WritePage records a page.
func (j *MemJournal) WritePage(pageNum PageNumber, data []byte) error {
	rec := journalRecord{
		pageNum: pageNum,
		data:    make([]byte, len(data)),
	}
	copy(rec.data, data)
	j.records = append(j.records, rec)
	return nil
}

// Commit is a no-op for in-memory journal.
func (j *MemJournal) Commit() error {
	return nil
}

// Rollback returns the recorded pages for rollback.
func (j *MemJournal) Rollback() ([]journalRecord, error) {
	result := make([]journalRecord, len(j.records))
	for i, r := range j.records {
		result[len(j.records)-1-i] = r
	}
	return result, nil
}

// Close is a no-op for in-memory journal.
func (j *MemJournal) Close() error {
	j.records = nil
	return nil
}

// IsOpen returns true.
func (j *MemJournal) IsOpen() bool {
	return true
}
