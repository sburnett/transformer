package store

// A store from which you can read Records. You must call BeginReading, then
// ReaderRecord, then EndReading.
type Reader interface {
	BeginReading() error
	ReadRecord() (*Record, error)
	EndReading() error
}

// A store to which you can write Records. You must call BeginWriting, then
// WriteRecord, then EndWriting.
type Writer interface {
	BeginWriting() error
	WriteRecord(*Record) error
	EndWriting() error
}

// A Reader that can seek to arbitrary keys. Like ReadRecord, Seek can only be
// used between BeginReading and EndReading calls.
type Seeker interface {
	Reader
	Seek([]byte) error
}

// A Writer that can erase all keys from the store. Like WriteRecord,
// DeleteAllRecords can only be used between BeginWriting and EndWriting calls.
type Deleter interface {
	Writer
	DeleteAllRecords() error
}

// A store that is both a Reader and a Writer
type ReadingWriter interface {
	Reader
	Writer
}

// A store that is both a Seeker and a Writer.
type SeekingWriter interface {
	Seeker
	Writer
}

// A store that is both a Reader and a Deleter.
type ReadingDeleter interface {
	Reader
	Deleter
}

// A store that is both a Seeker and a Deleter.
type SeekingDeleter interface {
	Seeker
	Deleter
}
