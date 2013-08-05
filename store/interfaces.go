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

// A Manager is an interface for creating stores.
//
// The arguments to each creator usually get passed to the store's constructor.
// Typically, a manager manages all stores in a directory. For examples,
// NewLevelDbManager creates multiple LevelDB databases inside a single parent
// directory using NewLevelDbStore.
//
// This interface exists to give pipielines an interface with which they can
// access multiple stores, without your needing to pass those stores to the
// pipeline individually.
type Manager interface {
	Reader(...interface{}) Reader
	Writer(...interface{}) Writer
	Seeker(...interface{}) Seeker
	Deleter(...interface{}) Deleter
	ReadingWriter(...interface{}) ReadingWriter
	SeekingWriter(...interface{}) SeekingWriter
	ReadingDeleter(...interface{}) ReadingDeleter
	SeekingDeleter(...interface{}) SeekingDeleter
}
