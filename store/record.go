package store

// This represents a single entry in a LevelDb database and is the foundation of
// record processing.
//
// You will find github.com/sburnett/transformer/key very helpful for encoding
// and decoding Keys and Values.
type Record struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
}

// Perform a deep copy of a Record.
func (record *Record) Copy() *Record {
	return &Record{
		Key:           []byte(record.Key),
		Value:         []byte(record.Value),
		DatabaseIndex: record.DatabaseIndex,
	}
}

// Convenience function to make a new record from strings. Useful in tests.
func NewRecord(key string, value string, databaseIndex uint8) *Record {
	return &Record{
		Key:           []byte(key),
		Value:         []byte(value),
		DatabaseIndex: databaseIndex,
	}
}
