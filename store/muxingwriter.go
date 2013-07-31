package store

import (
	"fmt"
	"math"
)

type MuxingWriter []Writer

// This builds a writer that writes each record it recieves to one of the
// provided writers according to the record's DatabaseIndex. For example, the
// writer NewMuxedStoreWriter(db0, db1) will write records with DatabaseIndex =
// 0 to db0 and records with DatabaseIndex = 1 to db1.
func NewMuxingWriter(writers ...Writer) MuxingWriter {
	if len(writers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot write to more than %d databases", math.MaxUint8))
	}
	return MuxingWriter(writers)
}

func (writers MuxingWriter) BeginWriting() error {
	for _, writer := range writers {
		if err := writer.BeginWriting(); err != nil {
			return err
		}
	}
	return nil
}

func (writers MuxingWriter) WriteRecord(record *Record) error {
	return writers[record.DatabaseIndex].WriteRecord(record)
}

func (writers MuxingWriter) EndWriting() error {
	for _, writer := range writers {
		if err := writer.EndWriting(); err != nil {
			return err
		}
	}
	return nil
}
