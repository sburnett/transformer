package store

import (
	"bytes"
)

type PrefixIncludingReader struct {
	reader                Seeker
	includedReader        Reader
	currentIncludedRecord *Record
}

// Construct a Seeker that reads records from reader that have prefixes
// indicated by includedReader. includedReader specifies prefixes in the Key and
// ignores the Value.
func NewPrefixIncludingReader(reader Seeker, includedReader Reader) *PrefixIncludingReader {
	return &PrefixIncludingReader{
		reader:         reader,
		includedReader: includedReader,
	}
}

func (store *PrefixIncludingReader) BeginReading() error {
	if err := store.reader.BeginReading(); err != nil {
		return err
	}
	if err := store.includedReader.BeginReading(); err != nil {
		return err
	}
	currentIncludedRecord, err := store.includedReader.ReadRecord()
	if err != nil {
		return err
	}
	store.currentIncludedRecord = currentIncludedRecord
	return nil
}

func (store *PrefixIncludingReader) ReadRecord() (*Record, error) {
	currentRecord, err := store.reader.ReadRecord()
	if currentRecord == nil || err != nil {
		return nil, err
	}
	for store.currentIncludedRecord != nil && !bytes.HasPrefix(currentRecord.Key, store.currentIncludedRecord.Key) {
		comparison := bytes.Compare(currentRecord.Key, store.currentIncludedRecord.Key)
		switch {
		case comparison < 0:
			seeks.Add(1)
			store.reader.Seek(store.currentIncludedRecord.Key)
			currentRecord, err = store.reader.ReadRecord()
			if currentRecord == nil || err != nil {
				return nil, err
			}
		case comparison > 0:
			currentIncludedRecord, err := store.includedReader.ReadRecord()
			if err != nil {
				return nil, err
			}
			store.currentIncludedRecord = currentIncludedRecord
		}
	}
	if store.currentIncludedRecord == nil {
		return nil, nil
	}
	return currentRecord, nil
}

func (store *PrefixIncludingReader) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *PrefixIncludingReader) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.includedReader.EndReading(); err != nil {
		return err
	}
	return nil
}
