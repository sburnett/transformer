package store

import (
	"bytes"
)

type RangeIncludingReader struct {
	reader                Seeker
	includedReader        Reader
	currentIncludedRecord *Record
}

// Read records from reader that fall within ranges indicated by includedReader.
// includedReader specifies ranges where Key is the beginning of the range and
// Value is the end of the range. Ranges are closed intervals, so endpoints will
// be included.
func NewRangeIncludingReader(reader Seeker, includedReader Reader) *RangeIncludingReader {
	return &RangeIncludingReader{
		reader:         reader,
		includedReader: includedReader,
	}
}

func (store *RangeIncludingReader) BeginReading() error {
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

func (store *RangeIncludingReader) ReadRecord() (*Record, error) {
	currentRecord, err := store.reader.ReadRecord()
	if currentRecord == nil || err != nil {
		return nil, err
	}
	for store.currentIncludedRecord != nil && (bytes.Compare(currentRecord.Key, store.currentIncludedRecord.Key) < 0 || bytes.Compare(currentRecord.Key, store.currentIncludedRecord.Value) > 0) {
		if bytes.Compare(currentRecord.Key, store.currentIncludedRecord.Key) < 0 {
			seeks.Add(1)
			store.reader.Seek(store.currentIncludedRecord.Key)
			currentRecord, err = store.reader.ReadRecord()
			if currentRecord == nil || err != nil {
				return nil, err
			}
		}
		if bytes.Compare(currentRecord.Key, store.currentIncludedRecord.Value) > 0 {
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

func (store *RangeIncludingReader) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *RangeIncludingReader) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.includedReader.EndReading(); err != nil {
		return err
	}
	return nil
}
