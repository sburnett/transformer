package store

import (
	"bytes"
)

type RangeExcludingReader struct {
	reader               Seeker
	excludedReader       Reader
	currentExcludeRecord *Record
}

// Read all records from reader except those that fall within ranges specified
// in excludedReader. excludedReader encodes ranges where the Key is the
// beginning of the range and Value is the end of the range. Ranges are closed
// intervals, so endpoints will not be read from the reader.
func NewRangeExcludingReader(reader Seeker, excludedReader Reader) *RangeExcludingReader {
	return &RangeExcludingReader{
		reader:         reader,
		excludedReader: excludedReader,
	}
}

func (store *RangeExcludingReader) BeginReading() error {
	if err := store.reader.BeginReading(); err != nil {
		return err
	}
	if err := store.excludedReader.BeginReading(); err != nil {
		return err
	}
	currentExcludeRecord, err := store.excludedReader.ReadRecord()
	if err != nil {
		return err
	}
	store.currentExcludeRecord = currentExcludeRecord
	return nil
}

func (store *RangeExcludingReader) ReadRecord() (*Record, error) {
	if store.currentExcludeRecord == nil {
		return store.reader.ReadRecord()
	}

	currentRecord, err := store.reader.ReadRecord()
	if currentRecord == nil || err != nil {
		return nil, err
	}
	for store.currentExcludeRecord != nil && bytes.Compare(currentRecord.Key, store.currentExcludeRecord.Key) >= 0 && bytes.Compare(currentRecord.Key, store.currentExcludeRecord.Value) <= 0 {
		seeks.Add(1)
		store.reader.Seek(store.currentExcludeRecord.Value)
		currentRecord, err = store.reader.ReadRecord()
		if currentRecord == nil || err != nil {
			return nil, err
		}
		if bytes.Compare(currentRecord.Key, store.currentExcludeRecord.Value) == 0 {
			currentRecord, err = store.reader.ReadRecord()
			if currentRecord == nil || err != nil {
				return nil, err
			}
		}
		currentExcludeRecord, err := store.excludedReader.ReadRecord()
		if err != nil {
			return nil, err
		}
		store.currentExcludeRecord = currentExcludeRecord
	}
	return currentRecord, nil
}

func (store *RangeExcludingReader) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *RangeExcludingReader) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.excludedReader.EndReading(); err != nil {
		return err
	}
	return nil
}
