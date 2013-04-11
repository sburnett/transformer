package transformer

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"math"
	"sort"
)

type StoreReader interface {
	BeginReading() error
	ReadRecord() (*LevelDbRecord, error)
	EndReading() error
}

type StoreWriter interface {
	BeginWriting() error
	WriteRecord(*LevelDbRecord) error
	EndWriting() error
}

type StoreSeeker interface {
	StoreReader
	Seek([]byte) error
}

type StoreDeleter interface {
	StoreWriter
	DeleteAllRecords() error
}

type Datastore interface {
	StoreReader
	StoreWriter
}

type DatastoreFull interface {
	StoreDeleter
	StoreSeeker
}

type DemuxStoreReader struct {
	readers []StoreReader
	records PriorityQueue
}

func NewDemuxStoreReader(readers ...StoreReader) *DemuxStoreReader {
	if len(readers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}
	return &DemuxStoreReader{readers: readers}
}

func (demuxer *DemuxStoreReader) BeginReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.BeginReading(); err != nil {
			return err
		}
	}
	return nil
}

func (demuxer *DemuxStoreReader) ReadRecord() (*LevelDbRecord, error) {
	readRecord := func(reader StoreReader, queue *PriorityQueue, databaseIndex uint8) error {
		record, err := reader.ReadRecord()
		if err != nil {
			return err
		}
		if record == nil {
			return nil
		}
		record.DatabaseIndex = databaseIndex
		item := &Item{
			record:   record,
			reader:   reader,
			priority: Priority{key: record.Key, databaseIndex: uint8(databaseIndex)},
		}
		heap.Push(queue, item)
		return nil
	}

	if demuxer.records == nil {
		demuxer.records = make(PriorityQueue, 0, len(demuxer.readers))
		for idx, reader := range demuxer.readers {
			if err := readRecord(reader, &demuxer.records, uint8(idx)); err != nil {
				return nil, err
			}
		}
	}

	if demuxer.records.Len() == 0 {
		return nil, nil
	}

	item := heap.Pop(&demuxer.records).(*Item)
	if err := readRecord(item.reader, &demuxer.records, item.priority.databaseIndex); err != nil {
		return nil, err
	}
	return item.record, nil
}

func (demuxer *DemuxStoreReader) EndReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.EndReading(); err != nil {
			return err
		}
	}
	return nil
}

type DemuxStoreSeeker struct {
	readers []StoreSeeker
	records PriorityQueue
}

func NewDemuxStoreSeeker(readers ...StoreSeeker) *DemuxStoreSeeker {
	if len(readers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}
	return &DemuxStoreSeeker{readers: readers}
}

func (demuxer *DemuxStoreSeeker) BeginReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.BeginReading(); err != nil {
			return err
		}
	}
	return nil
}

func (demuxer *DemuxStoreSeeker) ReadRecord() (*LevelDbRecord, error) {
	readRecord := func(reader StoreReader, queue *PriorityQueue, databaseIndex uint8) error {
		record, err := reader.ReadRecord()
		if err != nil {
			return err
		}
		if record == nil {
			return nil
		}
		record.DatabaseIndex = databaseIndex
		item := &Item{
			record:   record,
			reader:   reader,
			priority: Priority{key: record.Key, databaseIndex: uint8(databaseIndex)},
		}
		heap.Push(queue, item)
		return nil
	}

	if demuxer.records == nil {
		demuxer.records = make(PriorityQueue, 0, len(demuxer.readers))
		for idx, reader := range demuxer.readers {
			if err := readRecord(reader, &demuxer.records, uint8(idx)); err != nil {
				return nil, err
			}
		}
	}

	if demuxer.records.Len() == 0 {
		return nil, nil
	}

	item := heap.Pop(&demuxer.records).(*Item)
	if err := readRecord(item.reader, &demuxer.records, item.priority.databaseIndex); err != nil {
		return nil, err
	}
	return item.record, nil
}

func (demuxer *DemuxStoreSeeker) Seek(key []byte) error {
	for _, reader := range demuxer.readers {
		if err := reader.Seek(key); err != nil {
			return err
		}
	}
	demuxer.records = nil
	return nil
}

func (demuxer *DemuxStoreSeeker) EndReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.EndReading(); err != nil {
			return err
		}
	}
	return nil
}

type MuxedStoreWriter []StoreWriter

func NewMuxedStoreWriter(writers ...StoreWriter) MuxedStoreWriter {
	if len(writers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot write to more than %d databases", math.MaxUint8))
	}
	return MuxedStoreWriter(writers)
}

func (writers MuxedStoreWriter) BeginWriting() error {
	for _, writer := range writers {
		if err := writer.BeginWriting(); err != nil {
			return err
		}
	}
	return nil
}

func (writers MuxedStoreWriter) WriteRecord(record *LevelDbRecord) error {
	return writers[record.DatabaseIndex].WriteRecord(record)
}

func (writers MuxedStoreWriter) EndWriting() error {
	for _, writer := range writers {
		if err := writer.EndWriting(); err != nil {
			return err
		}
	}
	return nil
}

type StoreWriterTruncate struct {
	writer StoreDeleter
}

func TruncateBeforeWriting(writer StoreDeleter) *StoreWriterTruncate {
	return &StoreWriterTruncate{writer: writer}
}

func (store *StoreWriterTruncate) BeginWriting() error {
	if err := store.writer.BeginWriting(); err != nil {
		return err
	}
	return store.writer.DeleteAllRecords()
}

func (store *StoreWriterTruncate) WriteRecord(record *LevelDbRecord) error {
	return store.writer.WriteRecord(record)
}

func (store *StoreWriterTruncate) EndWriting() error {
	return store.writer.EndWriting()
}

type StoreReaderExcludeRanges struct {
	reader               StoreSeeker
	excludedReader       StoreReader
	currentExcludeRecord *LevelDbRecord
}

func ReadExcludingRanges(reader StoreSeeker, excludedReader StoreReader) *StoreReaderExcludeRanges {
	return &StoreReaderExcludeRanges{
		reader:         reader,
		excludedReader: excludedReader,
	}
}

func (store *StoreReaderExcludeRanges) BeginReading() error {
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

func (store *StoreReaderExcludeRanges) ReadRecord() (*LevelDbRecord, error) {
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

func (store *StoreReaderExcludeRanges) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *StoreReaderExcludeRanges) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.excludedReader.EndReading(); err != nil {
		return err
	}
	return nil
}

type StoreReaderIncludeRanges struct {
	reader                StoreSeeker
	includedReader        StoreReader
	currentIncludedRecord *LevelDbRecord
}

func ReadIncludingRanges(reader StoreSeeker, includedReader StoreReader) *StoreReaderIncludeRanges {
	return &StoreReaderIncludeRanges{
		reader:         reader,
		includedReader: includedReader,
	}
}

func (store *StoreReaderIncludeRanges) BeginReading() error {
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

func (store *StoreReaderIncludeRanges) ReadRecord() (*LevelDbRecord, error) {
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

func (store *StoreReaderIncludeRanges) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *StoreReaderIncludeRanges) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.includedReader.EndReading(); err != nil {
		return err
	}
	return nil
}

type StoreReaderIncludePrefixes struct {
	reader                StoreSeeker
	includedReader        StoreReader
	currentIncludedRecord *LevelDbRecord
}

func ReadIncludingPrefixes(reader StoreSeeker, includedReader StoreReader) *StoreReaderIncludePrefixes {
	return &StoreReaderIncludePrefixes{
		reader:         reader,
		includedReader: includedReader,
	}
}

func (store *StoreReaderIncludePrefixes) BeginReading() error {
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

func (store *StoreReaderIncludePrefixes) ReadRecord() (*LevelDbRecord, error) {
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

func (store *StoreReaderIncludePrefixes) Seek(key []byte) error {
	return store.reader.Seek(key)
}

func (store *StoreReaderIncludePrefixes) EndReading() error {
	if err := store.reader.EndReading(); err != nil {
		return err
	}
	if err := store.includedReader.EndReading(); err != nil {
		return err
	}
	return nil
}

type SliceStore struct {
	records []*LevelDbRecord
	cursor  int
}

type LevelDbRecordSlice []*LevelDbRecord

func (p LevelDbRecordSlice) Len() int           { return len(p) }
func (p LevelDbRecordSlice) Less(i, j int) bool { return bytes.Compare(p[i].Key, p[j].Key) < 0 }
func (p LevelDbRecordSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (store *SliceStore) BeginReading() error {
	sort.Sort(LevelDbRecordSlice(store.records))
	store.cursor = -1
	return nil
}

func (store *SliceStore) ReadRecord() (*LevelDbRecord, error) {
	store.cursor++
	if store.cursor >= len(store.records) {
		return nil, nil
	}
	return store.records[store.cursor].Copy(), nil
}

func (store *SliceStore) EndReading() error {
	return nil
}

func (store *SliceStore) BeginWriting() error {
	return nil
}

func (store *SliceStore) WriteRecord(record *LevelDbRecord) error {
	for idx, existingRecord := range store.records {
		if bytes.Equal(record.Key, existingRecord.Key) {
			store.records[idx] = record.Copy()
			return nil
		}
	}
	store.records = append(store.records, record.Copy())
	return nil
}

func (store *SliceStore) EndWriting() error {
	return nil
}

func (store *SliceStore) DeleteAllRecords() error {
	store.records = nil
	return nil
}

func (store *SliceStore) Seek(key []byte) error {
	store.cursor = -1
	for store.cursor < len(store.records) {
		if store.cursor+1 >= len(store.records) || bytes.Compare(store.records[store.cursor+1].Key, key) >= 0 {
			break
		}
		store.cursor++
	}
	return nil
}

func (store *SliceStore) Print() {
	store.BeginReading()
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		log.Printf("%s: %s (%v: %v)", record.Key, record.Value, record.Key, record.Value)
	}
}
