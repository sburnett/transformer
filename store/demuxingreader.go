package store

import (
	"container/heap"
	"fmt"
	"math"
)

type DemuxingReader struct {
	readers []Reader
	records PriorityQueue
}

// Make a new reader that reads records from the provided set of StoreReaders in
// lexicographic order by key. This lets you join multiple LevelDBs.
//
// When using records from the DemuxingReader, the DatabaseIndex field will be
// set according to the database's position in the argument list. For example
// for NewDemuxStoreReader(db1, db2), records from db1 will have DatabaseIndex =
// 0 and records from db2 will have DatabaseIndex = 1.
func NewDemuxingReader(readers ...Reader) *DemuxingReader {
	if len(readers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}
	return &DemuxingReader{readers: readers}
}

func (demuxer *DemuxingReader) BeginReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.BeginReading(); err != nil {
			return err
		}
	}
	return nil
}

func (demuxer *DemuxingReader) ReadRecord() (*Record, error) {
	readRecord := func(reader Reader, queue *PriorityQueue, databaseIndex uint8) error {
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

func (demuxer *DemuxingReader) EndReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.EndReading(); err != nil {
			return err
		}
	}
	return nil
}
