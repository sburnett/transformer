package store

import (
	"container/heap"
	"fmt"
	"math"
)

type DemuxingSeeker struct {
	readers []Seeker
	records PriorityQueue
}

// This is the same as NewDemuxStoreReader, except that all readers must be
// seekable, which lets you seek on the returned DemuxingSeeker.
func NewDemuxingSeeker(readers ...Seeker) *DemuxingSeeker {
	if len(readers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}
	return &DemuxingSeeker{readers: readers}
}

func (demuxer *DemuxingSeeker) BeginReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.BeginReading(); err != nil {
			return err
		}
	}
	return nil
}

func (demuxer *DemuxingSeeker) ReadRecord() (*Record, error) {
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

func (demuxer *DemuxingSeeker) Seek(key []byte) error {
	for _, reader := range demuxer.readers {
		if err := reader.Seek(key); err != nil {
			return err
		}
	}
	demuxer.records = nil
	return nil
}

func (demuxer *DemuxingSeeker) EndReading() error {
	for _, reader := range demuxer.readers {
		if err := reader.EndReading(); err != nil {
			return err
		}
	}
	return nil
}
