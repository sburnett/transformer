package transformer

import (
	"container/heap"
	"fmt"
	"math"
)

type StoreReader interface {
	Read(chan *LevelDbRecord) error
}

type StoreWriter interface {
	Write(chan *LevelDbRecord) error
}

type DemuxStoreReader []StoreReader

func (readers DemuxStoreReader) Read(outputChan chan *LevelDbRecord) error {
	defer close(outputChan)

	if len(readers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}

	inputChans := make([]chan *LevelDbRecord, len(readers))
	readResultsChan := make(chan error, len(readers))
	for idx, reader := range readers {
		inputChans[idx] = make(chan *LevelDbRecord)
		go func(reader StoreReader, channelIndex int) {
			readResultsChan <- reader.Read(inputChans[channelIndex])
		}(reader, idx)
	}

	currentRecords := make(PriorityQueue, 0, len(inputChans))
	readRecord := func(inputChan chan *LevelDbRecord, databaseIndex uint8) {
		if record, ok := <-inputChan; ok {
			item := &Item{
				record:   record,
				channel:  inputChan,
				priority: Priority{key: record.Key, databaseIndex: databaseIndex},
			}
			heap.Push(&currentRecords, item)
		}
	}
	for idx, inputChan := range inputChans {
		readRecord(inputChan, uint8(idx))
	}
	for currentRecords.Len() > 0 {
		item := heap.Pop(&currentRecords).(*Item)
		outputChan <- item.record
		readRecord(item.channel, item.priority.databaseIndex)
	}

	close(readResultsChan)
	for readError := range readResultsChan {
		if readError != nil {
			return readError
		}
	}
	return nil
}

type MuxedStoreWriter []StoreWriter

func (writers MuxedStoreWriter) Write(inputChan chan *LevelDbRecord) error {
	if len(writers) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot write to more than %d databases", math.MaxUint8))
	}
	numWriters := uint8(len(writers))

	outputChans := make([]chan *LevelDbRecord, numWriters)
	writeResultsChan := make(chan error, numWriters)
	for idx, writer := range writers {
		outputChans[idx] = make(chan *LevelDbRecord)
		go func(writer StoreWriter, outputChan chan *LevelDbRecord) {
			writeResultsChan <- writer.Write(outputChan)
		}(writer, outputChans[idx])
	}

	for record := range inputChan {
		if record.DatabaseIndex >= numWriters {
			panic("Not enough writers")
		}
		outputChans[record.DatabaseIndex] <- record
	}
	for _, outputChan := range outputChans {
		close(outputChan)
	}

	for i := uint8(0); i < numWriters; i++ {
		err := <-writeResultsChan
		if err != nil {
			return err
		}
	}

	return nil
}
