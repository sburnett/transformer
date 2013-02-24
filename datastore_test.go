package transformer

import (
	"fmt"
)

func makeLevelDbRecord(key string, value string, databaseIndex uint8) *LevelDbRecord {
	return &LevelDbRecord{
		Key:           []byte(key),
		Value:         []byte(value),
		DatabaseIndex: databaseIndex,
	}
}

type ChannelStoreReader chan *LevelDbRecord

func (records ChannelStoreReader) Read(outputChan chan *LevelDbRecord) error {
	for record := range records {
		outputChan <- record
	}
	close(outputChan)
	return nil
}

func ExampleDemuxInputsSorted() {
	firstChan := ChannelStoreReader(make(chan *LevelDbRecord, 10))
	firstChan <- makeLevelDbRecord("d", "foo0", 0)
	firstChan <- makeLevelDbRecord("f", "bar0", 0)
	firstChan <- makeLevelDbRecord("h", "baz0", 0)
	close(firstChan)

	secondChan := ChannelStoreReader(make(chan *LevelDbRecord, 10))
	secondChan <- makeLevelDbRecord("e", "foo1", 1)
	secondChan <- makeLevelDbRecord("g", "bar1", 1)
	secondChan <- makeLevelDbRecord("i", "baz1", 1)
	close(secondChan)

	thirdChan := ChannelStoreReader(make(chan *LevelDbRecord, 10))
	thirdChan <- makeLevelDbRecord("a", "foo2", 2)
	thirdChan <- makeLevelDbRecord("b", "bar2", 2)
	thirdChan <- makeLevelDbRecord("c", "baz2", 2)
	close(thirdChan)

	outputChan := make(chan *LevelDbRecord, 30)

	DemuxStoreReader([]StoreReader{firstChan, secondChan, thirdChan}).Read(outputChan)

	for record := range outputChan {
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}

	// Output:
	// a: foo2
	// b: bar2
	// c: baz2
	// d: foo0
	// e: foo1
	// f: bar0
	// g: bar1
	// h: baz0
	// i: baz1
}

func ExampleDemuxInputsSorted_duplicateKeys() {
	firstChan := ChannelStoreReader(make(chan *LevelDbRecord, 10))
	firstChan <- makeLevelDbRecord("a", "foo0", 0)
	firstChan <- makeLevelDbRecord("b", "bar0", 0)
	firstChan <- makeLevelDbRecord("c", "baz0", 0)
	close(firstChan)

	secondChan := ChannelStoreReader(make(chan *LevelDbRecord, 10))
	secondChan <- makeLevelDbRecord("b", "foo1", 1)
	secondChan <- makeLevelDbRecord("c", "bar1", 1)
	close(secondChan)

	outputChan := make(chan *LevelDbRecord, 30)

	err := DemuxStoreReader([]StoreReader{firstChan, secondChan}).Read(outputChan)
	if err != nil {
		panic(err)
	}

	for record := range outputChan {
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}

	// Output:
	// a: foo0
	// b: bar0
	// b: foo1
	// c: baz0
	// c: bar1
}

type ChannelStoreWriter chan *LevelDbRecord

func (records ChannelStoreWriter) Write(inputChan chan *LevelDbRecord) error {
	for record := range inputChan {
		records <- record
	}
	close(records)
	return nil
}

func ExampleMuxedStoreWriter() {
	records := make(chan *LevelDbRecord, 10)
	records <- makeLevelDbRecord("a", "b", 0)
	records <- makeLevelDbRecord("c", "d", 1)
	records <- makeLevelDbRecord("e", "f", 0)
	records <- makeLevelDbRecord("g", "h", 1)
	close(records)

	firstChan := ChannelStoreWriter(make(chan *LevelDbRecord, 10))
	secondChan := ChannelStoreWriter(make(chan *LevelDbRecord, 10))

	writers := []StoreWriter{firstChan, secondChan}
	err := MuxedStoreWriter(writers).Write(records)
	if err != nil {
		panic(err)
	}

	for record := range firstChan {
		fmt.Printf("[0] %s: %s\n", record.Key, record.Value)
	}
	for record := range secondChan {
		fmt.Printf("[1] %s: %s\n", record.Key, record.Value)
	}

	// Output:
	// [0] a: b
	// [0] e: f
	// [1] c: d
	// [1] g: h
}
