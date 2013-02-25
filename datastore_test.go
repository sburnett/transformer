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

func ExampleDemuxInputsSorted() {
	firstChan := ChannelStore(make(chan *LevelDbRecord, 10))
	firstChan <- makeLevelDbRecord("d", "foo0", 0)
	firstChan <- makeLevelDbRecord("f", "bar0", 0)
	firstChan <- makeLevelDbRecord("h", "baz0", 0)
	close(firstChan)

	secondChan := ChannelStore(make(chan *LevelDbRecord, 10))
	secondChan <- makeLevelDbRecord("e", "foo1", 1)
	secondChan <- makeLevelDbRecord("g", "bar1", 1)
	secondChan <- makeLevelDbRecord("i", "baz1", 1)
	close(secondChan)

	thirdChan := ChannelStore(make(chan *LevelDbRecord, 10))
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
	firstChan := ChannelStore(make(chan *LevelDbRecord, 10))
	firstChan <- makeLevelDbRecord("a", "foo0", 0)
	firstChan <- makeLevelDbRecord("b", "bar0", 0)
	firstChan <- makeLevelDbRecord("c", "baz0", 0)
	close(firstChan)

	secondChan := ChannelStore(make(chan *LevelDbRecord, 10))
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

func ExampleMuxedStoreWriter() {
	records := make(chan *LevelDbRecord, 10)
	records <- makeLevelDbRecord("a", "b", 0)
	records <- makeLevelDbRecord("c", "d", 1)
	records <- makeLevelDbRecord("e", "f", 0)
	records <- makeLevelDbRecord("g", "h", 1)
	close(records)

	firstChan := ChannelStore(make(chan *LevelDbRecord, 10))
	secondChan := ChannelStore(make(chan *LevelDbRecord, 10))

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

func ExampleSliceStore() {
	store := SliceStore([]*LevelDbRecord{})
	inputChan := make(chan *LevelDbRecord, 10)
	inputChan <- makeLevelDbRecord("b", "x", 0)
	inputChan <- makeLevelDbRecord("c", "y", 0)
	inputChan <- makeLevelDbRecord("a", "z", 0)
	close(inputChan)
	outputChan := make(chan *LevelDbRecord, 10)
	if err := store.Write(inputChan); err != nil {
		panic(err)
	}
	if err := store.Read(outputChan); err != nil {
		panic(err)
	}
	for record := range outputChan {
		fmt.Printf("[0] %s: %s\n", record.Key, record.Value)
	}

	// Output:
	// [0] a: z
	// [0] b: x
	// [0] c: y
}
