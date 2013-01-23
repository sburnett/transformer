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
	inputChans := make([]chan *LevelDbRecord, 3)

	inputChans[0] = make(chan *LevelDbRecord, 10)
	inputChans[0] <- makeLevelDbRecord("d", "foo0", 0)
	inputChans[0] <- makeLevelDbRecord("f", "bar0", 0)
	inputChans[0] <- makeLevelDbRecord("h", "baz0", 0)
	close(inputChans[0])

	inputChans[1] = make(chan *LevelDbRecord, 10)
	inputChans[1] <- makeLevelDbRecord("e", "foo1", 1)
	inputChans[1] <- makeLevelDbRecord("g", "bar1", 1)
	inputChans[1] <- makeLevelDbRecord("i", "baz1", 1)
	close(inputChans[1])

	inputChans[2] = make(chan *LevelDbRecord, 10)
	inputChans[2] <- makeLevelDbRecord("a", "foo2", 2)
	inputChans[2] <- makeLevelDbRecord("b", "bar2", 2)
	inputChans[2] <- makeLevelDbRecord("c", "baz2", 2)
	close(inputChans[2])

	outputChan := make(chan *LevelDbRecord, 30)

	demuxInputsSorted(inputChans, outputChan)

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
	inputChans := make([]chan *LevelDbRecord, 2)

	inputChans[0] = make(chan *LevelDbRecord, 10)
	inputChans[0] <- makeLevelDbRecord("a", "foo0", 0)
	inputChans[0] <- makeLevelDbRecord("b", "bar0", 0)
	inputChans[0] <- makeLevelDbRecord("c", "baz0", 0)
	close(inputChans[0])

	inputChans[1] = make(chan *LevelDbRecord, 10)
	inputChans[1] <- makeLevelDbRecord("b", "foo1", 1)
	inputChans[1] <- makeLevelDbRecord("c", "bar1", 1)
	close(inputChans[1])

	outputChan := make(chan *LevelDbRecord, 30)

	demuxInputsSorted(inputChans, outputChan)

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
