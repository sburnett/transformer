package store

import (
	"fmt"
)

func ExampleMuxingWriter() {
	records := []*Record{
		NewRecord("a", "b", 0),
		NewRecord("c", "d", 1),
		NewRecord("e", "f", 0),
		NewRecord("g", "h", 1),
	}

	firstStore := SliceStore{}
	secondStore := SliceStore{}

	writer := NewMuxingWriter(&firstStore, &secondStore)
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			panic(err)
		}
	}

	firstStore.BeginReading()
	for {
		record, err := firstStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("[0] %s: %s\n", record.Key, record.Value)
	}
	firstStore.EndReading()

	secondStore.BeginReading()
	for {
		record, err := secondStore.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("[1] %s: %s\n", record.Key, record.Value)
	}
	secondStore.EndReading()

	// Output:
	// [0] a: b
	// [0] e: f
	// [1] c: d
	// [1] g: h
}
