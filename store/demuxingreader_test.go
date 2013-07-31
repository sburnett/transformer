package store

import (
	"fmt"
)

func ExampleDemuxingReader() {
	firstStore := SliceStore{}
	firstStore.BeginWriting()
	firstStore.WriteRecord(NewRecord("d", "foo0", 0))
	firstStore.WriteRecord(NewRecord("f", "bar0", 0))
	firstStore.WriteRecord(NewRecord("h", "baz0", 0))
	firstStore.EndWriting()

	secondStore := SliceStore{}
	secondStore.BeginWriting()
	secondStore.WriteRecord(NewRecord("e", "foo1", 1))
	secondStore.WriteRecord(NewRecord("g", "bar1", 1))
	secondStore.WriteRecord(NewRecord("i", "baz1", 1))
	secondStore.EndWriting()

	thirdStore := SliceStore{}
	thirdStore.BeginWriting()
	thirdStore.WriteRecord(NewRecord("a", "foo2", 2))
	thirdStore.WriteRecord(NewRecord("b", "bar2", 2))
	thirdStore.WriteRecord(NewRecord("c", "baz2", 2))
	thirdStore.EndWriting()

	reader := NewDemuxingReader(&firstStore, &secondStore, &thirdStore)
	reader.BeginReading()
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	reader.EndReading()

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

func ExampleDemuxingReader_duplicateKeys() {
	firstStore := SliceStore{}
	firstStore.BeginWriting()
	firstStore.WriteRecord(NewRecord("a", "foo0", 0))
	firstStore.WriteRecord(NewRecord("b", "bar0", 0))
	firstStore.WriteRecord(NewRecord("c", "baz0", 0))
	firstStore.EndWriting()

	secondStore := SliceStore{}
	secondStore.BeginWriting()
	secondStore.WriteRecord(NewRecord("b", "foo1", 1))
	secondStore.WriteRecord(NewRecord("c", "bar1", 1))
	secondStore.EndWriting()

	reader := NewDemuxingReader(&firstStore, &secondStore)
	reader.BeginReading()
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	reader.EndReading()

	// Output:
	// a: foo0
	// b: bar0
	// b: foo1
	// c: baz0
	// c: bar1
}
