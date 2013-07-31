package store

import (
	"fmt"
)

func ExampleDemuxingSeeker() {
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

	reader := NewDemuxingSeeker(&firstStore, &secondStore, &thirdStore)
	reader.BeginReading()
	readRecords := func(count int) {
		for i := 0; i < count; i++ {
			record, err := reader.ReadRecord()
			if err != nil {
				panic(err)
			}
			if record == nil {
				break
			}
			fmt.Printf("%s: %s\n", record.Key, record.Value)
		}
	}
	readRecords(3)
	fmt.Printf("SEEK\n")
	reader.Seek([]byte("b"))
	readRecords(3)
	fmt.Printf("SEEK\n")
	reader.Seek([]byte("g"))
	readRecords(3)
	reader.EndReading()

	// Output:
	// a: foo2
	// b: bar2
	// c: baz2
	// SEEK
	// b: bar2
	// c: baz2
	// d: foo0
	// SEEK
	// g: bar1
	// h: baz0
	// i: baz1
}
