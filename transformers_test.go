package transformer

import (
	"bytes"
	"fmt"

	"github.com/sburnett/transformer/store"
)

func ExampleMapper() {
	mapper := MakeMapFunc(func(inputRecord *store.Record) *store.Record {
		return &store.Record{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
	}, 1)

	inputChan := make(chan *store.Record, 3)
	inputChan <- store.NewRecord("a", "b", 0)
	inputChan <- store.NewRecord("c", "d", 0)
	inputChan <- store.NewRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *store.Record, 3)

	mapper.Do(inputChan, outputChan)
	close(outputChan)

	for record := range outputChan {
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}

	// Output:
	// a: b,b
	// c: d,d
	// e: f,f
}

func ExampleDoer() {
	doer := MakeDoFunc(func(inputRecord *store.Record, outputChan chan *store.Record) {
		outputChan <- &store.Record{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
		outputChan <- &store.Record{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
		}
	}, 1)

	inputChan := make(chan *store.Record, 3)
	inputChan <- store.NewRecord("a", "b", 0)
	inputChan <- store.NewRecord("c", "d", 0)
	inputChan <- store.NewRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *store.Record, 6)

	doer.Do(inputChan, outputChan)
	close(outputChan)

	for record := range outputChan {
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}

	// Output:
	// a: b,b
	// a: b;b
	// c: d,d
	// c: d;d
	// e: f,f
	// e: f;f
}

func ExampleMultipleOutputsDoer() {
	multiDoer := MakeMultipleOutputsDoFunc(func(inputRecord *store.Record, outputChans ...chan *store.Record) {
		outputChans[0] <- &store.Record{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
		outputChans[1] <- &store.Record{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
		}
	}, 2, 1)

	inputChan := make(chan *store.Record, 3)
	inputChan <- store.NewRecord("a", "b", 0)
	inputChan <- store.NewRecord("c", "d", 0)
	inputChan <- store.NewRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *store.Record, 6)

	multiDoer.Do(inputChan, outputChan)
	close(outputChan)

	for record := range outputChan {
		fmt.Printf("[%d] %s: %s\n", record.DatabaseIndex, record.Key, record.Value)
	}

	// Output:
	// [0] a: b,b
	// [1] a: b;b
	// [0] c: d,d
	// [1] c: d;d
	// [0] e: f,f
	// [1] e: f;f
}

func ExampleGroupDoer() {
	groupDoer := MakeGroupDoFunc(func(inputRecords []*store.Record, outputChan chan *store.Record) {
		for _, inputRecord := range inputRecords {
			switch inputRecord.DatabaseIndex {
			case 0:
				outputChan <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
				}
			case 1:
				outputChan <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
				}
			}
		}
	}, 1)

	inputChan := make(chan *store.Record, 4)
	inputChan <- store.NewRecord("a", "b", 0)
	inputChan <- store.NewRecord("c", "d", 1)
	inputChan <- store.NewRecord("e", "f", 0)
	inputChan <- store.NewRecord("g", "h", 1)
	close(inputChan)

	outputChan := make(chan *store.Record, 4)

	groupDoer.Do(inputChan, outputChan)
	close(outputChan)

	for record := range outputChan {
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}

	// Output:
	// a: b,b
	// c: d;d
	// e: f,f
	// g: h;h
}

func ExampleMultipleOutputsGroupDoer() {
	multiGroupDoer := MakeMultipleOutputsGroupDoFunc(func(inputRecords []*store.Record, outputChans ...chan *store.Record) {
		for _, inputRecord := range inputRecords {
			switch inputRecord.DatabaseIndex {
			case 0:
				outputChans[0] <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
				}
				outputChans[1] <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte("/")),
				}
			case 1:
				outputChans[0] <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
				}
				outputChans[1] <- &store.Record{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte("|")),
				}
			}
		}
	}, 2, 1)

	inputChan := make(chan *store.Record, 3)
	inputChan <- store.NewRecord("a", "b", 0)
	inputChan <- store.NewRecord("c", "d", 1)
	inputChan <- store.NewRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *store.Record, 6)

	multiGroupDoer.Do(inputChan, outputChan)
	close(outputChan)

	for record := range outputChan {
		fmt.Printf("[%d] %s: %s\n", record.DatabaseIndex, record.Key, record.Value)
	}

	// Output:
	// [0] a: b,b
	// [1] a: b/b
	// [0] c: d;d
	// [1] c: d|d
	// [0] e: f,f
	// [1] e: f/f
}
