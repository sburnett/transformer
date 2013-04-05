package transformer

import (
	"bytes"
	"fmt"
)

func ExampleMapTransformer() {
	mapper := MakeMapFunc(func(inputRecord *LevelDbRecord) *LevelDbRecord {
		return &LevelDbRecord{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
	}, 1)

	inputChan := make(chan *LevelDbRecord, 3)
	inputChan <- makeLevelDbRecord("a", "b", 0)
	inputChan <- makeLevelDbRecord("c", "d", 0)
	inputChan <- makeLevelDbRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *LevelDbRecord, 3)

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

func ExampleDoTransformer() {
	doer := MakeDoFunc(func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
		outputChan <- &LevelDbRecord{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
		outputChan <- &LevelDbRecord{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
		}
	}, 1)

	inputChan := make(chan *LevelDbRecord, 3)
	inputChan <- makeLevelDbRecord("a", "b", 0)
	inputChan <- makeLevelDbRecord("c", "d", 0)
	inputChan <- makeLevelDbRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *LevelDbRecord, 6)

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

func ExampleMultipleOutputsDoTransformer() {
	multiDoer := MakeMultipleOutputsDoFunc(func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		outputChans[0] <- &LevelDbRecord{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
		}
		outputChans[1] <- &LevelDbRecord{
			Key:   inputRecord.Key,
			Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
		}
	}, 2, 1)

	inputChan := make(chan *LevelDbRecord, 3)
	inputChan <- makeLevelDbRecord("a", "b", 0)
	inputChan <- makeLevelDbRecord("c", "d", 0)
	inputChan <- makeLevelDbRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *LevelDbRecord, 6)

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

func ExampleGroupDoTransformer() {
	groupDoer := MakeGroupDoFunc(func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord) {
		for _, inputRecord := range inputRecords {
			switch inputRecord.DatabaseIndex {
			case 0:
				outputChan <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
				}
			case 1:
				outputChan <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
				}
			}
		}
	}, 1)

	inputChan := make(chan *LevelDbRecord, 4)
	inputChan <- makeLevelDbRecord("a", "b", 0)
	inputChan <- makeLevelDbRecord("c", "d", 1)
	inputChan <- makeLevelDbRecord("e", "f", 0)
	inputChan <- makeLevelDbRecord("g", "h", 1)
	close(inputChan)

	outputChan := make(chan *LevelDbRecord, 4)

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

func ExampleMultipleOutputsGroupDoTransformer() {
	multiGroupDoer := MakeMultipleOutputsGroupDoFunc(func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		for _, inputRecord := range inputRecords {
			switch inputRecord.DatabaseIndex {
			case 0:
				outputChans[0] <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(",")),
				}
				outputChans[1] <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte("/")),
				}
			case 1:
				outputChans[0] <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte(";")),
				}
				outputChans[1] <- &LevelDbRecord{
					Key:   inputRecord.Key,
					Value: bytes.Join([][]byte{inputRecord.Value, inputRecord.Value}, []byte("|")),
				}
			}
		}
	}, 2, 1)

	inputChan := make(chan *LevelDbRecord, 3)
	inputChan <- makeLevelDbRecord("a", "b", 0)
	inputChan <- makeLevelDbRecord("c", "d", 1)
	inputChan <- makeLevelDbRecord("e", "f", 0)
	close(inputChan)

	outputChan := make(chan *LevelDbRecord, 6)

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
