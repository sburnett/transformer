package transformer

import (
	"bytes"
)

// Map each input record to 0 or 1 output records. This is the simplest and most
// efficient kind of transformer.
//
// Return nil to emit 0 records for a key.
//
// Map will be invoked concurrently from many goroutines, so access to shared
// state must be synchronized.
type Mapper interface {
	Map(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)
}

// Map each input record to an arbitrary number of output records on one output
// channel. Do will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type Doer interface {
	Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)
}

// Map each input record to many output records on multiple output channels.
// DoToMultipleOutputs will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type MultipleOutputsDoer interface {
	DoToMultipleOutputs(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord)
}

// Map input records from several sources to an arbitrary number of output
// records on one output channel. Several input sources might have mappings for
// the same key; we combine those into a single inputRecords slice.
//
// Invariants:
// inputRecords[i].Key == inputRecords[j].Key for all i, j
// inputRecords[i].DatabaseIndex != inputRecords[j].DatabaseIndex for all i != j
//
// GroupDo will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type GroupDoer interface {
	GroupDo(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord)
}

// Map input records from several sources to an arbitrary number of output
// records on multiple output channels. Several input sources might have
// mappings for the same key; we combine those into a single inputRecords slice.
//
// Invariants:
// inputRecords[i].Key == inputRecords[j].Key for all i, j
// inputRecords[i].DatabaseIndex != inputRecords[j].DatabaseIndex for all i != j
//
// GroupDo will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type MultipleOutputsGroupDoer interface {
	GroupDoToMultipleOutputs(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord)
}

func MakeMapTransformer(mapper Mapper, numConcurrent int) Transformer {
	doFunc := func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
		outputChan <- mapper.Map(inputRecord)
	}
	return MakeDoFunc(doFunc, numConcurrent)
}

func MakeDoTransformer(doer Doer, numConcurrent int) Transformer {
	return TransformFunc(func(inputChan, outputChan chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range inputChan {
					doer.Do(record, outputChan)
				}
				doneChan <- true
			}()
		}
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
		close(outputChan)
	})
}

func MakeMultipleOutputsDoTransformer(doer MultipleOutputsDoer, numOutputs, numConcurrent int) Transformer {
	doFunc := func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
		outputChans := make([]chan *LevelDbRecord, numOutputs, numOutputs)
		doneChan := make(chan bool)
		for idx := range outputChans {
			outputChans[idx] = make(chan *LevelDbRecord)
		}
		for i := 0; i < numOutputs; i++ {
			go func(idx int) {
				for record := range outputChans[idx] {
					record.DatabaseIndex = uint8(idx)
					outputChan <- record
				}
				doneChan <- true
			}(i)
		}
		doer.DoToMultipleOutputs(inputRecord, outputChans...)
		for _, outputChan := range outputChans {
			close(outputChan)
		}
		for i := 0; i < numOutputs; i++ {
			<-doneChan
		}
	}
	return MakeDoFunc(doFunc, numConcurrent)
}

func MakeGroupDoTransformer(doer GroupDoer, numConcurrent int) Transformer {
	return TransformFunc(func(inputChan, outputChan chan *LevelDbRecord) {
		doneChan := make(chan bool)
		groupedInputsChan := make(chan []*LevelDbRecord)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range groupedInputsChan {
					doer.GroupDo(record, outputChan)
				}
				doneChan <- true
			}()
		}
		var currentKey []byte
		var currentRecords []*LevelDbRecord
		for record := range inputChan {
			if currentKey == nil {
				currentKey = record.Key
			}
			if !bytes.Equal(currentKey, record.Key) {
				groupedInputsChan <- currentRecords
				currentKey = record.Key
				currentRecords = nil
			}
			currentRecords = append(currentRecords, record)
		}
		if currentRecords != nil {
			groupedInputsChan <- currentRecords
		}
		close(groupedInputsChan)
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
		close(outputChan)
	})
}

func MakeMultipleOutputsGroupDoTransformer(doer MultipleOutputsGroupDoer, numOutputs, numConcurrent int) Transformer {
	groupDoFunc := func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord) {
		outputChans := make([]chan *LevelDbRecord, numOutputs, numOutputs)
		doneChan := make(chan bool)
		for idx := range outputChans {
			outputChans[idx] = make(chan *LevelDbRecord)
			go func(idx int) {
				for record := range outputChans[idx] {
					record.DatabaseIndex = uint8(idx)
					outputChan <- record
				}
				doneChan <- true
			}(idx)
		}
		doer.GroupDoToMultipleOutputs(inputRecords, outputChans...)
		for _, outputChan := range outputChans {
			close(outputChan)
		}
		for i := 0; i < numOutputs; i++ {
			<-doneChan
		}
	}
	return MakeGroupDoFunc(groupDoFunc, numConcurrent)
}

type TransformFunc func(inputChan, outputChan chan *LevelDbRecord)

type MapFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)

type DoFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)

type MultipleOutputsDoFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord)

type GroupDoFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord)

type MultipleOutputsGroupDoFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord)

func (transformer TransformFunc) Do(inputChan, outputChan chan *LevelDbRecord) {
	transformer(inputChan, outputChan)
}

func (mapFunc MapFunc) Map(inputRecord *LevelDbRecord) *LevelDbRecord {
	return mapFunc(inputRecord)
}

func (doFunc DoFunc) Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
	doFunc(inputRecord, outputChan)
}

func (multipleOutputsDoFunc MultipleOutputsDoFunc) DoToMultipleOutputs(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	multipleOutputsDoFunc(inputRecord, outputChans...)
}

func (groupDoFunc GroupDoFunc) GroupDo(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord) {
	groupDoFunc(inputRecords, outputChan)
}

func (multipleOutputsGroupDoFunc MultipleOutputsGroupDoFunc) GroupDoToMultipleOutputs(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	multipleOutputsGroupDoFunc(inputRecords, outputChans...)
}

func MakeMapFunc(mapperFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMapTransformer(MapFunc(mapperFunc), numConcurrent)
}

func MakeDoFunc(doFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeDoTransformer(DoFunc(doFunc), numConcurrent)
}

func MakeMultipleOutputsDoFunc(multiDoFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord), numOutputs, numConcurrent int) Transformer {
	return MakeMultipleOutputsDoTransformer(MultipleOutputsDoFunc(multiDoFunc), numOutputs, numConcurrent)
}

func MakeGroupDoFunc(doFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeGroupDoTransformer(GroupDoFunc(doFunc), numConcurrent)
}

func MakeMultipleOutputsGroupDoFunc(multiGroupDoFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord), numOutputs, numConcurrent int) Transformer {
	return MakeMultipleOutputsGroupDoTransformer(MultipleOutputsGroupDoFunc(multiGroupDoFunc), numOutputs, numConcurrent)
}
