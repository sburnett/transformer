package transformer

import (
	"bytes"
)

// This is the type of general transformations on data stored in LevelDB. Use
// one of the more specialized transformers when possible since they can
// parallize computation across multiple cores.
type Transformer interface {
	Do(inputChan chan *LevelDbRecord, outputChan ...chan *LevelDbRecord)
}

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
	return TransformFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for inputRecord := range inputChan {
					outputRecord := mapper.Map(inputRecord)
					if outputRecord != nil {
						outputChans[0] <- outputRecord
					}
				}
				doneChan <- true
			}()
		}
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
	})
}

func MakeDoTransformer(doer Doer, numConcurrent int) Transformer {
	multiDoer := func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		if len(outputChans) != 1 {
			panic("MakeDoTransformer only accepts one output channel")
		}
		doer.Do(inputRecord, outputChans[0])
	}
	return MakeMultipleOutputsDoTransformer(MultipleOutputsDoFunc(multiDoer), numConcurrent)
}

func MakeMultipleOutputsDoTransformer(doer MultipleOutputsDoer, numConcurrent int) Transformer {
	return TransformFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range inputChan {
					doer.DoToMultipleOutputs(record, outputChans...)
				}
				doneChan <- true
			}()
		}
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
	})
}

func MakeGroupDoTransformer(doer GroupDoer, numConcurrent int) Transformer {
	multiGroupDoer := func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		if len(outputChans) != 1 {
			panic("MakeGroupDoTransformer only accepts one output channel")
		}
		doer.GroupDo(inputRecords, outputChans[0])
	}
	return MakeMultipleOutputsGroupDoFunc(multiGroupDoer, numConcurrent)
}

func MakeMultipleOutputsGroupDoTransformer(doer MultipleOutputsGroupDoer, numConcurrent int) Transformer {
	return TransformFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		groupedInputsChan := make(chan []*LevelDbRecord)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range groupedInputsChan {
					doer.GroupDoToMultipleOutputs(record, outputChans...)
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
	})
}

type TransformFunc func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord)

type MapFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)

type DoFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)

type MultipleOutputsDoFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord)

type GroupDoFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord)

type MultipleOutputsGroupDoFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord)

func (transformer TransformFunc) Do(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	transformer(inputChan, outputChans...)
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

func MakeMultipleOutputsDoFunc(multiDoFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMultipleOutputsDoTransformer(MultipleOutputsDoFunc(multiDoFunc), numConcurrent)
}

func MakeGroupDoFunc(doFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeGroupDoTransformer(GroupDoFunc(doFunc), numConcurrent)
}

func MakeMultipleOutputsGroupDoFunc(multiGroupDoFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMultipleOutputsGroupDoTransformer(MultipleOutputsGroupDoFunc(multiGroupDoFunc), numConcurrent)
}
