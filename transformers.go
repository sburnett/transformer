package transformer

import (
	"bytes"
)

type TransformerFunc func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord)

func (transformer TransformerFunc) Do(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	transformer(inputChan, outputChans...)
}

// A convenience type for Transformers that can process multiple input records
// in parallel. DoMulti will be called concurrently from multiple goroutines, so
// access to internal state of the MultiDoer must be threadsafe.
type MultiDoer interface {
	DoMulti(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord)
}

type DoMultiFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord)

func (doFunc DoMultiFunc) DoMulti(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	doFunc(inputRecord, outputChans...)
}

func MakeMultiDoTransformer(doer MultiDoer, numConcurrent int) Transformer {
	return TransformerFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range inputChan {
					doer.DoMulti(record, outputChans...)
				}
				doneChan <- true
			}()
		}
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
	})
}

func MakeMultiDoTransformerFunc(multiDoFunc func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMultiDoTransformer(DoMultiFunc(multiDoFunc), numConcurrent)
}

// A convenience type for Transformers that can process multiple input records
// in parallel, but wish to process records with identical keys together (e.g.,
// when joining data from multiple databases.) GroupDoMulti will be called
// concurrently from multiple goroutines, so access to internal state of the
// MultiGrouper must be threadsafe.
type MultiGrouper interface {
	GroupDoMulti(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord)
}

type GroupDoMultiFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord)

func (doFunc GroupDoMultiFunc) GroupDoMulti(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord) {
	doFunc(inputRecords, outputChans...)
}

func MakeMultiGroupDoTransformer(doer MultiGrouper, numConcurrent int) Transformer {
	return TransformerFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		groupedInputsChan := make(chan []*LevelDbRecord)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range groupedInputsChan {
					doer.GroupDoMulti(record, outputChans...)
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
		for i := 0; i < numConcurrent; i++ {
			<-doneChan
		}
	})
}

func MakeMultiGroupDoTransformerFunc(multiGroupDoFunc func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMultiGroupDoTransformer(GroupDoMultiFunc(multiGroupDoFunc), numConcurrent)
}

// A convenience type for MultiDoers that only emit to one output channel.
type Doer interface {
	Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)
}

type DoFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)

func (doFunc DoFunc) Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
	doFunc(inputRecord, outputChan)
}

func MakeDoTransformer(doer Doer, numConcurrent int) Transformer {
	multiDoer := func(inputRecord *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		if len(outputChans) != 1 {
			panic("MakeDoTransformer only accepts one output channel")
		}
		doer.Do(inputRecord, outputChans[0])
	}
	return MakeMultiDoTransformer(DoMultiFunc(multiDoer), numConcurrent)
}

func MakeDoTransformerFunc(doFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeDoTransformer(DoFunc(doFunc), numConcurrent)
}

// A convenience type for MultiGroupDoers that only emit to one output channel.
type GroupDoer interface {
	GroupDo(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord)
}

type GroupDoFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord)

func (doFunc GroupDoFunc) GroupDo(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord) {
	doFunc(inputRecords, outputChan)
}

func MakeGroupDoTransformer(doer GroupDoer, numConcurrent int) Transformer {
	multiGroupDoer := func(inputRecords []*LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		if len(outputChans) != 1 {
			panic("MakeGroupDoTransformer only accepts one output channel")
		}
		doer.GroupDo(inputRecords, outputChans[0])
	}
	return MakeMultiGroupDoTransformer(GroupDoMultiFunc(multiGroupDoer), numConcurrent)
}

func MakeGroupDoTransformerFunc(doFunc func(inputRecords []*LevelDbRecord, outputChan chan *LevelDbRecord), numConcurrent int) Transformer {
	return MakeGroupDoTransformer(GroupDoFunc(doFunc), numConcurrent)
}

// A convenience type for Transformers that can process multiple input records
// in parallel. MapMulti will be called concurrently from multiple goroutines,
// so access to internal state of the MultiMapper must be threadsafe.
type MultiMapper interface {
	MapMulti(inputRecord *LevelDbRecord) (outputRecord []*LevelDbRecord)
}

type MultiMapperFunc func(inputRecord *LevelDbRecord) (outputRecord []*LevelDbRecord)

func (mapperFunc MultiMapperFunc) MapMulti(inputRecord *LevelDbRecord) []*LevelDbRecord {
	return mapperFunc(inputRecord)
}

func MakeMultiMapper(mapper MultiMapper, numConcurrent int) Transformer {
	return TransformerFunc(func(inputChan chan *LevelDbRecord, outputChans ...chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range inputChan {
					results := mapper.MapMulti(record)
					for idx, result := range results {
						if results[idx] != nil {
							outputChans[idx] <- result
						}
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

func MakeMultiMapperFunc(multiMapperFunc func(inputRecord *LevelDbRecord) (outputRecord []*LevelDbRecord), numConcurrent int) Transformer {
	return MakeMultiMapper(MultiMapperFunc(multiMapperFunc), numConcurrent)
}

// A convenience type for MultiMappers that only return one type of output.
type Mapper interface {
	Map(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)
}

type MapperFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)

func (mapperFunc MapperFunc) Map(inputRecord *LevelDbRecord) *LevelDbRecord {
	return mapperFunc(inputRecord)
}

func MakeMapper(mapper Mapper, numConcurrent int) Transformer {
	multiMapper := func(inputRecord *LevelDbRecord) []*LevelDbRecord {
		return []*LevelDbRecord{mapper.Map(inputRecord)}
	}
	return MakeMultiMapper(MultiMapperFunc(multiMapper), numConcurrent)
}

func MakeMapperFunc(mapperFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord), numConcurrent int) Transformer {
	return MakeMapper(MapperFunc(mapperFunc), numConcurrent)
}
