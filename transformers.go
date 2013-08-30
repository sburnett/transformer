package transformer

import (
	"bytes"
	"flag"
	"fmt"
	"runtime"

	"github.com/sburnett/transformer/store"
)

var workers int

func init() {
	cores := runtime.NumCPU()
	flag.IntVar(&workers, "workers", 2*cores, "Number of worker threads for mappers.")
}

// Restricts the maximum number of concurrent workers to one, which forces
// predictable unit test output for Mappers and Doers.
func RestrictWorkersForTests() {
	workers = 1
}

// Map each input record to 0 or 1 output records. This is the simplest and most
// efficient kind of transformer.
//
// Return nil to emit 0 records for a key.
//
// Map will be invoked concurrently from many goroutines, so access to shared
// state must be synchronized.
type Mapper interface {
	Map(inputRecord *store.Record) (outputRecord *store.Record)
}

// Map each input record to an arbitrary number of output records on one output
// channel. Do will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type Doer interface {
	Do(inputRecord *store.Record, outputChan chan *store.Record)
}

// Map each input record to many output records on multiple output channels.
// DoToMultipleOutputs will be invoked concurrently from many goroutines, so access to
// shared state must be synchronized.
type MultipleOutputsDoer interface {
	DoToMultipleOutputs(inputRecord *store.Record, outputChans ...chan *store.Record)
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
	GroupDo(inputRecords []*store.Record, outputChan chan *store.Record)
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
	GroupDoToMultipleOutputs(inputRecords []*store.Record, outputChans ...chan *store.Record)
}

// Turn a Mapper into a Transformer.
func MakeMapTransformer(mapper Mapper) Transformer {
	doFunc := func(inputRecord *store.Record, outputChan chan *store.Record) {
		outputChan <- mapper.Map(inputRecord)
	}
	return MakeDoFunc(doFunc)
}

// Turn a Doer into a Transformer.
func MakeDoTransformer(doer Doer) Transformer {
	if !flag.Parsed() {
		panic(fmt.Errorf("flags must be parsed"))
	}
	return TransformFunc(func(inputChan, outputChan chan *store.Record) {
		doneChan := make(chan bool)
		for i := 0; i < workers; i++ {
			go func() {
				for record := range inputChan {
					doer.Do(record, outputChan)
				}
				doneChan <- true
			}()
		}
		for i := 0; i < workers; i++ {
			<-doneChan
		}
	})
}

// Turn a MultpleOutputsDoer into a Transformer.
func MakeMultipleOutputsDoTransformer(doer MultipleOutputsDoer, numOutputs int) Transformer {
	doFunc := func(inputRecord *store.Record, outputChan chan *store.Record) {
		outputChans := make([]chan *store.Record, numOutputs, numOutputs)
		doneChan := make(chan bool)
		for idx := range outputChans {
			outputChans[idx] = make(chan *store.Record)
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
	return MakeDoFunc(doFunc)
}

// Turn a GroupDoer into a Transformer.
func MakeGroupDoTransformer(doer GroupDoer) Transformer {
	if !flag.Parsed() {
		panic(fmt.Errorf("flags must be parsed"))
	}
	return TransformFunc(func(inputChan, outputChan chan *store.Record) {
		doneChan := make(chan bool)
		groupedInputsChan := make(chan []*store.Record)
		for i := 0; i < workers; i++ {
			go func() {
				for record := range groupedInputsChan {
					doer.GroupDo(record, outputChan)
				}
				doneChan <- true
			}()
		}
		var currentKey []byte
		var currentRecords []*store.Record
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
		for i := 0; i < workers; i++ {
			<-doneChan
		}
	})
}

// Turn a MultipleOutputsGroupDoer into a Transformer.
func MakeMultipleOutputsGroupDoTransformer(doer MultipleOutputsGroupDoer, numOutputs int) Transformer {
	groupDoFunc := func(inputRecords []*store.Record, outputChan chan *store.Record) {
		outputChans := make([]chan *store.Record, numOutputs, numOutputs)
		doneChan := make(chan bool)
		for idx := range outputChans {
			outputChans[idx] = make(chan *store.Record)
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
	return MakeGroupDoFunc(groupDoFunc)
}

type TransformFunc func(inputChan, outputChan chan *store.Record)

type MapFunc func(inputRecord *store.Record) (outputRecord *store.Record)

type DoFunc func(inputRecord *store.Record, outputChan chan *store.Record)

type MultipleOutputsDoFunc func(inputRecord *store.Record, outputChans ...chan *store.Record)

type GroupDoFunc func(inputRecords []*store.Record, outputChan chan *store.Record)

type MultipleOutputsGroupDoFunc func(inputRecords []*store.Record, outputChans ...chan *store.Record)

func (transformer TransformFunc) Do(inputChan, outputChan chan *store.Record) {
	transformer(inputChan, outputChan)
}

func (mapFunc MapFunc) Map(inputRecord *store.Record) *store.Record {
	return mapFunc(inputRecord)
}

func (doFunc DoFunc) Do(inputRecord *store.Record, outputChan chan *store.Record) {
	doFunc(inputRecord, outputChan)
}

func (multipleOutputsDoFunc MultipleOutputsDoFunc) DoToMultipleOutputs(inputRecord *store.Record, outputChans ...chan *store.Record) {
	multipleOutputsDoFunc(inputRecord, outputChans...)
}

func (groupDoFunc GroupDoFunc) GroupDo(inputRecords []*store.Record, outputChan chan *store.Record) {
	groupDoFunc(inputRecords, outputChan)
}

func (multipleOutputsGroupDoFunc MultipleOutputsGroupDoFunc) GroupDoToMultipleOutputs(inputRecords []*store.Record, outputChans ...chan *store.Record) {
	multipleOutputsGroupDoFunc(inputRecords, outputChans...)
}

// Turn a MapFunc into a Transformer.
func MakeMapFunc(mapperFunc MapFunc) Transformer {
	return MakeMapTransformer(MapFunc(mapperFunc))
}

// Turn a DoFunc into a Transformer.
func MakeDoFunc(doFunc DoFunc) Transformer {
	return MakeDoTransformer(DoFunc(doFunc))
}

// Turn a MultipleOutputsDoFunc into a Transformer.
func MakeMultipleOutputsDoFunc(multiDoFunc MultipleOutputsDoFunc, numOutputs int) Transformer {
	return MakeMultipleOutputsDoTransformer(MultipleOutputsDoFunc(multiDoFunc), numOutputs)
}

// Turn GroupDoFunc into a Transformer.
func MakeGroupDoFunc(doFunc GroupDoFunc) Transformer {
	return MakeGroupDoTransformer(GroupDoFunc(doFunc))
}

// Turn a MultipleOutputsGroupDoFunc into a Transformer.
func MakeMultipleOutputsGroupDoFunc(multiGroupDoFunc MultipleOutputsGroupDoFunc, numOutputs int) Transformer {
	return MakeMultipleOutputsGroupDoTransformer(MultipleOutputsGroupDoFunc(multiGroupDoFunc), numOutputs)
}
