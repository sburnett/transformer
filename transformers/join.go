package transformers

import (
	"bytes"
	"fmt"

	"github.com/sburnett/transformer/store"
)

// Join multiple stores on identical keys by concatenating values. Typically,
// you demultiplex a set of stores using store.DemuxingReader before joining.
//
// This function's parameters are the default values to use when a store is
// missing a record for a key present in the other stores. If a default is nil,
// then Join will omit that key entirely. You can use this feature to
// construct left, right, inner and outer joins. See the examples.
func Join(defaults ...[]byte) func([]*store.Record, chan *store.Record) {
	numTables := len(defaults)
	return func(inputRecords []*store.Record, outputChan chan *store.Record) {
		values := make([][]byte, numTables)
		for _, record := range inputRecords {
			if int(record.DatabaseIndex) > len(values) {
				panic(fmt.Errorf("Number of defaults and number of tables don't match in Join"))
			}
			values[record.DatabaseIndex] = record.Value
		}
		for idx := 0; idx < numTables; idx++ {
			if values[idx] != nil {
				continue
			}
			if defaults[idx] == nil {
				return
			}
			values[idx] = defaults[idx]
		}
		valueBytes := bytes.Join(values, []byte{})
		outputChan <- &store.Record{
			Key:   inputRecords[0].Key,
			Value: valueBytes,
		}
	}
}
