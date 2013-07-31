package transformer

// This represents a single entry in a LevelDb database and is the foundation of
// record processing.
//
// You will find github.com/sburnett/transformer/key very helpful for encoding
// and decoding Keys and Values.
type LevelDbRecord struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
}

// Perform a deep copy of a LevelDbRecord.
func (record *LevelDbRecord) Copy() *LevelDbRecord {
	return &LevelDbRecord{
		Key:           []byte(record.Key),
		Value:         []byte(record.Value),
		DatabaseIndex: record.DatabaseIndex,
	}
}

// This is the type of general transformations on data stored in LevelDB. Use
// one of the more specialized transformers when possible since they can
// parallize computation across multiple cores.
type Transformer interface {
	Do(inputChan, outputChan chan *LevelDbRecord)
}

// Read records from reader, pass them to transformer and read results from
// transformer and write them to writer. Do not exit until all records have been
// processed. Running transformers is a fundamental data processing operation,
// but you should almost never run this function directly. Instead, use
// RunPipeline to run a series of pipeline stages.
func RunTransformer(transformer Transformer, reader StoreReader, writer StoreWriter) {
	inputChan := make(chan *LevelDbRecord)
	outputChan := make(chan *LevelDbRecord)

	if reader != nil {
		go func() {
			if err := reader.BeginReading(); err != nil {
				panic(err)
			}
			for {
				record, err := reader.ReadRecord()
				if err != nil {
					panic(err)
				}
				if record == nil {
					break
				}
				inputChan <- record
			}
			if err := reader.EndReading(); err != nil {
				panic(err)
			}
			close(inputChan)
		}()
	}

	transformerDone := make(chan bool)
	if transformer != nil {
		go func() {
			transformer.Do(inputChan, outputChan)
			close(outputChan)
			transformerDone <- true
		}()
	} else {
		outputChan = inputChan
		go func() {
			transformerDone <- true
		}()
	}

	if writer != nil {
		if err := writer.BeginWriting(); err != nil {
			panic(err)
		}
		for record := range outputChan {
			if err := writer.WriteRecord(record); err != nil {
				panic(err)
			}
		}
		if err := writer.EndWriting(); err != nil {
			panic(err)
		}
	}

	<-transformerDone
}
