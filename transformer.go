package transformer

type LevelDbRecord struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
}

func (record *LevelDbRecord) Copy() *LevelDbRecord {
    return &LevelDbRecord{
        Key: []byte(record.Key),
        Value: []byte(record.Value),
        DatabaseIndex: record.DatabaseIndex,
    }
}

// This is the type of general transformations on data stored in LevelDB. Use
// one of the more specialized transformers when possible since they can
// parallize computation across multiple cores.
type Transformer interface {
	Do(inputChan, outputChan chan *LevelDbRecord)
}

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
