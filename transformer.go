package transformer

type LevelDbRecord struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
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
			if err := reader.Read(inputChan); err != nil {
				panic(err)
			}
		}()
	}
	if transformer != nil {
		go func() {
			transformer.Do(inputChan, outputChan)
		}()
	} else {
		outputChan = inputChan
	}
	if writer != nil {
		if err := writer.Write(outputChan); err != nil {
			panic(err)
		}
	}
}
