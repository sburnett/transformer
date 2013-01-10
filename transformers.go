package transformer

type TransformerFunc func(inputChan, outputChan chan *LevelDbRecord)

func (transformer TransformerFunc) Do(inputChan, outputChan chan *LevelDbRecord) {
	transformer(inputChan, outputChan)
}

type Mapper interface {
	Map(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)
}

type MapperFunc func(inputRecord *LevelDbRecord) (outputRecord *LevelDbRecord)

func (mapperFunc MapperFunc) Map(inputRecord *LevelDbRecord) *LevelDbRecord {
	return mapperFunc(inputRecord)
}

func MakeMapper(mapper Mapper, numConcurrent int) Transformer {
	return TransformerFunc(func(inputChan, outputChan chan *LevelDbRecord) {
		doneChan := make(chan bool)
		for i := 0; i < numConcurrent; i++ {
			go func() {
				for record := range inputChan {
					result := mapper.Map(record)
					if result != nil {
						outputChan <- result
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
