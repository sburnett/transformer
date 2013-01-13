package transformer

type TransformerFunc func(inputChan, outputChan chan *LevelDbRecord)

func (transformer TransformerFunc) Do(inputChan, outputChan chan *LevelDbRecord) {
	transformer(inputChan, outputChan)
}

type Doer interface {
	Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)
}

type DoFunc func(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord)

func (doFunc DoFunc) Do(inputRecord *LevelDbRecord, outputChan chan *LevelDbRecord) {
	doFunc(inputRecord, outputChan)
}

func MakeDoTransformer(doer Doer, numConcurrent int) Transformer {
	return TransformerFunc(func(inputChan, outputChan chan *LevelDbRecord) {
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
	})
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

func MakeMapperFunc(mapper MapperFunc, numConcurrent int) Transformer {
	return MakeMapper(MapperFunc(mapper), numConcurrent)
}
