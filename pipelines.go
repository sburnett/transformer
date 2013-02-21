package transformer

import (
	"expvar"
	"log"
	"path/filepath"
	"sort"
)

type PipelineStage struct {
	Name        string
	Transformer Transformer
	InputDbs    []string
	OutputDbs   []string
	FirstKey    []byte
	LastKey     []byte
	OnlyKeys    bool
}

var stagesDone *expvar.Int
var currentStage *expvar.String

func init() {
	stagesDone = expvar.NewInt("StagesComplete")
	currentStage = expvar.NewString("CurrentStage")
}

func RunPipeline(dbRoot string, stages []PipelineStage, skipStages int) {
	for idx, stage := range stages[skipStages:] {
		currentStage.Set(stage.Name)
		log.Printf("Running pipeline stage %v (%v)", idx+skipStages, stage.Name)
		inputDbPaths := make([]string, len(stage.InputDbs))
		for idx, inputDb := range stage.InputDbs {
			inputDbPaths[idx] = filepath.Join(dbRoot, inputDb)
		}
		outputDbPaths := make([]string, len(stage.OutputDbs))
		for idx, outputDb := range stage.OutputDbs {
			outputDbPaths[idx] = filepath.Join(dbRoot, outputDb)
		}
		RunTransformer(stage.Transformer, inputDbPaths, outputDbPaths, stage.FirstKey, stage.LastKey, stage.OnlyKeys)
		stagesDone.Add(1)
	}
	log.Printf("Done all pipelines.")
}

func RunPipelineWithoutLevelDb(stages []PipelineStage, databases map[string]map[string]string, channelSize int) {
	for _, stage := range stages {
		inputChans := make([]chan *LevelDbRecord, len(stage.InputDbs))
		for databaseIndex, inputDb := range stage.InputDbs {
			inputChans[databaseIndex] = make(chan *LevelDbRecord, len(databases[inputDb]))
			inputKeys := make([]string, 0)
			for key := range databases[inputDb] {
				inputKeys = append(inputKeys, key)
			}
			sort.Sort(sort.StringSlice(inputKeys))
			for _, key := range inputKeys {
				value := databases[inputDb][key]
				inputChans[databaseIndex] <- &LevelDbRecord{
					Key:           []byte(key),
					Value:         []byte(value),
					DatabaseIndex: uint8(databaseIndex),
				}
			}
			close(inputChans[databaseIndex])
		}
		inputChan := make(chan *LevelDbRecord, channelSize)
		demuxInputsSorted(inputChans, inputChan)
		outputChans := make([]chan *LevelDbRecord, len(stage.OutputDbs))
		for idx := range outputChans {
			outputChans[idx] = make(chan *LevelDbRecord, channelSize)
		}
		stage.Transformer.Do(inputChan, outputChans...)
		for idx, outputChan := range outputChans {
			close(outputChan)
			outputDbName := stage.OutputDbs[idx]
			_, ok := databases[outputDbName]
			if !ok {
				databases[outputDbName] = make(map[string]string)
			}
			database := databases[outputDbName]
			for record := range outputChan {
				database[string(record.Key)] = string(record.Value)
			}
		}
	}
}
