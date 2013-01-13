package transformer

import (
	"expvar"
	"log"
	"path/filepath"
)

type PipelineStage struct {
	Name        string
	Transformer Transformer
	InputDbs    []string
	OutputDbs   []string
	FirstKey    []byte
	LastKey     []byte
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
		log.Printf("Running pipeline stage %v (%v)", idx, stage.Name)
		inputDbPaths := make([]string, len(stage.InputDbs))
		for idx, inputDb := range stage.InputDbs {
			inputDbPaths[idx] = filepath.Join(dbRoot, inputDb)
		}
		outputDbPaths := make([]string, len(stage.OutputDbs))
		for idx, outputDb := range stage.OutputDbs {
			outputDbPaths[idx] = filepath.Join(dbRoot, outputDb)
		}
		RunTransformer(stage.Transformer, inputDbPaths, outputDbPaths, stage.FirstKey, stage.LastKey)
		stagesDone.Add(1)
	}
	log.Printf("Done all pipelines.")
}
