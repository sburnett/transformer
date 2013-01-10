package transformer

import (
	"expvar"
	"log"
	"path/filepath"
)

type PipelineStage struct {
	Transformer Transformer
	InputDb     string
	InputTable  string
	OutputDb    string
}

var stagesDone *expvar.Int

func init() {
	stagesDone = expvar.NewInt("StagesComplete")
}

func RunPipeline(dbRoot string, stages []PipelineStage, skipStages int) {
	for idx, stage := range stages[skipStages:] {
		log.Printf("Running pipeline stage %v", idx)
		inputDbPath := filepath.Join(dbRoot, stage.InputDb)
		outputDbPath := filepath.Join(dbRoot, stage.OutputDb)
		RunTransformer(stage.Transformer, inputDbPath, stage.InputTable, outputDbPath)
		stagesDone.Add(1)
	}
	log.Printf("Done all pipelines.")
}
