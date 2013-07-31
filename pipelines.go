package transformer

import (
	"expvar"
	"log"

	"github.com/sburnett/transformer/store"
)

// A pipeline stage is a single step of data processing, which reads data from
// Reader, sends each record to Transformer, and writes the resulting Records to
// Writer. The Name is purely informational.
type PipelineStage struct {
	Name        string
	Transformer Transformer
	Reader      store.Reader
	Writer      store.Writer
}

var stagesDone *expvar.Int
var currentStage *expvar.String

func init() {
	stagesDone = expvar.NewInt("StagesComplete")
	currentStage = expvar.NewString("CurrentStage")
}

// Run a set of pipeline stages, skipping the first skipStages. We run stages
// sequentially, with no parallelism between stages.
func RunPipeline(stages []PipelineStage, skipStages int) {
	for idx, stage := range stages[skipStages:] {
		currentStage.Set(stage.Name)
		log.Printf("Running pipeline stage %v (%v)", idx+skipStages, stage.Name)
		RunTransformer(stage.Transformer, stage.Reader, stage.Writer)
		stagesDone.Add(1)
	}
	log.Printf("Done all pipelines.")
}
