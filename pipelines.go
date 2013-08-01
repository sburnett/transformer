package transformer

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

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

type PipelineFunc func(dbRoot string, workers int) []PipelineStage

// Convenience function to parse command line arguments, figure out which
// pipeline to run and configure that pipeline to run.
func ParsePipelineChoice(pipelineFuncs map[string]PipelineFunc) (string, []PipelineStage) {
	workers := flag.Int("workers", 4, "Number of worker threads for mappers.")
	skipStages := flag.Int("skip_stages", 0, "Skip this many stages at the beginning of the pipeline.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <db root> <pipeline>:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}
	dbRoot := flag.Arg(0)
	pipelineName := flag.Arg(1)

	pipelineFunc, ok := pipelineFuncs[pipelineName]
	if !ok {
		flag.Usage()
		var pipelineNames []string
		for name := range pipelineFuncs {
			pipelineNames = append(pipelineNames, name)
		}
		sort.Strings(pipelineNames)
		fmt.Println("Possible pipelines:", strings.Join(pipelineNames, ", "))
		os.Exit(1)
	}
	pipeline := pipelineFunc(dbRoot, *workers)
	return pipelineName, pipeline[*skipStages:]
}

// Run a set of pipeline stages. We run stages
// sequentially, with no parallelism between stages.
func RunPipeline(pipeline []PipelineStage) {
	for idx, stage := range pipeline {
		currentStage.Set(stage.Name)
		log.Printf("Running pipeline stage %v (%v)", idx, stage.Name)
		RunTransformer(stage.Transformer, stage.Reader, stage.Writer)
		stagesDone.Add(1)
	}
	log.Printf("Done all pipelines.")
}
