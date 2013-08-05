package transformer

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
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

type Pipeline []PipelineStage

func (pipeline Pipeline) StageNames() []string {
	var names []string
	for _, stage := range pipeline {
		names = append(names, stage.Name)
	}
	return names
}

var stagesDone *expvar.Int
var currentStage *expvar.String

func init() {
	stagesDone = expvar.NewInt("StagesComplete")
	currentStage = expvar.NewString("CurrentStage")
}

type PipelineFunc func(dbRoot string, workers int) Pipeline

// Convenience function to parse command line arguments, figure out which
// pipeline to run and configure that pipeline to run.
func ParsePipelineChoice(pipelineFuncs map[string]PipelineFunc) (string, Pipeline) {
	workers := flag.Int("workers", 4, "Number of worker threads for mappers.")
	runOnly := flag.String("run_only", "", "Comma separated list of stages to run.")
	runAfter := flag.String("run_from", "", "Run this stage and all stages following it.")
	listStages := flag.Bool("list_stages", false, "List the stages in the pipeline and exit.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s [global flags] <database root> <pipeline> [pipeline flags]:\n", os.Args[0])
		fmt.Fprintln(os.Stderr, " [global flags] can be:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, " <database root> is a directory where we store multiple LevelDB databases.")
		var pipelineNames []string
		for name := range pipelineFuncs {
			pipelineNames = append(pipelineNames, name)
		}
		sort.Strings(pipelineNames)
		fmt.Fprintln(os.Stderr, " <pipeline> is one of these:", strings.Join(pipelineNames, ", "))
		fmt.Fprintln(os.Stderr, " Pass '-help' to a pipeline to see [pipeline flags]")
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
		fmt.Fprintf(os.Stderr, "Invalid pipeline!\n\n")
		flag.Usage()
		os.Exit(1)
	}
	pipeline := pipelineFunc(dbRoot, *workers)
	if *listStages {
		fmt.Fprintln(os.Stderr, strings.Join(pipeline.StageNames(), "\n"))
		os.Exit(0)
	}
	if len(*runOnly) > 0 {
		stageNames := strings.Split(*runOnly, ",")
		var stagesToRun []PipelineStage
		for _, stageName := range stageNames {
			foundStage := false
			for _, stage := range pipeline {
				if stage.Name == stageName {
					stagesToRun = append(stagesToRun, stage)
					foundStage = true
					break
				}
			}
			if !foundStage {
				fmt.Fprintf(os.Stderr, "Invalid stage in pipeline %s\n", pipelineName)
				fmt.Fprintf(os.Stderr, "Possible stages:\n  %s\n", strings.Join(pipeline.StageNames(), "\n  "))
				os.Exit(1)
			}
		}
		return pipelineName, stagesToRun
	}
	if len(*runAfter) > 0 {
		stageNames := strings.Split(*runAfter, ",")
		for idx, stage := range pipeline {
			for _, stageName := range stageNames {
				if stage.Name == stageName {
					return pipelineName, pipeline[idx:]
				}
			}
		}
		fmt.Fprintf(os.Stderr, "Invalid stage in pipeline %s\n", pipelineName)
		fmt.Fprintf(os.Stderr, "Possible stages:\n  %s\n", strings.Join(pipeline.StageNames(), "\n  "))
		os.Exit(1)
	}
	return pipelineName, pipeline
}

// Run a set of pipeline stages. We run stages
// sequentially, with no parallelism between stages.
func RunPipeline(pipeline Pipeline) {
	for idx, stage := range pipeline {
		currentStage.Set(stage.Name)
		log.Printf("Running %s pipeline stage: %v", humanize.Ordinal(idx+1), stage.Name)
		RunTransformer(stage.Transformer, stage.Reader, stage.Writer)
		stagesDone.Add(1)
	}
	log.Printf("All stages complete")
}
