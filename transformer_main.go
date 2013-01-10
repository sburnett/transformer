package transformer

import (
	"flag"
	"fmt"
	"github.com/sburnett/cube"
	"log"
	"os"
	"strings"
)

func TransformerMain(monitorPrefix string, transformerMap map[string]Transformer) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <transform> <input leveldb> <input table> <output leveldb>:\n", os.Args[0])
		transformerNames := []string{}
		for name, _ := range transformerMap {
			transformerNames = append(transformerNames, name)
		}
		fmt.Fprintf(os.Stderr, "Available transforms: %v\n", strings.Join(transformerNames, ", "))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 4 {
		flag.Usage()
		return
	}
	transform := flag.Arg(0)
	inputDbPath := flag.Arg(1)
	inputTable := flag.Arg(2)
	outputDbPath := flag.Arg(3)

	go cube.Run(fmt.Sprintf("%s_%s", monitorPrefix, transform))

	transformer, ok := transformerMap[transform]
	if !ok {
		flag.Usage()
		log.Fatalf("Invalid transform.")
	}
	RunTransformer(transformer, inputDbPath, inputTable, outputDbPath)
}
