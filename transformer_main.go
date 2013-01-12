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
	firstKey := flag.String("first_key", "", "Read from the input leveldb starting at this key. Overridden by --key_prefix.")
	lastKey := flag.String("last_key", "", "Read from the input leveldb up to and including this key. Overridden by --key_prefix.")
	keyPrefix := flag.String("key_prefix", "", "Read all keys with this prefix. Overrides firstKey and --last_key.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <transform> <input leveldb> <output leveldb> <first key> <last key>:\n", os.Args[0])
		transformerNames := []string{}
		for name, _ := range transformerMap {
			transformerNames = append(transformerNames, name)
		}
		fmt.Fprintf(os.Stderr, "Available transforms: %v\n", strings.Join(transformerNames, ", "))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 3 {
		flag.Usage()
		return
	}
	transform := flag.Arg(0)
	inputDbPath := flag.Arg(1)
	outputDbPath := flag.Arg(2)

	go cube.Run(fmt.Sprintf("%s_%s", monitorPrefix, transform))

	transformer, ok := transformerMap[transform]
	if !ok {
		flag.Usage()
		log.Fatalf("Invalid transform.")
	}
	if *keyPrefix == "" {
		RunTransformer(transformer, inputDbPath, outputDbPath, []byte(*firstKey), []byte(*lastKey))
	} else {
		RunTransformer(transformer, inputDbPath, outputDbPath, []byte(*keyPrefix), nil)
	}
}
