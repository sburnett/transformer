package transformers

import (
	"fmt"

	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func ExampleJoin_inner() {
	left := store.SliceStore{}
	left.BeginWriting()
	left.WriteRecord(store.NewRecord("1", "news", 0))
	left.WriteRecord(store.NewRecord("2", "book", 0))
	left.EndWriting()

	right := store.SliceStore{}
	right.BeginWriting()
	right.WriteRecord(store.NewRecord("1", "paper", 0))
	right.WriteRecord(store.NewRecord("3", "brush", 0))
	right.EndWriting()

	output := store.SliceStore{}

	joiner := Join(nil, nil)
	transformer.RunTransformer(transformer.MakeGroupDoFunc(joiner), store.NewDemuxingReader(&left, &right), &output)

	output.BeginReading()
	for {
		record, err := output.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	output.EndReading()

	// Output:
	//
	// 1: newspaper
}

func ExampleJoin_left() {
	left := store.SliceStore{}
	left.BeginWriting()
	left.WriteRecord(store.NewRecord("1", "news", 0))
	left.WriteRecord(store.NewRecord("2", "book", 0))
	left.EndWriting()

	right := store.SliceStore{}
	right.BeginWriting()
	right.WriteRecord(store.NewRecord("1", "paper", 0))
	right.WriteRecord(store.NewRecord("3", "brush", 0))
	right.EndWriting()

	output := store.SliceStore{}

	joiner := Join(nil, []byte("shelf"))
	transformer.RunTransformer(transformer.MakeGroupDoFunc(joiner), store.NewDemuxingReader(&left, &right), &output)

	output.BeginReading()
	for {
		record, err := output.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	output.EndReading()

	// Output:
	//
	// 1: newspaper
	// 2: bookshelf
}

func ExampleJoin_right() {
	left := store.SliceStore{}
	left.BeginWriting()
	left.WriteRecord(store.NewRecord("1", "news", 0))
	left.WriteRecord(store.NewRecord("2", "book", 0))
	left.EndWriting()

	right := store.SliceStore{}
	right.BeginWriting()
	right.WriteRecord(store.NewRecord("1", "paper", 0))
	right.WriteRecord(store.NewRecord("3", "brush", 0))
	right.EndWriting()

	output := store.SliceStore{}

	joiner := Join([]byte("tooth"), nil)
	transformer.RunTransformer(transformer.MakeGroupDoFunc(joiner), store.NewDemuxingReader(&left, &right), &output)

	output.BeginReading()
	for {
		record, err := output.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	output.EndReading()

	// Output:
	//
	// 1: newspaper
	// 3: toothbrush
}

func ExampleJoin_outer() {
	left := store.SliceStore{}
	left.BeginWriting()
	left.WriteRecord(store.NewRecord("1", "news", 0))
	left.WriteRecord(store.NewRecord("2", "book", 0))
	left.EndWriting()

	right := store.SliceStore{}
	right.BeginWriting()
	right.WriteRecord(store.NewRecord("1", "paper", 0))
	right.WriteRecord(store.NewRecord("3", "brush", 0))
	right.EndWriting()

	output := store.SliceStore{}

	joiner := Join([]byte("tooth"), []byte("shelf"))
	transformer.RunTransformer(transformer.MakeGroupDoFunc(joiner), store.NewDemuxingReader(&left, &right), &output)

	output.BeginReading()
	for {
		record, err := output.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	output.EndReading()

	// Output:
	//
	// 1: newspaper
	// 2: bookshelf
	// 3: toothbrush
}

func ExampleJoin_multiple() {
	left := store.SliceStore{}
	left.BeginWriting()
	left.WriteRecord(store.NewRecord("1", "what", 0))
	left.WriteRecord(store.NewRecord("2", "news", 0))
	left.EndWriting()

	middle := store.SliceStore{}
	middle.BeginWriting()
	middle.WriteRecord(store.NewRecord("1", "so", 0))
	middle.WriteRecord(store.NewRecord("3", "the", 0))
	middle.EndWriting()

	right := store.SliceStore{}
	right.BeginWriting()
	right.WriteRecord(store.NewRecord("1", "ever", 0))
	right.WriteRecord(store.NewRecord("3", "less", 0))
	right.EndWriting()

	output := store.SliceStore{}

	joiner := Join([]byte("never"), []byte("paper"), []byte("man"))
	transformer.RunTransformer(transformer.MakeGroupDoFunc(joiner), store.NewDemuxingReader(&left, &middle, &right), &output)

	output.BeginReading()
	for {
		record, err := output.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	output.EndReading()

	// Output:
	//
	// 1: whatsoever
	// 2: newspaperman
	// 3: nevertheless
}
