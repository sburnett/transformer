package transformer

import (
	"bytes"
	"fmt"
)

type Priority struct {
	key           []byte
	databaseIndex uint8
}

// An Item is something we manage in a priority queue.
type Item struct {
	record   *LevelDbRecord
	reader   StoreReader
	priority Priority // The priority of the item in the queue.
	// The index is needed by changePriority and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	cmp := bytes.Compare(pq[i].priority.key, pq[j].priority.key)
	switch cmp {
	case -1:
		return true
	case 0:
		return pq[i].priority.databaseIndex < pq[j].priority.databaseIndex
	case 1:
		return false
	default:
		panic(fmt.Errorf("bytes.Compare returned an unexpected value: %v", cmp))
	}
	panic("This should never happen")
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	// To simplify indexing expressions in these methods, we save a copy of the
	// slice object. We could instead write (*pq)[i].
	a := *pq
	n := len(a)
	a = a[0 : n+1]
	item := x.(*Item)
	item.index = n
	a[n] = item
	*pq = a
}

func (pq *PriorityQueue) Pop() interface{} {
	a := *pq
	n := len(a)
	item := a[n-1]
	item.index = -1 // for safety
	*pq = a[0 : n-1]
	return item
}
