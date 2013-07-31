package transformer

import (
	"math"
)

type Nonce chan int64

// Create a Nonce, which returns a unique integer each time you call Get(). It
// is useful to append a nonce value to otherwise identical keys to resolve
// collisions in LevelDB stores.
func NewNonce() Nonce {
	nonce := make(chan int64)
	go func() {
		for i := int64(0); i < math.MaxInt64; i++ {
			nonce <- i
		}
	}()
	return nonce
}

func (nonce Nonce) Get() int64 {
	return <-nonce
}
