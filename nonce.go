package transformer

import (
	"math"
)

type Nonce chan int64

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
