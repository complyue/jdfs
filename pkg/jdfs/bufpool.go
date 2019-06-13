package jdfs

import (
	"os"
	"sort"
	"sync"

	"github.com/complyue/jdfs/pkg/errors"
)

type bufArena struct {
	cap  int
	pool [][]byte
}

// BufPool maintains a pool of bytes buffer,
// with capacity aligned to os page size.
type BufPool struct {
	reg []bufArena

	mu sync.Mutex
}

// Get returns a byte slice with specified length,
// its capacity is aligned to os page size boundaries.
func (bp *BufPool) Get(length int) (buf []byte) {
	if length <= 0 { // be foolproof,
		return nil // let the caller suffer nil dereferencing if it dares
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	capacity := alignCap(length)
	ba := bp.arena(capacity)

	alen := len(ba.pool)
	if alen > 0 {
		buf = ba.pool[alen-1][0:length:capacity]
		ba.pool = ba.pool[:alen-1]
	} else {
		buf = make([]byte, length, capacity)
	}

	return
}

// Return puts the specified byte slice back into the pool,
// its capacity must be aligned to os page size boundaries.
func (bp *BufPool) Return(buf []byte) {
	capacity := cap(buf)
	if capacity <= 0 {
		panic(errors.Errorf("Returning nil/empty buffer to pool ?!"))
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	alignedCap := alignCap(capacity)
	if capacity != alignedCap {
		panic(errors.Errorf("Buffer [:%d:%d] returned to the pool ?! cap should be %d",
			len(buf), capacity, alignedCap))
	}

	ba := bp.arena(capacity)

	ba.pool = append(ba.pool, buf[0:0:capacity])
}

var osPageSize int

func init() { osPageSize = os.Getpagesize() }

func alignCap(capacity int) int {
	rmdr := capacity % osPageSize
	if rmdr > 0 {
		return capacity + osPageSize - rmdr
	}
	return capacity
}

func (bp *BufPool) arena(capacity int) (ba *bufArena) {
	i := sort.Search(len(bp.reg), func(i int) bool {
		return bp.reg[i].cap >= capacity
	})
	if i >= len(bp.reg) {
		bp.reg = append(bp.reg, bufArena{cap: capacity})
		ba = &bp.reg[len(bp.reg)-1]
	} else if bp.reg[i].cap == capacity {
		ba = &bp.reg[i]
	} else {
		bp.reg = append(bp.reg[:i], append([]bufArena{bufArena{cap: capacity}}, bp.reg[i:]...)...)
		ba = &bp.reg[i]
	}
	return
}
