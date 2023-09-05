package autopool

import (
	"runtime"
	"sync"
)

type AutoPool[T any] struct {
	pool  sync.Pool
	New   func() *T
	Reset func(*T)
}

func (p *AutoPool[T]) Get() (v *T) {
	if v0 := p.pool.Get(); v0 != nil {
		v = v0.(*T)
	} else {
		if p.New != nil {
			v = p.New()
		} else {
			v = new(T)
		}
	}
	runtime.SetFinalizer(
		v,
		func(x any) {
			p.Put(v)
		},
	)

	if p.Reset != nil {
		p.Reset(v)
	}

	return v
}

func (p *AutoPool[T]) Put(v *T) {
	runtime.SetFinalizer(v, nil)
	p.pool.Put(v)
}
