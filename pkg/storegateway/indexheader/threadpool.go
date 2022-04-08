// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/threadpool.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"context"
	"errors"
	"runtime"
)

var ErrPoolStopped = errors.New("thread pool has been stopped")

type Threadpool struct {
	ctx        context.Context
	cancel     context.CancelFunc
	pool       chan *OSThread
	numThreads int
}

func NewThreadPool(num int) (*Threadpool, error) {
	if num <= 0 {
		return nil, nil
	}

	if num >= runtime.GOMAXPROCS(0) {
		return nil, errors.New("threadpool size must be GOMAXPROCS - 1 at most")
	}

	ctx, cancel := context.WithCancel(context.Background())
	tp := &Threadpool{
		ctx:        ctx,
		cancel:     cancel,
		pool:       make(chan *OSThread, num),
		numThreads: num,
	}

	for i := 0; i < num; i++ {
		t := NewOSThread(ctx)
		t.Start()
		tp.pool <- t
	}

	return tp, nil
}

func (t *Threadpool) start() {
	for range t.ctx.Done() {
		for i := 0; i < t.numThreads; i++ {
			thread := <-t.pool
			thread.Join()
		}
	}
}

func (t *Threadpool) Start() {
	go t.start()
}

func (t *Threadpool) Stop() {
	t.cancel()
}

func (t *Threadpool) Call(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case <-t.ctx.Done():
		return nil, ErrPoolStopped
	case thread := <-t.pool:
		// TODO(56quarters): Instrument time taken to get a thread from the pool and
		//  time taken for each task to execute. The threadpool should also make the
		//  number of running tasks available as a gauge.
		defer func() { t.pool <- thread }()
		return thread.Call(fn)

	}
}
