// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/threadpool.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"errors"
	"runtime"
)

var ErrPoolStopped = errors.New("thread pool has been stopped")

type Threadpool struct {
	pool       chan *OSThread
	stopping   chan struct{}
	stopped    chan struct{}
	numThreads int
}

func NewThreadPool(num int) (*Threadpool, error) {
	if num <= 0 {
		return nil, nil
	}

	if num >= runtime.GOMAXPROCS(0) {
		return nil, errors.New("threadpool size must be GOMAXPROCS - 1 at most")
	}

	tp := &Threadpool{
		pool:       make(chan *OSThread, num),
		stopping:   make(chan struct{}),
		stopped:    make(chan struct{}),
		numThreads: num,
	}

	for i := 0; i < num; i++ {
		t := NewOSThread()
		t.Start()
		tp.pool <- t
	}

	return tp, nil
}

func (t *Threadpool) start() {
	defer func() {
		close(t.stopped)
	}()

	// The .stopping channel is never written so this blocks until the channel is
	// closed at which point the threadpool is shutting down, so we want to stop
	// each of the expected threads in it.
	<-t.stopping
	for i := 0; i < t.numThreads; i++ {
		thread := <-t.pool
		thread.Stop()
		thread.Join()
	}
}

func (t *Threadpool) Start() {
	go t.start()
}

func (t *Threadpool) StopAndWait() {
	// Indicate to all thread that they should stop, then wait for them to do so
	// by trying to read from a channel that will be closed when all threads have
	// finally stopped.
	close(t.stopping)
	<-t.stopped
}

func (t *Threadpool) Call(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case <-t.stopping:
		return nil, ErrPoolStopped
	case thread := <-t.pool:
		// TODO(56quarters): Instrument time taken to get a thread from the pool and
		//  time taken for each task to execute. The threadpool should also make the
		//  number of running tasks available as a gauge.
		defer func() { t.pool <- thread }()
		return thread.Call(fn)

	}
}
