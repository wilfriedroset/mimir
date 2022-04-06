// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/thread.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"context"
	"runtime"

	"go.uber.org/atomic"
)

type execResult struct {
	value interface{}
	err   error
}

type OSThread struct {
	call       chan func() (interface{}, error)
	res        chan execResult
	ctx        context.Context
	cancel     context.CancelFunc
	localTasks *atomic.Uint64
}

func NewOSThread(ctx context.Context, cancel context.CancelFunc) *OSThread {
	if cancel == nil {
		ctx, cancel = context.WithCancel(ctx)
	}

	return &OSThread{
		call:       make(chan func() (interface{}, error)),
		res:        make(chan execResult),
		ctx:        ctx,
		cancel:     cancel,
		localTasks: atomic.NewUint64(0),
	}
}

func (o *OSThread) start() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for {
		select {
		case <-o.ctx.Done():
			return
		case fn := <-o.call:
			val, err := fn()
			o.localTasks.Dec()
			o.res <- execResult{value: val, err: err}
			break
		}
	}
}

func (o *OSThread) Start() {
	go o.start()
}

func (o *OSThread) Call(fn func() (interface{}, error)) (interface{}, error) {
	o.localTasks.Inc()
	o.call <- fn

	res := <-o.res
	return res.value, res.err
}

func (o *OSThread) Join() {
	for range o.ctx.Done() {
		tasks := o.localTasks.Load()
		if tasks == 0 {
			break
		}
	}

	o.cancel()
}

func (o *OSThread) Stop() {
	o.cancel()
}
