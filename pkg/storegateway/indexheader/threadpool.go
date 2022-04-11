// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/threadpool.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	LabelWaiting  = "waiting"
	LabelComplete = "complete"
)

var ErrPoolStopped = errors.New("thread pool has been stopped")

type Threadpool struct {
	pool       chan *OSThread
	stopping   chan struct{}
	stopped    chan struct{}
	numThreads int

	timing *prometheus.HistogramVec
	tasks  prometheus.Gauge
}

func NewThreadPool(num int, reg prometheus.Registerer) (*Threadpool, error) {
	if num <= 0 {
		return nil, nil
	}

	tp := &Threadpool{
		pool:       make(chan *OSThread, num),
		stopping:   make(chan struct{}),
		stopped:    make(chan struct{}),
		numThreads: num,

		timing: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "cortex_storegateway_index_header_thread_pool_seconds",
			Help: "Amount of time spent performing index header operations on a dedicated thread",
		}, []string{"stage"}),
		tasks: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_index_header_thread_pool_tasks",
			Help: "Number of index header operations currently executing",
		}),
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
	start := time.Now()

	select {
	case <-t.stopping:
		return nil, ErrPoolStopped
	case thread := <-t.pool:
		waiting := time.Since(start)

		defer func() {
			complete := time.Since(start)

			t.pool <- thread
			t.tasks.Dec()
			t.timing.WithLabelValues(LabelWaiting).Observe(waiting.Seconds())
			t.timing.WithLabelValues(LabelComplete).Observe(complete.Seconds())
		}()

		t.tasks.Inc()
		return thread.Call(fn)
	}
}
