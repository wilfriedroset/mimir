// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
)

type threadedReader struct {
	pool   *Threadpool
	reader indexheader.Reader
}

func NewThreadedReader(pool *Threadpool, reader indexheader.Reader) indexheader.Reader {
	return &threadedReader{
		pool:   pool,
		reader: reader,
	}
}

func (t *threadedReader) Close() error {
	_, err := t.pool.Call(func() (interface{}, error) {
		return nil, t.reader.Close()
	})

	return err
}

func (t *threadedReader) IndexVersion() (int, error) {
	val, err := t.pool.Call(func() (interface{}, error) {
		return t.reader.IndexVersion()
	})

	return val.(int), err
}

func (t *threadedReader) PostingsOffset(name string, value string) (index.Range, error) {
	val, err := t.pool.Call(func() (interface{}, error) {
		return t.reader.PostingsOffset(name, value)
	})

	return val.(index.Range), err
}

func (t *threadedReader) LookupSymbol(o uint32) (string, error) {
	val, err := t.pool.Call(func() (interface{}, error) {
		return t.reader.LookupSymbol(o)
	})

	return val.(string), err
}

func (t *threadedReader) LabelValues(name string) ([]string, error) {
	val, err := t.pool.Call(func() (interface{}, error) {
		return t.reader.LabelValues(name)
	})

	return val.([]string), err
}

func (t *threadedReader) LabelNames() ([]string, error) {
	val, err := t.pool.Call(func() (interface{}, error) {
		return t.reader.LabelNames()
	})

	return val.([]string), err
}
