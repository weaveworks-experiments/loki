package loki

import (
	"log"
	"net/http"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

var globalCollector = NewCollector(1024)

type Collector struct {
	mtx   sync.Mutex
	spans []*zipkincore.Span
	i     int
	l     int
}

func NewCollector(capacity int) *Collector {
	return &Collector{
		spans: make([]*zipkincore.Span, capacity, capacity),
		i:     0,
		l:     0,
	}
}

func (c *Collector) Collect(span *zipkincore.Span) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.spans[c.i] = span

	c.i++
	c.i %= cap(c.spans) // wrap
	if c.l < cap(c.spans) {
		c.l++
	}

	return nil
}

func (*Collector) Close() error {
	return nil
}

func (c *Collector) gather() []*zipkincore.Span {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	spans := make([]*zipkincore.Span, 0, c.l)
	for i, iters := c.i, 0; iters < c.l; iters++ {
		i++
		i %= cap(c.spans)
		spans = append(spans, c.spans[i])
		c.spans[i] = nil
	}
	c.i = 0
	c.l = 0
	return spans
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	spans := c.gather()
	transport := thrift.NewStreamTransportW(w)
	protocol := thrift.NewTCompactProtocol(transport)

	if err := protocol.WriteListBegin(thrift.STRUCT, len(spans)); err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, span := range spans {
		if err := span.Write(protocol); err != nil {
			log.Printf("error writing spans: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err := protocol.WriteListEnd(); err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := protocol.Flush(); err != nil {
		log.Printf("error flushing: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Handler() http.Handler {
	return globalCollector
}
