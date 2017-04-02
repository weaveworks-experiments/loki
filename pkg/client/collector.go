package loki

import (
	"log"
	"net/http"
	"sync"

	"github.com/weaveworks-experiments/loki/pkg/model"
)

// Want to be able to support a service doing 100 QPS with a 15s scrape interval
var globalCollector = NewCollector(15 * 100)

type Collector struct {
	mtx      sync.Mutex
	traceIDs map[uint64]int // map from trace ID to index in traces
	traces   []trace
	next     int
	length   int
}

type trace struct {
	traceID uint64
	spans   []*model.Span
}

func NewCollector(capacity int) *Collector {
	return &Collector{
		traceIDs: make(map[uint64]int, capacity),
		traces:   make([]trace, capacity, capacity),
		next:     0,
		length:   0,
	}
}

func (c *Collector) Collect(span *model.Span) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	traceID := span.TraceId
	idx, ok := c.traceIDs[traceID]
	if !ok {
		// Pick a slot in c.spans for this trace
		idx = c.next
		c.next++
		c.next %= cap(c.traces) // wrap

		// If the slot it occupied, we'll need to clear the trace ID index,
		// otherwise we'll need to number of traces.
		if c.length == cap(c.traces) {
			delete(c.traceIDs, c.traces[idx].traceID)
		} else {
			c.length++
		}

		// Initialise said slot.
		c.traceIDs[traceID] = idx
		c.traces[idx].traceID = traceID
		c.traces[idx].spans = c.traces[idx].spans[:0]
	}

	c.traces[idx].spans = append(c.traces[idx].spans, span)
	return nil
}

func (*Collector) Close() error {
	return nil
}

func (c *Collector) gather() []*model.Span {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	spans := make([]*model.Span, 0, c.length)
	i, count := c.next-c.length, 0
	if i < 0 {
		i = cap(c.traces) + i
	}
	for count < c.length {
		i %= cap(c.traces)
		spans = append(spans, c.traces[i].spans...)
		delete(c.traceIDs, c.traces[i].traceID)
		i++
		count++
	}
	c.length = 0
	if len(c.traceIDs) != 0 {
		panic("didn't clear all trace ids")
	}
	return spans
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	spans := model.Spans{
		Spans: c.gather(),
	}
	buf, err := spans.Marshal()
	if err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(buf); err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Handler() http.Handler {
	return globalCollector
}
