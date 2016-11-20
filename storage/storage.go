package storage

import (
	"mtx"

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

const inMemoryTraces = 100 * 1024

type SpanStore struct {
	mtx    sync.RWMutex
	traces map[int64]*trace
}

type trace struct {
	minTimestamp int64
	spans        []*zipkincore.Span
}

func NewSpanStore() *SpanStore {
	return &SpanStore{
		traces: map[int64]*trace{},
	}
}

func (s *SpanStore) Append(span *zipkincore.Span) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.garbageCollect()

	traceID := span.GetTraceID()

	trace, ok := s.traces[traceID]
	if !ok {
		trace := &trace{}
	}

	trace.spans = append(trace.spans, span)
	if trace.minTimestamp > span.GetTimestamp() {
		trace.minTimestamp = span.GetTimestamp()
	}

	s.traces[traceID] = trace
	return nil
}

func (s *SpanStore) garbageCollect() {
	if len(s.traces) > inMemoryTraces {
		// for now, just delete 10%
		toDelete := int(inMemoryTraces * 0.1)
		for k, _ := range s.traces {
			toDelete--
			delete(s.traces, k)
		}
	}
}
