package storage

import (
	"sort"
	"sync"

	log "github.com/Sirupsen/logrus"
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

type Query struct {
	ServiceName string
	SpanName    string
	MinDuration int64
	MaxDuration int64
	EndMS       int64
	StartMS     int64
	Limit       int
}

func (t *trace) match(query Query) bool {
	for _, span := range t.spans {
		spanStartMS := span.GetTimestamp() / 1000
		spanEndMS := (span.GetTimestamp() + span.GetDuration()) / 1000
		if spanEndMS < query.StartMS || spanStartMS > query.EndMS {
			log.Infof("dropping span - %d < %d || %d > %d", spanEndMS, query.StartMS, spanStartMS, query.EndMS)
			return false
		}
	}

	if query.ServiceName != "" {
		found := false
	outerServiceName:
		for _, span := range t.spans {
			for _, annotation := range span.Annotations {
				if annotation.IsSetHost() && annotation.GetHost().GetServiceName() == query.ServiceName {
					found = true
					break outerServiceName
				}
			}
			for _, annotation := range span.BinaryAnnotations {
				if annotation.IsSetHost() && annotation.GetHost().GetServiceName() == query.ServiceName {
					found = true
					break outerServiceName
				}
			}
		}
		if !found {
			return false
		}
	}

	if query.SpanName != "" && query.SpanName != "all" {
		found := false
	outerSpanName:
		for _, span := range t.spans {
			if span.GetName() == query.SpanName {
				found = true
				break outerSpanName
			}
		}
		if !found {
			return false
		}
	}

	return true
}

type byMinTimestamp []*trace

func (ts byMinTimestamp) Len() int           { return len(ts) }
func (ts byMinTimestamp) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts byMinTimestamp) Less(i, j int) bool { return ts[i].minTimestamp < ts[j].minTimestamp }

type byTimestamp []*zipkincore.Span

func (ts byTimestamp) Len() int           { return len(ts) }
func (ts byTimestamp) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts byTimestamp) Less(i, j int) bool { return ts[i].GetTimestamp() < ts[j].GetTimestamp() }

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

	t, ok := s.traces[traceID]
	if !ok {
		t = &trace{}
	}

	t.spans = append(t.spans, span)
	sort.Sort(byTimestamp(t.spans))

	if t.minTimestamp > span.GetTimestamp() {
		t.minTimestamp = span.GetTimestamp()
	}

	s.traces[traceID] = t
	return nil
}

func (s *SpanStore) garbageCollect() {
	if len(s.traces) > inMemoryTraces {
		// for now, just delete 10%
		toDelete := int(inMemoryTraces * 0.1)
		for k := range s.traces {
			toDelete--
			if toDelete < 0 {
				return
			}
			delete(s.traces, k)
		}
	}
}

func (s *SpanStore) Services() []string {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	services := map[string]struct{}{}
	for _, trace := range s.traces {
		for _, span := range trace.spans {
			for _, annotation := range span.Annotations {
				services[annotation.Host.ServiceName] = struct{}{}
			}
			for _, annotation := range span.BinaryAnnotations {
				services[annotation.Host.ServiceName] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(services))
	for service := range services {
		result = append(result, service)
	}
	return result
}

func (s *SpanStore) SpanNames(serviceName string) []string {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	names := map[string]struct{}{}
	for _, trace := range s.traces {
		for _, span := range trace.spans {
			names[span.Name] = struct{}{}
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result
}

func (s *SpanStore) Trace(id int64) []*zipkincore.Span {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	trace, ok := s.traces[id]
	if !ok {
		return nil
	}
	return trace.spans
}

func (s *SpanStore) Traces(query Query) [][]*zipkincore.Span {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	traces := []*trace{}
	for _, trace := range s.traces {
		if trace.match(query) {
			traces = append(traces, trace)
		}
	}
	sort.Sort(sort.Reverse(byMinTimestamp(traces)))
	if query.Limit > 0 && len(traces) > query.Limit {
		traces = traces[:query.Limit]
	}

	result := [][]*zipkincore.Span{}
	for _, trace := range traces {
		result = append(result, trace.spans)
	}
	return result
}
