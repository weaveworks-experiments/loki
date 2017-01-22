package storage

import (
	"sort"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

const inMemoryTraces = 1024 * 1024

type trace struct {
	minTimestamp int64
	spans        []*zipkincore.Span
}

func (t *trace) match(query Query) bool {
	for _, span := range t.spans {
		spanStartMS := span.GetTimestamp() / 1000
		spanEndMS := (span.GetTimestamp() + span.GetDuration()) / 1000

		// All spans must be within the time range
		if spanEndMS < query.StartMS || spanStartMS > query.EndMS {
			log.Infof("dropping span - %d < %d || %d > %d", spanEndMS, query.StartMS, spanStartMS, query.EndMS)
			return false
		}

		// Only one span needs to be of length MinDuration
		minDuration = minDuration || span.GetDuration() >= query.MinDurationUS
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

func NewSpanStore() *inMemory {
	return &inMemory{
		traces:    map[int64]*trace{},
		services:  map[string]struct{}{},
		spanNames: map[string]map[string]struct{}{},
	}
}

type inMemory struct {
	mtx       sync.RWMutex
	traces    map[int64]*trace
	services  map[string]struct{}
	spanNames map[string]map[string]struct{}
}

func (s *inMemory) Append(span *zipkincore.Span) error {
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

	// update services 'index'
	services := map[string]struct{}{}
	for _, annotation := range span.Annotations {
		s.services[annotation.Host.ServiceName] = struct{}{}
		services[annotation.Host.ServiceName] = struct{}{}
	}
	for _, annotation := range span.BinaryAnnotations {
		s.services[annotation.Host.ServiceName] = struct{}{}
		services[annotation.Host.ServiceName] = struct{}{}
	}

	// update spanNames 'index'
	for service := range services {
		if _, ok := s.spanNames[service]; !ok {
			s.spanNames[service] = map[string]struct{}{}
		}
		s.spanNames[service][span.Name] = struct{}{}
	}

	return nil
}

func (s *inMemory) garbageCollect() {
	if len(s.traces) > inMemoryTraces {
		// for now, just delete random 10%
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

func (s *inMemory) Services() ([]string, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	result := make([]string, 0, len(s.services))
	for service := range s.services {
		result = append(result, service)
	}
	return result, nil
}

func (s *inMemory) SpanNames(serviceName string) ([]string, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	names, ok := s.spanNames[serviceName]
	if !ok {
		return nil, nil
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result, nil
}

func (s *inMemory) Trace(id int64) ([]*zipkincore.Span, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	trace, ok := s.traces[id]
	if !ok {
		return nil, nil
	}
	return trace.spans, nil
}

func (s *inMemory) Traces(query Query) ([][]*zipkincore.Span, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	traces := []*trace{}
	for _, trace := range s.traces {
		if trace.match(query) {
			traces = append(traces, trace)
		}
	}
	sort.Sort(sort.Reverse(byMinTimestamp(traces)))

	result := [][]*zipkincore.Span{}
	for _, trace := range traces {
		result = append(result, trace.spans)
	}
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}
