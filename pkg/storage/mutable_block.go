package storage

import (
	"sync"

	prom_model "github.com/prometheus/common/model"
	"github.com/weaveworks-experiments/loki/pkg/model"
)

type mutableBlock struct {
	mtx       sync.RWMutex
	traces    map[uint64]*Trace
	services  map[string]struct{}
	spanNames map[string]map[string]struct{}
}

func newMutableBlock() *mutableBlock {
	return &mutableBlock{
		traces:    make(map[uint64]*Trace, numMutableTraces),
		services:  map[string]struct{}{},
		spanNames: map[string]map[string]struct{}{},
	}
}

func (s *mutableBlock) Size() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.traces)
}

func (s *mutableBlock) HasTrace(id uint64) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	_, ok := s.traces[id]
	return ok
}

func (s *mutableBlock) Append(span *model.Span) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	traceID := span.TraceId
	t, ok := s.traces[traceID]
	if ok {
		t.addSpan(span)
	} else {
		t = newTrace(span)
		s.traces[traceID] = t
	}

	// update services 'index'
	services := map[string]struct{}{}
	for _, tag := range span.Tags {
		if tag.Key != prom_model.JobLabel {
			continue
		}
		s.services[tag.String_] = struct{}{}
		services[tag.String_] = struct{}{}
	}

	// update spanNames 'index'
	for service := range services {
		if _, ok := s.spanNames[service]; !ok {
			s.spanNames[service] = map[string]struct{}{}
		}
		s.spanNames[service][span.OperationName] = struct{}{}
	}

	return nil
}

func (s *mutableBlock) Services() ([]string, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	result := make([]string, 0, len(s.services))
	for service := range s.services {
		result = append(result, service)
	}
	return result, nil
}

func (s *mutableBlock) SpanNames(serviceName string) ([]string, error) {
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

func (s *mutableBlock) Trace(id uint64) (Trace, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	trace, ok := s.traces[id]
	if !ok {
		return Trace{}, nil
	}
	return *trace, nil
}

func (s *mutableBlock) Traces(query Query) ([]Trace, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	traces := []Trace{}
	for _, trace := range s.traces {
		if trace.MaxTimestamp.After(query.Start) && trace.MinTimestamp.Before(query.End) {
			traces = append(traces, *trace)
		}
	}
	return traces, nil
}
