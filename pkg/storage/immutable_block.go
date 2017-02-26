package storage

import (
	"sort"
)

type immutableBlock struct {
	//from, through int64 // in ms

	traceIDs  map[int64]int
	traces    []Trace // sorted by minTimestamp
	services  []string
	spanNames map[string][]string
}

func newImmutableBlock(b *mutableBlock) *immutableBlock {
	//from, through := int64(math.MaxInt64), int64(0)

	traces := make([]Trace, 0, len(b.traces))
	for _, trace := range b.traces {
		//	from = min(from, trace.MinTimestamp)
		//	through = max(through, trace.MinTimestamp)
		traces = append(traces, *trace)
	}

	sort.Sort(byMinTimestamp(traces))
	traceIDs := make(map[int64]int, len(b.traces))
	for i, trace := range traces {
		traceIDs[trace.ID] = i
	}

	services := make([]string, 0, len(b.services))
	for service := range b.services {
		services = append(services, service)
	}

	spanNames := make(map[string][]string, len(b.spanNames))
	for service := range b.spanNames {
		names := make([]string, 0, len(b.spanNames[service]))
		for name := range b.spanNames[service] {
			names = append(names, name)
		}
		spanNames[service] = names
	}

	return &immutableBlock{
		traceIDs:  traceIDs,
		traces:    traces,
		services:  services,
		spanNames: spanNames,
	}
}

func (s *immutableBlock) Services() ([]string, error) {
	return s.services, nil
}

func (s *immutableBlock) SpanNames(serviceName string) ([]string, error) {
	return s.spanNames[serviceName], nil
}

func (s *immutableBlock) Trace(id int64) (Trace, error) {
	i, ok := s.traceIDs[id]
	if !ok {
		return Trace{}, nil
	}
	return s.traces[i], nil
}

func (s *immutableBlock) Traces(query Query) ([]Trace, error) {
	// the smallest index i in [0, n) at which f(i) is true
	first := sort.Search(len(s.traces), func(i int) bool {
		return s.traces[i].MinTimestamp >= (query.StartMS * 1000)
	})
	if first == len(s.traces) {
		return nil, nil
	}
	last := sort.Search(len(s.traces), func(i int) bool {
		return s.traces[i].MinTimestamp > (query.EndMS * 1000)
	})
	return s.traces[first:last], nil
}
