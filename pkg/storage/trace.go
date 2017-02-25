package storage

import (
	"sort"

	log "github.com/Sirupsen/logrus"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type Trace struct {
	ID           int64
	MinTimestamp int64 // in microseconds
	MaxTimestamp int64
	Spans        []*zipkincore.Span
}

func newTrace(span *zipkincore.Span) *Trace {
	return &Trace{
		ID:           span.GetTraceID(),
		MinTimestamp: span.GetTimestamp(),
		MaxTimestamp: span.GetTimestamp() + span.GetDuration(),
		Spans:        []*zipkincore.Span{span},
	}
}

func (t *Trace) addSpan(span *zipkincore.Span) {
	t.Spans = append(t.Spans, span)
	sort.Sort(byTimestamp(t.Spans))

	if t.MinTimestamp > span.GetTimestamp() {
		t.MinTimestamp = span.GetTimestamp()
	}

	spanMax := span.GetTimestamp() + span.GetDuration()
	if t.MaxTimestamp < spanMax {
		t.MaxTimestamp = spanMax
	}
}

func (t *Trace) match(query Query) bool {
	traceStartMS := t.MinTimestamp / 1000
	traceEndMS := t.MaxTimestamp / 1000
	if traceEndMS < query.StartMS || traceStartMS > query.EndMS {
		log.Infof("dropping trace %d - out of time range (%d < %d || %d > %d)", t.ID, traceEndMS, query.StartMS, traceStartMS, query.EndMS)
		return false
	}

	traceDuration := t.MaxTimestamp - t.MinTimestamp
	if traceDuration < query.MinDurationUS {
		log.Infof("dropping span %d - too short %d < %d", t.ID, traceDuration, query.MinDurationUS)
		return false
	}

	if query.ServiceName != "" {
		found := false
	outerServiceName:
		for _, span := range t.Spans {
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
		for _, span := range t.Spans {
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

type byMinTimestamp []Trace

func (ts byMinTimestamp) Len() int           { return len(ts) }
func (ts byMinTimestamp) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts byMinTimestamp) Less(i, j int) bool { return ts[i].MinTimestamp < ts[j].MinTimestamp }

type byTimestamp []*zipkincore.Span

func (ts byTimestamp) Len() int           { return len(ts) }
func (ts byTimestamp) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts byTimestamp) Less(i, j int) bool { return ts[i].GetTimestamp() < ts[j].GetTimestamp() }
