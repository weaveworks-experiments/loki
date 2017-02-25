package storage

import (
	log "github.com/Sirupsen/logrus"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type Trace struct {
	ID           int64
	MinTimestamp int64
	Spans        []*zipkincore.Span
}

func (t *Trace) match(query Query) bool {
	minDuration := false
	for _, span := range t.Spans {
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
	if !minDuration {
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
