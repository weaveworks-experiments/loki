package storage

import (
	"sort"
	"time"

	log "github.com/Sirupsen/logrus"
	prom_model "github.com/prometheus/common/model"
	"github.com/weaveworks-experiments/loki/pkg/model"
)

type Trace struct {
	ID           uint64
	MinTimestamp time.Time
	MaxTimestamp time.Time
	Spans        []*model.Span
}

func newTrace(span *model.Span) *Trace {
	return &Trace{
		ID:           span.TraceId,
		MinTimestamp: span.Start,
		MaxTimestamp: span.End,
		Spans:        []*model.Span{span},
	}
}

func (t *Trace) addSpan(span *model.Span) {
	t.Spans = append(t.Spans, span)
	sort.Sort(byTimestamp(t.Spans))

	if t.MinTimestamp.After(span.Start) {
		t.MinTimestamp = span.Start
	}

	if t.MaxTimestamp.Before(span.End) {
		t.MaxTimestamp = span.End
	}
}

func (t *Trace) match(query Query) bool {
	if t.MaxTimestamp.Before(query.Start) || t.MinTimestamp.After(query.End) {
		log.Infof("dropping trace %d - out of time range (%v < %v || %v > %v)", t.ID, t.MaxTimestamp, query.Start, t.MinTimestamp, query.End)
		return false
	}

	traceDuration := t.MaxTimestamp.Sub(t.MinTimestamp)
	if traceDuration < query.MinDuration {
		log.Infof("dropping span %d - too short %d < %d", t.ID, traceDuration, query.MinDuration)
		return false
	}

	if query.ServiceName != "" {
		found := false
	outerServiceName:
		for _, span := range t.Spans {
			for _, tag := range span.Tags {
				if tag.Key == prom_model.JobLabel && tag.String_ == query.ServiceName {
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
			if span.OperationName == query.SpanName {
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
func (ts byMinTimestamp) Less(i, j int) bool { return ts[i].MinTimestamp.Before(ts[j].MinTimestamp) }

type byTimestamp []*model.Span

func (ts byTimestamp) Len() int           { return len(ts) }
func (ts byTimestamp) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts byTimestamp) Less(i, j int) bool { return ts[i].Start.Before(ts[j].Start) }
