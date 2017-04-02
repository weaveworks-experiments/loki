package storage

import (
	"time"

	"github.com/weaveworks-experiments/loki/pkg/model"
)

type SpanStore interface {
	Append(*model.Span) error
	ReadStore
}

type ReadStore interface {
	Services() ([]string, error)
	SpanNames(serviceName string) ([]string, error)
	Trace(id uint64) (Trace, error)
	Traces(query Query) ([]Trace, error)
}

type Query struct {
	ServiceName string
	SpanName    string
	MinDuration time.Duration
	MaxDuration time.Duration
	End         time.Time
	Start       time.Time
	Limit       int
}
