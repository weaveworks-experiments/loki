package storage

import (
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type SpanStore interface {
	Append(*zipkincore.Span) error
	ReadStore
}

type ReadStore interface {
	Services() ([]string, error)
	SpanNames(serviceName string) ([]string, error)
	Trace(id int64) (Trace, error)
	Traces(query Query) ([]Trace, error)
}

type Query struct {
	ServiceName   string
	SpanName      string
	MinDurationUS int64
	MaxDurationUS int64
	EndMS         int64
	StartMS       int64
	Limit         int
}
