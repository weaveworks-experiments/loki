package storage

import (
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type SpanStore interface {
	Append(*zipkincore.Span) error
	Services() []string
	SpanNames(serviceName string) []string
	Trace(id int64) []*zipkincore.Span
	Traces(query Query) [][]*zipkincore.Span
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
