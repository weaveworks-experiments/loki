package storage

import (
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

type SpanStore struct {
}

func NewSpanStore() *SpanStore {
	return &SpanStore{}
}

func (s *SpanStore) Append(*zipkincore.Span) error {
	return nil
}
