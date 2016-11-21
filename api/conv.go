package api

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

func FromIdStr(id string) (int64, error) {
	bytes, err := hex.DecodeString(id)
	if err != nil {
		return 0, err
	}
	if len(bytes) != 8 {
		return 0, fmt.Errorf("Invalid id")
	}
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

func idStr(id *int64) string {
	if id == nil {
		return ""
	}

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], uint64(*id))
	return hex.EncodeToString(idBytes[:])
}

func endpointToWire(endpoint *zipkincore.Endpoint) interface{} {
	var ipaddr [4]byte
	binary.BigEndian.PutUint32(ipaddr[:], uint32(endpoint.Ipv4))

	return struct {
		ServiceName string `json:"serviceName"`
		Ipv4        string `json:"ipv4"`
		Port        int16  `json:"port"`
	}{
		ServiceName: endpoint.ServiceName,
		Ipv4:        net.IP(ipaddr[:]).String(),
		Port:        endpoint.Port,
	}
}

func annotationToWire(annotation *zipkincore.Annotation) interface{} {
	return struct {
		Endpoint  interface{} `json:"endpoint"`
		Timestamp int64       `json:"timestamp"`
		Value     interface{} `json:"value"`
	}{
		Endpoint:  endpointToWire(annotation.Host),
		Timestamp: annotation.Timestamp,
		Value:     annotation.Value,
	}
}

func annotationsToWire(annotations []*zipkincore.Annotation) []interface{} {
	result := make([]interface{}, 0, len(annotations))
	for _, annotation := range annotations {
		result = append(result, annotationToWire(annotation))
	}
	return result
}

func spanToWire(span *zipkincore.Span) interface{} {
	return struct {
		TraceID           string        `json:"traceId"`
		Name              string        `json:"name"`
		ID                string        `json:"id"`
		ParentID          string        `json:"parentId,omitempty"`
		Timestamp         *int64        `json:"timestamp,omitempty"`
		Duration          *int64        `json:"duration,omitempty"`
		Annotations       []interface{} `json:"annotations"`
		BinaryAnnotations []interface{} `json:"binaryAnnotations"`
	}{
		TraceID:     idStr(&span.TraceID),
		Name:        span.Name,
		ID:          idStr(&span.ID),
		ParentID:    idStr(span.ParentID),
		Timestamp:   span.Timestamp,
		Duration:    span.Duration,
		Annotations: annotationsToWire(span.Annotations),
	}
}

func SpansToWire(spans []*zipkincore.Span) []interface{} {
	result := make([]interface{}, 0, len(spans))
	for _, span := range spans {
		result = append(result, spanToWire(span))
	}
	return result
}

func TracesToWire(traces [][]*zipkincore.Span) [][]interface{} {
	result := make([][]interface{}, 0, len(traces))
	for _, trace := range traces {
		result = append(result, SpansToWire(trace))
	}
	return result
}
