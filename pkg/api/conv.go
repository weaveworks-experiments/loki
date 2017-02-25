package api

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	_ "unsafe" // For math.Float64frombits

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"

	"github.com/weaveworks-experiments/loki/pkg/storage"
)

func fromIDStr(id string) (int64, error) {
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

func binaryAnnotationToWire(annotation *zipkincore.BinaryAnnotation) interface{} {
	var value interface{}
	switch annotation.AnnotationType {
	case zipkincore.AnnotationType_BOOL:
		value = len(annotation.Value) > 0 && annotation.Value[0] == '\x01'

	case zipkincore.AnnotationType_BYTES:
		value = annotation.Value

	case zipkincore.AnnotationType_I16:
		value = int16(binary.BigEndian.Uint16(annotation.Value))

	case zipkincore.AnnotationType_I32:
		value = int32(binary.BigEndian.Uint32(annotation.Value))

	case zipkincore.AnnotationType_I64:
		value = int64(binary.BigEndian.Uint64(annotation.Value))

	case zipkincore.AnnotationType_DOUBLE:
		b := binary.BigEndian.Uint64(annotation.Value)
		value = math.Float64frombits(b)

	case zipkincore.AnnotationType_STRING:
		value = string(annotation.Value)
	}

	return struct {
		Endpoint interface{} `json:"endpoint"`
		Key      string      `json:"key"`
		Value    interface{} `json:"value"`
	}{
		Endpoint: endpointToWire(annotation.Host),
		Key:      annotation.Key,
		Value:    value,
	}

}

func binaryAnnotationsToWire(annotations []*zipkincore.BinaryAnnotation) []interface{} {
	result := make([]interface{}, 0, len(annotations))
	for _, annotation := range annotations {
		result = append(result, binaryAnnotationToWire(annotation))
	}
	return result
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
		TraceID:           idStr(&span.TraceID),
		Name:              span.Name,
		ID:                idStr(&span.ID),
		ParentID:          idStr(span.ParentID),
		Timestamp:         span.Timestamp,
		Duration:          span.Duration,
		Annotations:       annotationsToWire(span.Annotations),
		BinaryAnnotations: binaryAnnotationsToWire(span.BinaryAnnotations),
	}
}

func SpansToWire(spans []*zipkincore.Span) []interface{} {
	result := make([]interface{}, 0, len(spans))
	for _, span := range spans {
		result = append(result, spanToWire(span))
	}
	return result
}

func TracesToWire(traces []storage.Trace) [][]interface{} {
	result := make([][]interface{}, 0, len(traces))
	for _, trace := range traces {
		result = append(result, SpansToWire(trace.Spans))
	}
	return result
}
