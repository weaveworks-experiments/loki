package api

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"
	_ "unsafe" // For math.Float64frombits

	otext "github.com/opentracing/opentracing-go/ext"
	prom_model "github.com/prometheus/common/model"
	"github.com/weaveworks-experiments/loki/pkg/model"
	"github.com/weaveworks-experiments/loki/pkg/storage"
)

const (
	CLIENT_SEND     = "cs"
	CLIENT_RECV     = "cr"
	SERVER_SEND     = "ss"
	SERVER_RECV     = "sr"
	LOCAL_COMPONENT = "lc"
)

type annotation struct {
	Endpoint  endpoint    `json:"endpoint"`
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

type binaryAnnotation struct {
	Endpoint endpoint    `json:"endpoint"`
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
}

type endpoint struct {
	ServiceName string `json:"serviceName"`
	Ipv4        string `json:"ipv4"`
	Port        int16  `json:"port"`
}

func fromIDStr(id string) (uint64, error) {
	bytes, err := hex.DecodeString(id)
	if err != nil {
		return 0, err
	}
	if len(bytes) != 8 {
		return 0, fmt.Errorf("Invalid id")
	}
	return binary.BigEndian.Uint64(bytes), nil
}

func idStr(id *uint64) string {
	if id == nil {
		return ""
	}

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], *id)
	return hex.EncodeToString(idBytes[:])
}

func wireEndpoint(job, addr string) endpoint {
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return endpoint{
		ServiceName: job,
		Ipv4:        host,
		Port:        int16(port),
	}
}

func calculateAnnotations(span *model.Span) ([]annotation, []binaryAnnotation) {
	var annotations []annotation
	var binaryAnnotations []binaryAnnotation

	endpoint := wireEndpoint(
		span.Tag(prom_model.JobLabel).String_,
		span.Tag(prom_model.AddressLabel).String_,
	)

	annotate := func(timestamp time.Time, value string) {
		annotations = append(annotations, annotation{
			Timestamp: timestamp.UnixNano() / 1e3,
			Value:     value,
			Endpoint:  endpoint,
		})
	}

	annotateBinary := func(key string, value interface{}) {
		binaryAnnotations = append(binaryAnnotations, binaryAnnotation{
			Key:      key,
			Value:    value,
			Endpoint: endpoint,
		})
	}

	switch span.Tag(string(otext.SpanKind)).String_ {
	case string(otext.SpanKindRPCClientEnum):
		annotate(span.Start, CLIENT_SEND)
		annotate(span.End, CLIENT_RECV)
	case string(otext.SpanKindRPCServerEnum):
		annotate(span.Start, SERVER_RECV)
		annotate(span.End, SERVER_SEND)
		//		case SpanKindResource:
		//			serviceName, ok := sp.Tags[string(otext.PeerService)]
		//			if !ok {
		//				serviceName = r.endpoint.GetServiceName()
		//			}
		//			host, ok := sp.Tags[string(otext.PeerHostname)].(string)
		//			if !ok {
		//				if r.endpoint.GetIpv4() > 0 {
		//					ip := make([]byte, 4)
		//					binary.BigEndian.PutUint32(ip, uint32(r.endpoint.GetIpv4()))
		//					host = net.IP(ip).To4().String()
		//				} else {
		//					ip := r.endpoint.GetIpv6()
		//					host = net.IP(ip).String()
		//				}
		//			}
		//			var sPort string
		//			port, ok := sp.Tags[string(otext.PeerPort)]
		//			if !ok {
		//				sPort = strconv.FormatInt(int64(r.endpoint.GetPort()), 10)
		//			} else {
		//				sPort = strconv.FormatInt(int64(port.(uint16)), 10)
		//			}
		//			re := makeEndpoint(net.JoinHostPort(host, sPort), serviceName.(string))
		//			if re != nil {
		//				annotateBinary(span, zipkincore.SERVER_ADDR, serviceName, re)
		//			} else {
		//				fmt.Printf("endpoint creation failed: host: %q port: %q", host, sPort)
		//			}
		//			annotate(span, sp.Start, zipkincore.CLIENT_SEND, r.endpoint)
		//			annotate(span, sp.Start.Add(sp.Duration), zipkincore.CLIENT_RECV, r.endpoint)
	default:
		annotateBinary(LOCAL_COMPONENT, span.Tag(prom_model.JobLabel).String_)
	}

	for _, tag := range span.Tags {
		annotateBinary(tag.Key, tag.Value())
	}

	return annotations, binaryAnnotations
}

func spanToWire(span *model.Span) interface{} {
	annotations, binaryAnnotations := calculateAnnotations(span)
	return struct {
		TraceID           string             `json:"traceId"`
		Name              string             `json:"name"`
		ID                string             `json:"id"`
		ParentID          string             `json:"parentId,omitempty"`
		Timestamp         int64              `json:"timestamp,omitempty"`
		Duration          int64              `json:"duration,omitempty"`
		Annotations       []annotation       `json:"annotations"`
		BinaryAnnotations []binaryAnnotation `json:"binaryAnnotations"`
	}{
		TraceID:           idStr(&span.TraceId),
		Name:              span.OperationName,
		ID:                idStr(&span.SpanId),
		ParentID:          idStr(&span.ParentSpanId),
		Timestamp:         span.Start.UnixNano() / int64(time.Millisecond),
		Duration:          int64(span.Start.Sub(span.End) / time.Millisecond),
		Annotations:       annotations,
		BinaryAnnotations: binaryAnnotations,
	}
}

func SpansToWire(spans []*model.Span) []interface{} {
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
