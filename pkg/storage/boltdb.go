package storage

import (
	"bytes"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/boltdb/bolt"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/willf/bloom"
)

const (
	expectedNumServices  = 1000
	expectedNumSpanNames = 100
)

var (
	tracesBucket       = []byte("traces")
	servicesBucket     = []byte("services")
	spanNamesBucket    = []byte("span_names")
	serviceIndexBucket = []byte("service_index")
)

type boltDBStorage struct {
	db *bolt.DB

	servicesFilter, spanNameFilter *bloom.BloomFilter
}

func newBoltDBStorage() (*boltDBStorage, error) {
	db, err := bolt.Open("traces.db", 0666, &bolt.Options{})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(tracesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(servicesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(spanNamesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(serviceIndexBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	return &boltDBStorage{
		db:             db,
		servicesFilter: bloom.New(expectedNumServices, 5),
		spanNameFilter: bloom.New(expectedNumServices*expectedNumSpanNames, 5),
	}, nil
}

func encodeSpan(span *zipkincore.Span) ([]byte, error) {
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTCompactProtocol(transport)
	if err := span.Write(protocol); err != nil {
		return nil, err
	}
	if err := protocol.Flush(); err != nil {
		return nil, err
	}
	return transport.Buffer.Bytes(), nil
}

func decodeSpan(buf []byte) (*zipkincore.Span, error) {
	transport := thrift.NewTMemoryBuffer()
	transport.Buffer = bytes.NewBuffer(buf)
	protocol := thrift.NewTCompactProtocol(transport)
	span := zipkincore.NewSpan()
	if err := span.Read(protocol); err != nil {
		return nil, err
	}
	return span, nil
}

func services(span *zipkincore.Span) []string {
	services := map[string]struct{}{}
	for _, annotation := range span.Annotations {
		services[annotation.Host.ServiceName] = struct{}{}
	}
	for _, annotation := range span.BinaryAnnotations {
		services[annotation.Host.ServiceName] = struct{}{}
	}
	result := make([]string, 0, len(services))
	for service := range services {
		result = append(result, service)
	}
	return result
}

func (s *boltDBStorage) Append(span *zipkincore.Span) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var (
			traceID       = span.GetTraceID()
			spanTimestamp = span.GetTimestamp()
			spanID        = span.GetID()
			services      = services(span)
		)
		spanKey, err := lex.Encode(traceID, spanTimestamp, spanID)
		if err != nil {
			return err
		}
		traceKeys := [][]byte{}
		for _, service := range services {
			key, err := lex.Encode(service, spanTimestamp, traceID)
			if err != nil {
				return err
			}
			traceKeys = append(traceKeys, key)
		}
		spanBytes, err := encodeSpan(span)
		if err != nil {
			return err
		}

		// First update the service index.  Use a bloom filter to minimise writes.
		{
			b := tx.Bucket(servicesBucket)
			for _, service := range services {
				serviceBytes := []byte(service)
				if s.servicesFilter.Test(serviceBytes) {
					continue
				}
				if err := b.Put(serviceBytes, nil); err != nil {
					return err
				}
				s.servicesFilter.Add(serviceBytes)
			}
		}

		// Next update the span name index.  Use a bloom filter to minimise writes.
		{
			b := tx.Bucket(spanNamesBucket)
			for _, service := range services {
				spanNameBytes, err := lex.Encode(service, span.Name)
				if err != nil {
					return err
				}
				if s.spanNameFilter.Test(spanNameBytes) {
					continue
				}
				if err := b.Put(spanNameBytes, nil); err != nil {
					return err
				}
				s.spanNameFilter.Add(spanNameBytes)
			}
		}

		// Finally put the span
		{
			b := tx.Bucket(tracesBucket)
			if err := b.Put(spanKey, spanBytes); err != nil {
				return err
			}
		}

		// And put an entry in the main index
		{
			b := tx.Bucket(serviceIndexBucket)
			for _, traceKey := range traceKeys {
				if err := b.Put(traceKey, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *boltDBStorage) Services() ([]string, error) {
	var result []string
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(servicesBucket).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			result = append(result, string(k))
		}
		return nil
	})
	return result, err
}

func (s *boltDBStorage) SpanNames(serviceName string) ([]string, error) {
	var result []string
	err := s.db.View(func(tx *bolt.Tx) error {
		prefix := []byte(serviceName)
		c := tx.Bucket([]byte(spanNamesBucket)).Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			var serviceName, spanName string
			if _, err := lex.Decode(k, &serviceName, &spanName); err != nil {
				return err
			}
			result = append(result, spanName)
		}
		return nil
	})
	return result, err
}

func (s *boltDBStorage) Trace(id int64) ([]*zipkincore.Span, error) {
	var result []*zipkincore.Span
	err := s.db.View(func(tx *bolt.Tx) error {
		prefix, err := lex.Encode(id)
		if err != nil {
			return err
		}
		c := tx.Bucket([]byte(tracesBucket)).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			span, err := decodeSpan(v)
			if err != nil {
				return err
			}
			result = append(result, span)
		}
		return nil
	})
	return result, err
}

func (s *boltDBStorage) Traces(query Query) ([][]*zipkincore.Span, error) {
	var result [][]*zipkincore.Span
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(tracesBucket)).Cursor()
		var previousTraceID int64
		var numTraces int
		var currentTrace []*zipkincore.Span

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var traceID, timestamp, spanID int64
			if _, err := lex.Decode(k, &traceID, &timestamp, &spanID); err != nil {
				return err
			}

			// Only want query.Limit traces
			if traceID != previousTraceID {
				previousTraceID = traceID
				numTraces++
				result = append(result, currentTrace)
				currentTrace = nil
			}
			if numTraces > query.Limit {
				return nil
			}

			span, err := decodeSpan(v)
			if err != nil {
				return err
			}
			currentTrace = append(currentTrace, span)
		}
		return nil
	})
	return result, err
}
