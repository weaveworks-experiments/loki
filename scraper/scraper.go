package scraper

import (
	"fmt"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

type Appender interface {
	Append(*zipkincore.Span) error
}

func NewScraperFn(appender Appender) retrieval.ScraperFn {
	return func(target *retrieval.Target, cfg *config.ScrapeConfig, client *http.Client) retrieval.Scraper {
		return &scraper{
			appender: appender,
			target:   target,
			cfg:      cfg,
			client:   client,
		}
	}
}

type scraper struct {
	appender Appender
	target   *retrieval.Target
	cfg      *config.ScrapeConfig
	client   *http.Client
}

func (s *scraper) NeedsThrottling() bool {
	return false
}

func (s *scraper) Scrape(ctx context.Context, ts time.Time) error {
	req, err := http.NewRequest("GET", s.target.URL().String(), nil)
	if err != nil {
		return err
	}

	resp, err := ctxhttp.Do(ctx, s.client, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	transport := thrift.NewStreamTransportR(resp.Body)
	protocol := thrift.NewTCompactProtocol(transport)

	ttype, size, err := protocol.ReadListBegin()
	if err != nil {
		return err
	}
	if ttype != thrift.STRUCT {
		return fmt.Errorf("unexpected type: %v", ttype)
	}
	for i := 0; i < size; i++ {
		span := zipkincore.NewSpan()
		if err := span.Read(protocol); err != nil {
			return err
		}
		if err := s.appender.Append(span); err != nil {
			return err
		}
	}
	return protocol.ReadListEnd()
}

func (s *scraper) Offset(interval time.Duration) time.Duration {
	return interval
}
