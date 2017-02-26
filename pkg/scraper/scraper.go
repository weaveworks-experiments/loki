package scraper

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	client "github.com/weaveworks-experiments/loki/pkg/client"
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

func (s *scraper) Offset(interval time.Duration) time.Duration {
	return interval
}

func (s *scraper) Scrape(ctx context.Context, ts time.Time) error {

	if err := s.scrape(ctx, ts); err != nil {
		log.Errorf("Error scraping %s: %v", s.target.URL().String(), err)
		return err
	}
	return nil
}

func (s *scraper) scrape(ctx context.Context, ts time.Time) error {
	req, err := http.NewRequest("GET", s.target.URL().String(), nil)
	if err != nil {
		log.Errorf("1: %v", err)
		return err
	}

	resp, err := ctxhttp.Do(ctx, s.client, req)
	if err != nil {
		log.Errorf("2: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	spans, err := client.ReadSpans(resp.Body)
	if err != nil {
		log.Errorf("3: %v", err)
		return err
	}

	// Pick out the job and use that as the service name and the
	// instance as the address/port
	labels := s.target.Labels()
	endpoint := zipkincore.NewEndpoint()
	endpoint.ServiceName = string(labels[model.JobLabel])
	if hostname, port, err := net.SplitHostPort(string(labels[model.InstanceLabel])); err == nil {
		port, err := strconv.Atoi(port)
		if err != nil {
			endpoint.Port = int16(port)
		}
		if ip := net.ParseIP(hostname); ip != nil {
			endpoint.Ipv4 = int32(binary.BigEndian.Uint32(ip.To4()))
		}
	}

	log.Infof("Scraping %s - %d spans", s.target.URL().String(), len(spans))
	for _, span := range spans {

		for _, annotation := range span.Annotations {
			annotation.Host = endpoint
		}
		for _, annotation := range span.BinaryAnnotations {
			annotation.Host = endpoint
		}

		if err := s.appender.Append(span); err != nil {
			return err
		}
	}
	return nil
}
