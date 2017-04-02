package scraper

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	prom_model "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/weaveworks-experiments/loki/pkg/model"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

type Appender interface {
	Append(*model.Span) error
}

func NewScraperFn(appender Appender) retrieval.ScraperFn {
	return func(target *retrieval.Target, client *http.Client, _ prom_model.LabelSet, cfg *config.ScrapeConfig) retrieval.Scraper {
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

func (s *scraper) Scrape(ctx context.Context) error {
	if err := s.scrape(ctx); err != nil {
		log.Errorf("Error scraping %s: %v", s.target.URL().String(), err)
		return err
	}
	return nil
}

func (s *scraper) scrape(ctx context.Context) error {
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

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var spans model.Spans
	if err := spans.Unmarshal(buf); err != nil {
		return err
	}

	// Pick out the job and use that as the service name and the
	// instance as the address/port
	labels := s.target.Labels()
	tags := make([]model.KeyValue, 0, len(labels))
	for k, v := range labels {
		if tag, ok := model.KeyValueFrom(string(k), string(v)); ok {
			tags = append(tags, tag)
		}
	}

	log.Infof("Scraping %s - %d spans", s.target.URL().String(), len(spans.Spans))
	for _, span := range spans.Spans {
		span.Tags = append(span.Tags, tags...)
		if err := s.appender.Append(span); err != nil {
			return err
		}
	}
	return nil
}
