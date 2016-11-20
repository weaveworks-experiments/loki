package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/weaveworks/scope/common/middleware"

	"github.com/tomwilkie/loki/scraper"
	"github.com/tomwilkie/loki/storage"
	"github.com/tomwilkie/loki/zipkin-ui"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.ExponentialBuckets(0.000128, 4, 10),
	}, []string{"method", "route", "status_code", "ws"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

func main() {
	listenPort := flag.Int("web.listen-port", 80, "HTTP server listen port.")
	configFile := flag.String("config.file", "loki.yml", "Loki configuration file name.")

	config, err := config.LoadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	store := storage.NewSpanStore()

	targetManager := retrieval.NewTargetManager(scraper.NewScraperFn(store))
	targetManager.ApplyScrapeConfig(config.ScrapeConfigs)

	router := mux.NewRouter()

	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	})

	router.Handle("/config.json", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(struct {
			DefaultLookback int `json:"defaultLookback"`
			QueryLimit      int `json:"queryLimit"`
		}{
			DefaultLookback: 3600000,
			QueryLimit:      10,
		}); err != nil {
			log.Errorf("Error marshalling config: %v", err)
		}
	}))
	router.Handle("/api/v1/dependencies", noopHandler)
	router.Handle("/api/v1/services", noopHandler)
	router.Handle("/api/v1/spans", noopHandler)
	router.Handle("/api/v1/trace/{id}", noopHandler)
	router.Handle("/api/v1/traces", noopHandler)

	router.Handle("/metrics", prometheus.Handler())
	router.PathPrefix("/").Handler(ui.Handler)

	instrumented := middleware.Merge(
		middleware.Log{
			LogSuccess: true,
		},
		middleware.Instrument{
			Duration:     requestDuration,
			RouteMatcher: router,
		},
	).Wrap(router)

	go http.ListenAndServe(fmt.Sprintf(":%d", *listenPort), instrumented)
	term := make(chan os.Signal)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	<-term
	log.Warn("Received SIGTERM, exiting gracefully...")
}
