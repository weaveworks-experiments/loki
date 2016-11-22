package main

import (
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

	"github.com/tomwilkie/loki/pkg/api"
	"github.com/tomwilkie/loki/pkg/scraper"
	"github.com/tomwilkie/loki/pkg/storage"
	"github.com/tomwilkie/loki/pkg/zipkin-ui"
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
	flag.Parse()

	config, err := config.LoadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	store := storage.NewSpanStore()

	targetManager := retrieval.NewTargetManager(scraper.NewScraperFn(store))
	targetManager.ApplyScrapeConfig(config.ScrapeConfigs)
	go targetManager.Run()
	defer targetManager.Stop()

	router := mux.NewRouter()
	api.Register(router, store)

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
