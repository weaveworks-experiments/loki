package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/weaveworks/scope/common/middleware"

	"github.com/tomwilkie/loki/api"
	"github.com/tomwilkie/loki/scraper"
	"github.com/tomwilkie/loki/storage"
	"github.com/tomwilkie/loki/zipkin-ui"
)

const (
	defaultWindowMS = 60 * 60 * 1000
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

func parseInt64(values url.Values, key string, def int64) (int64, error) {
	value := values.Get(key)
	if value == "" {
		return def, nil
	}

	intVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}

	return intVal, nil
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

	router.Handle("/api/v1/dependencies", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))

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

	router.Handle("/api/v1/services", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(store.Services()); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))
	router.Handle("/api/v1/spans", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		serviceName := values.Get("serviceName")
		if serviceName == "" {
			http.Error(w, "serviceName required", http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(store.SpanNames(serviceName)); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))
	router.Handle("/api/v1/trace/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := api.FromIdStr(mux.Vars(r)["id"])
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		trace := store.Trace(id)
		if err := json.NewEncoder(w).Encode(api.SpansToWire(trace)); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))
	router.Handle("/api/v1/traces", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nowMS := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		values := r.URL.Query()

		startTS, err := parseInt64(values, "startTS", nowMS-defaultWindowMS)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		endTS, err := parseInt64(values, "endTS", nowMS)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		serviceName := values.Get("serviceName")
		if serviceName == "" {
			http.Error(w, "serviceName required", http.StatusBadRequest)
			return
		}

		query := storage.Query{
			EndMS:       endTS,
			StartMS:     startTS,
			Limit:       10,
			ServiceName: serviceName,
		}
		traces := store.Traces(query)
		if err := json.NewEncoder(w).Encode(api.TracesToWire(traces)); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))

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
