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
	"github.com/weaveworks/scope/common/middleware"
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

	router := mux.NewRouter()

	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	router.Handle("/api/v1/dependencies", noopHandler)
	router.Handle("/api/v1/services", noopHandler)
	router.Handle("/api/v1/spans", noopHandler)
	router.Handle("/api/v1/trace/{id}", noopHandler)
	router.Handle("/api/v1/traces", noopHandler)

	router.Handle("/metrics", prometheus.Handler())

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
