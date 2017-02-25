package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/weaveworks-experiments/loki/pkg/storage"
)

const (
	defaultWindowMS = 60 * 60 * 1000
)

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

func Register(router *mux.Router, store storage.SpanStore) {
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
		services, err := store.Services()
		if err != nil {
			log.Errorf("Store error: %v", err)
			return
		}
		if err := json.NewEncoder(w).Encode(services); err != nil {
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
		spanNames, err := store.SpanNames(serviceName)
		if err != nil {
			log.Errorf("Store error: %v", err)
			return
		}
		if err := json.NewEncoder(w).Encode(spanNames); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))

	router.Handle("/api/v1/trace/{id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := fromIDStr(mux.Vars(r)["id"])
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		trace, err := store.Trace(id)
		if err != nil {
			log.Errorf("Store error: %v", err)
			return
		}

		if err := json.NewEncoder(w).Encode(SpansToWire(trace.Spans)); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))

	router.Handle("/api/v1/traces", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nowMS := time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		values := r.URL.Query()

		endTS, err := parseInt64(values, "endTs", nowMS)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		lookback, err := parseInt64(values, "lookback", defaultWindowMS)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		minDuration, err := parseInt64(values, "minDuration", 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		limit, err := parseInt64(values, "limit", 10)
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
			EndMS:         endTS,
			StartMS:       endTS - lookback,
			Limit:         int(limit),
			ServiceName:   serviceName,
			SpanName:      values.Get("spanName"),
			MinDurationUS: minDuration,
		}
		traces, err := store.Traces(query)
		if err != nil {
			log.Errorf("Store error: %v", err)
			return
		}

		if err := json.NewEncoder(w).Encode(TracesToWire(traces)); err != nil {
			log.Errorf("Error marshalling: %v", err)
		}
	}))
}
