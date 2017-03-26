package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/retrieval"

	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/weaveworks-experiments/loki/pkg/api"
	"github.com/weaveworks-experiments/loki/pkg/scraper"
	"github.com/weaveworks-experiments/loki/pkg/storage"
	"github.com/weaveworks-experiments/loki/pkg/zipkin-ui"
)

func main() {
	serverConfig := server.Config{
		MetricsNamespace: "loki",
	}
	serverConfig.RegisterFlags(flag.CommandLine)
	configFile := flag.String("config.file", "loki.yml", "Loki configuration file name.")
	flag.Parse()

	if err := logging.Setup("info"); err != nil {
		log.Fatal(err)
	}

	config, err := config.LoadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error creating server: %v", err)
	}
	defer server.Shutdown()

	store := storage.NewSpanStore()

	targetManager := retrieval.NewTargetManager(scraper.NewScraperFn(store))
	targetManager.ApplyConfig(config)
	go targetManager.Run()
	defer targetManager.Stop()

	api.Register(server.HTTP, store)
	server.HTTP.PathPrefix("/").Handler(ui.Handler)
	server.Run()
}
