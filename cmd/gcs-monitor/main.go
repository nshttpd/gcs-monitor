package main

import (
	"flag"
	"os"
	"strings"

	"go.uber.org/zap"
	"github.com/nshttpd/gcs-monitor"
)

func main() {
	project := flag.String("project", "", "project pubsub queues will be in")
	queues := flag.String("queues", "", "list of queues (comma separated) to listen to")
	//	port := flag.Int("port", 9142, "port to listen on for /metrics scraping")
	flag.Parse()

	if *project == "" || *queues == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	qlist := strings.Split(*queues, ",")

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting up gcs-monitor",
		zap.String("project", *project),
		zap.Strings("queues", qlist),
	)

	metrics := new(gcs_monitor.PromMetrics)
	_, err := metrics.SetupPrometheus(*project)

	if err != nil {
		logger.Error("metrics init",
			zap.Error(err),
		)
		os.Exit(1)
	}

	os.Exit(0)
}
