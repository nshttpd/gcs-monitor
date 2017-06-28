package main

import (
	"flag"
	"os"
	"strings"

	"context"

	"cloud.google.com/go/pubsub"
	"github.com/nshttpd/gcs-monitor"
	"go.uber.org/zap"
)

const (
	defaultNumWorkers = 5
)

func main() {
	project := flag.String("project", "", "project pubsub queues will be in")
	topics := flag.String("topics", "", "list of topics (comma separated) to listen to")
	workers := flag.Int("workers", defaultNumWorkers, "number of workers per pub/sub queue")
	//	port := flag.Int("port", 9142, "port to listen on for /metrics scraping")
	flag.Parse()

	if *project == "" || *topics == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	tlist := strings.Split(*topics, ",")

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting up gcs-monitor",
		zap.String("project", *project),
		zap.Strings("queues", tlist),
	)

	metrics := new(gcs_monitor.PromMetrics)
	_, err := metrics.SetupPrometheus(*project)

	if err != nil {
		logger.Error("metrics init",
			zap.Error(err),
		)
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, *project)
	if err != nil {
		logger.Error("error creating pubsub client",
			zap.String("project", *project),
		)
	}

	defer client.Close()

	for _, t := range tlist {
		h := gcs_monitor.NewHandler(logger, metrics, *workers, ctx)
		err = h.Init(client, *project, t); if err != nil {
			logger.Error("error initializing handler",
				zap.String("project", *project),
				zap.String("topic", t),
				zap.Error(err),
			)
		}
	}

	os.Exit(0)
}
