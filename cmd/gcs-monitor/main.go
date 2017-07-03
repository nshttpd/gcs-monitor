package main

import (
	"flag"
	"os"
	"strings"

	"context"

	"net/http"

	"net"
	"os/signal"

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
	port := flag.String("port", ":8080", "port to listen on for /metrics")
	level := flag.String("level", "info", "log level")
	flag.Parse()

	if *project == "" || *topics == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	tlist := strings.Split(*topics, ",")

	config := zap.NewProductionConfig()

	// if not debug we don't care and Production Config defaults to Info
	if *level == "debug" {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	logger, _ := config.Build()

	defer logger.Sync()

	logger.Info("starting up gcs-monitor",
		zap.String("project", *project),
		zap.Strings("queues", tlist),
	)

	metrics := new(gcs_monitor.PromMetrics)
	ph, err := metrics.SetupPrometheus(*project)

	if err != nil {
		logger.Error("metrics init error",
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
		os.Exit(1)
	}

	defer client.Close()

	for _, t := range tlist {
		h := gcs_monitor.NewHandler(logger, metrics, *workers, ctx)
		err = h.Init(client, *project, t)
		if err != nil {
			logger.Error("error initializing handler",
				zap.String("project", *project),
				zap.String("topic", t),
				zap.Error(err),
			)
			os.Exit(1)
		}
		go h.Run()
	}

	l, err := net.Listen("tcp", *port)

	if err != nil {
		logger.Error("error setting up listener",
			zap.String("port", *port),
			zap.Error(err),
		)
	}

	mux := http.NewServeMux()

	mux.Handle("/metrics", ph)

	// Register auxiliary handler
	go func() {
		if serr := http.Serve(l, mux); serr != nil {
			logger.Error("unable to start service",
				zap.Error(serr),
			)
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, os.Kill)
	<-sigchan
	err = l.Close()
	if err != nil {
		logger.Error("error while stopping service",
			zap.Error(err),
		)
		os.Exit(1)
	}

	os.Exit(0)
}
