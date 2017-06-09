package gcs_monitor

import (
	"go.uber.org/zap"
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"time"
)

type BucketHandler struct {
	logger  *zap.Logger
	metrics *PromMetrics
	workers int
	sub     *pubsub.Subscription
}

const (
	subName = "gcs-monitor"
)

func NewHandler(logger *zap.Logger, metrics *PromMetrics, workers int) *BucketHandler {
	return &BucketHandler{
		logger: logger,
		metrics: metrics,
		workers: workers,
	}
}

func (h *BucketHandler) Init(client *pubsub.Client, project string, topic string) error {

	ctx := context.Background()

	h.logger.Info("initializing pubsub",
		zap.String("project", project),
		zap.String("topic", topic),
	)

	t := client.Topic(topic)

	ok, err := t.Exists(ctx); if err != nil {
		return err
	}

	if !ok {
		return errors.New("topic doesn't exist")
	}

	h.sub = client.Subscription(subName)

	ok, err = h.sub.Exists(ctx); if err != nil {
		return err
	}

	if !ok {
		cfg := pubsub.SubscriptionConfig{
			Topic: t,
			AckDeadline: 20 * time.Second,
		}

		h.sub, err = client.CreateSubscription(ctx, subName, cfg); if err != nil {
			return err
		}
	}

	return nil
}
