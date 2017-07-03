package gcs_monitor

import (
	"encoding/json"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	storage "google.golang.org/api/storage/v1"
	"fmt"
)

type BucketHandler struct {
	logger  *zap.Logger
	metrics *PromMetrics
	workers int
	sub     *pubsub.Subscription
	ctx     context.Context
	topic   string
}

const (
	subName = "gcs-monitor"
)

func NewHandler(logger *zap.Logger, metrics *PromMetrics, workers int, ctx context.Context) *BucketHandler {
	return &BucketHandler{
		logger:  logger,
		metrics: metrics,
		workers: workers,
		ctx:     ctx,
	}
}

func (h *BucketHandler) Init(client *pubsub.Client, project string, topic string) error {

	ctx := context.Background()

	h.logger.Info("initializing pubsub",
		zap.String("project", project),
		zap.String("topic", topic),
	)

	h.topic = topic // for logging purposes later

	t := client.Topic(topic)

	ok, err := t.Exists(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("topic doesn't exist")
	}

	h.sub = client.Subscription(subName)

	ok, err = h.sub.Exists(ctx)
	if err != nil {
		return err
	}

	if !ok {
		cfg := pubsub.SubscriptionConfig{
			Topic:       t,
			AckDeadline: 20 * time.Second,
		}

		h.sub, err = client.CreateSubscription(ctx, subName, cfg)
		h.sub.ReceiveSettings.MaxExtension = 1 * time.Minute
		h.sub.ReceiveSettings.NumGoroutines = h.workers
		if err != nil {
			return err
		}

		h.logger.Debug("created subscription",
			zap.String("topic", h.topic),
			zap.String("subscription", h.sub.String()),
		)
	}

	return nil
}

func (h *BucketHandler) handle(cancel context.CancelFunc, m *pubsub.Message) {
	defer func() {
		cancel()
		h.logger.Debug("canceling context for message",
			zap.String("msgID", m.ID),
		)
	}()

	// eventType and buckeId are always attributes on the message from GCS
	h.metrics.IncFileCounter(m.Attributes["eventType"], m.Attributes["bucketId"])

	if m.Attributes["payloadFormat"] == "JSON_API_V1" {
		var o storage.Object
		err := json.Unmarshal(m.Data, &o)
		if err != nil {
			h.logger.Error("error unmarshalling json payload",
				zap.String("msgID", m.ID),
				zap.Error(err),
			)
		} else {
			if o.Size > 0 {
				h.metrics.ObserveSize(m.Attributes["eventType"], m.Attributes["bucketId"], float64(o.Size))
			} else {
				h.metrics.IncZeroFileCounter(m.Attributes["eventType"], m.Attributes["bucketId"])
			}
			m.Ack()
		}
	}

	return
}

func (h *BucketHandler) Run() {

	h.logger.Info("starting runnner for subscription",
		zap.String("subscription", h.sub.String()),
		zap.String("topic", h.topic),
	)

	for {
		h.logger.Debug("Receive start")
		cctx, cancel := context.WithCancel(h.ctx)
		err := h.sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			h.logger.Debug("starting message from topic",
				zap.String("topic", h.topic),
				zap.String("msgID", m.ID),
			)
			h.handle(cancel, m)
		})
		if err != nil && err != context.Canceled {
			h.logger.Error("error processing message",
				zap.Error(err),
			)
			fmt.Printf("%v\n", err)
		}
		h.logger.Debug("Receive done")
	}
}
