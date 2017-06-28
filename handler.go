package gcs_monitor

import (
	"go.uber.org/zap"
	"cloud.google.com/go/pubsub"
	context "golang.org/x/net/context"
	"errors"
	"time"
	json "encoding/json"
	storage "google.golang.org/api/storage/v1"
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

func (h *BucketHandler) handle(cancel context.CancelFunc, m *pubsub.Message, t chan int) {
	defer cancel()

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
		}
	}

	if t != nil {
		<-t
	}
}

func (h *BucketHandler) Run() {

	h.logger.Info("starting runnner for subscription",
		zap.String("subscription", h.sub.String()),
		zap.String("topic", h.topic),
	)

	t := make(chan int, h.workers)

	for {
		t <- 1
		cctx, cancel := context.WithCancel(h.ctx)
		err := h.sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			h.logger.Debug("starting message from topic",
				zap.String("topic", h.topic),
				zap.String("msgID", m.ID),
			)
			go h.handle(cancel, m, t)
		})
		if err != context.Canceled {
			h.logger.Error("error processing message",
				zap.Error(err),
			)
		}
	}
}
