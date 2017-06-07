package gcs_monitor

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	promNamespace = "gcsmonitor"
)

var (
	labelNames = []string{"type", "project", "bucket"}
)

type PromMetrics struct {
	Project         string
	sizeSummary     *prometheus.SummaryVec
	fileCounter     *prometheus.CounterVec
	zeroFileCounter *prometheus.CounterVec
}

func metricStringCleanup(in string) string {
	return strings.Replace(in, "-", "_", -1)
}

// et == eventType, b == bucket
func (p *PromMetrics) makeLabels(et string, b string) prometheus.Labels {
	labels := make(prometheus.Labels)
	labels["project"] = p.Project
	labels["type"] = metricStringCleanup(et)
	labels["bucket"] = metricStringCleanup(b)
	return labels
}

func (p *PromMetrics) SetupPrometheus(project string) (http.Handler, error) {
	p.Project = project

	p.sizeSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: promNamespace,
		Name:      "size",
		Help:      "bucket object file size summary",
	}, labelNames)

	p.fileCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: "file",
		Name:      "count",
		Help:      "file per bucket count",
	}, labelNames)

	p.zeroFileCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: "zero",
		Name:      "count",
		Help:      "zero file per bucket count",
	}, labelNames)

	if err := prometheus.Register(p.sizeSummary); err != nil {
		return nil, err
	}

	if err := prometheus.Register(p.fileCounter); err != nil {
		return nil, err
	}

	if err := prometheus.Register(p.zeroFileCounter); err != nil {
		return nil, err
	}

	return promhttp.Handler(), nil
}

func (p *PromMetrics) ObserveSize(eventType string, bucket string, size float64) {
	labels := p.makeLabels(eventType, bucket)
	p.sizeSummary.With(labels).Observe(size)
}

func (p *PromMetrics) IncFileCounter(eventType string, bucket string) {
	labels := p.makeLabels(eventType, bucket)
	p.fileCounter.With(labels).Inc()
}

func (p *PromMetrics) IncZeroFileCounter(eventType string, bucket string) {
	labels := p.makeLabels(eventType, bucket)
	p.zeroFileCounter.With(labels).Inc()
}
