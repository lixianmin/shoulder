package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

/********************************************************************
created:    2022-01-13
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type WriterMetrics struct {
	writer *kafka.Writer

	writes       *prometheus.SummaryVec
	messages     *prometheus.SummaryVec
	bytes        *prometheus.SummaryVec
	avgWriteTime *prometheus.HistogramVec
	minWriteTime *prometheus.HistogramVec
	maxWriteTime *prometheus.HistogramVec
}

func NewWriterMetrics(writer *kafka.Writer) *WriterMetrics {
	if writer == nil {
		panic("writer is nil")
	}

	var labels = []string{"kind", "client", "topic"}
	var my = &WriterMetrics{
		writer: writer,
		writes: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "write_count",
				Help:      "kafka write count.",
			}, labels,
		),
		messages: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "write_message_count",
				Help:      "kafka message count.",
			}, labels,
		),
		bytes: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "write_size_bytes",
				Help:      "kafka write sizes in bytes.",
			}, labels,
		),
		avgWriteTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "write_avg_duration_seconds",
				Help:      "kafka average write latencies in seconds.",
			}, labels,
		),
		minWriteTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "write_min_duration_seconds",
				Help:      "kafka min write latencies in seconds.",
			}, labels,
		),
		maxWriteTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "write_max_duration_seconds",
				Help:      "kafka max write latencies in seconds.",
			}, labels,
		),
	}

	prometheus.MustRegister(my.writes, my.messages, my.bytes, my.avgWriteTime, my.minWriteTime, my.maxWriteTime)
	return my
}

func (my *WriterMetrics) Stats() {
	var stats = my.writer.Stats()
	var labels = []string{"writer", stats.ClientID, stats.Topic}

	my.writes.WithLabelValues(labels...).Observe(float64(stats.Writes))
	my.messages.WithLabelValues(labels...).Observe(float64(stats.Messages))
	my.bytes.WithLabelValues(labels...).Observe(float64(stats.Bytes))

	my.avgWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Avg.Seconds())
	my.minWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Min.Seconds())
	my.maxWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Max.Seconds())
}
