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

var (
	writeLabels = []string{"kind", "client", "topic"}

	writeWrites = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "write_count",
			Help:      "kafka write count.",
		}, writeLabels,
	)

	writeMessages = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "write_message_count",
			Help:      "kafka message count.",
		}, writeLabels,
	)

	writeBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "write_size_bytes",
			Help:      "kafka write sizes in bytes.",
		}, writeLabels,
	)

	writeAvgWriteTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_avg_duration_seconds",
			Help:      "kafka average write latencies in seconds.",
		}, writeLabels,
	)

	writeMinWriteTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_min_duration_seconds",
			Help:      "kafka min write latencies in seconds.",
		}, writeLabels,
	)

	writeMaxWriteTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_max_duration_seconds",
			Help:      "kafka max write latencies in seconds.",
		}, writeLabels,
	)
)

func init() {
	prometheus.MustRegister(writeWrites, writeMessages, writeBytes, writeAvgWriteTime, writeMinWriteTime, writeMaxWriteTime)
}

func TakeWriterStats(writer *kafka.Writer) {
	var stats = writer.Stats()
	var labels = []string{"writer", stats.ClientID, stats.Topic}

	writeWrites.WithLabelValues(labels...).Observe(float64(stats.Writes))
	writeMessages.WithLabelValues(labels...).Observe(float64(stats.Messages))
	writeBytes.WithLabelValues(labels...).Observe(float64(stats.Bytes))

	writeAvgWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Avg.Seconds())
	writeMinWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Min.Seconds())
	writeMaxWriteTime.WithLabelValues(labels...).Observe(stats.WriteTime.Max.Seconds())
}
