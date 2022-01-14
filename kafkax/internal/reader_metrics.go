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
	readLabels = []string{"kind", "group", "topic", "partition"}

	readOffset = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "read_offset",
			Help:      "kafka read offset.",
		}, readLabels,
	)

	readLag = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "read_lag",
			Help:      "kafka read lag.",
		}, readLabels,
	)

	readMessages = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "read_messages",
			Help:      "kafka read message count.",
		}, readLabels,
	)

	readBytes = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: metricsNamespace,
			Name:      "read_bytes",
			Help:      "kafka reade size in writeBytes.",
		}, readLabels,
	)

	readAvgReadTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "read_avg_duration_seconds",
			Help:      "kafka average read latencies in seconds.",
		}, readLabels,
	)

	readMinReadTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "read_min_duration_seconds",
			Help:      "kafka min read latencies in seconds.",
		}, readLabels,
	)

	readMaxReadTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "read_max_duration_seconds",
			Help:      "kafka max read latencies in seconds.",
		}, readLabels,
	)
)

func init() {
	prometheus.MustRegister(readOffset, readLag, readMessages, readBytes, readAvgReadTime, readMinReadTime, readMaxReadTime)
}

func TakeReaderStats(reader *kafka.Reader, groupId string) {
	var stats = reader.Stats()
	var labels = []string{"reader", groupId, stats.Topic, stats.Partition}

	readOffset.WithLabelValues(labels...).Observe(float64(stats.Offset))
	readLag.WithLabelValues(labels...).Observe(float64(stats.Lag))
	readMessages.WithLabelValues(labels...).Observe(float64(stats.Messages))
	readBytes.WithLabelValues(labels...).Observe(float64(stats.Bytes))

	readAvgReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Avg.Seconds())
	readMinReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Min.Seconds())
	readMaxReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Max.Seconds())
}
