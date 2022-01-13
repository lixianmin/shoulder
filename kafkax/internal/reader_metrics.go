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

type ReaderMetrics struct {
	groupId string
	reader  *kafka.Reader

	offset      *prometheus.SummaryVec
	lag         *prometheus.SummaryVec
	messages    *prometheus.SummaryVec
	bytes       *prometheus.SummaryVec
	avgReadTime *prometheus.HistogramVec
	minReadTime *prometheus.HistogramVec
	maxReadTime *prometheus.HistogramVec
}

func NewReaderMetrics(groupId string, reader *kafka.Reader) *ReaderMetrics {
	if reader == nil {
		panic("reader is nil")
	}

	var labels = []string{"kind", "group", "topic", "partition"}
	var my = &ReaderMetrics{
		groupId: groupId,
		reader:  reader,
		offset: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "read_offset",
				Help:      "kafka read offset.",
			}, labels,
		),
		lag: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "read_lag",
				Help:      "kafka read lag.",
			}, labels,
		),
		messages: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "read_messages",
				Help:      "kafka read message count.",
			}, labels,
		),
		bytes: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: metricsNamespace,
				Name:      "read_bytes",
				Help:      "kafka reade size in bytes.",
			}, labels,
		),
		avgReadTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "read_avg_duration_seconds",
				Help:      "kafka average read latencies in seconds.",
			}, labels,
		),
		minReadTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "read_min_duration_seconds",
				Help:      "kafka min read latencies in seconds.",
			}, labels,
		),
		maxReadTime: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Name:      "read_max_duration_seconds",
				Help:      "kafka max read latencies in seconds.",
			}, labels,
		),
	}

	prometheus.MustRegister(my.offset, my.lag, my.messages, my.bytes, my.avgReadTime, my.minReadTime, my.maxReadTime)
	return my
}

func (my *ReaderMetrics) Stats() {
	var stats = my.reader.Stats()
	var labels = []string{"reader", my.groupId, stats.Topic, stats.Partition}

	my.offset.WithLabelValues(labels...).Observe(float64(stats.Offset))
	my.lag.WithLabelValues(labels...).Observe(float64(stats.Lag))
	my.messages.WithLabelValues(labels...).Observe(float64(stats.Messages))
	my.bytes.WithLabelValues(labels...).Observe(float64(stats.Bytes))

	my.avgReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Avg.Seconds())
	my.minReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Min.Seconds())
	my.maxReadTime.WithLabelValues(labels...).Observe(stats.ReadTime.Max.Seconds())
}
