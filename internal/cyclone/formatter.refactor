/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"

import (
	"fmt"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/solnx/legacy"
)

// FormatMetrics is the formatting function to export Cyclone metrics
// via legacy.MetricSocket, implementing legacy.Formatter
func FormatMetrics(batch *legacy.PluginMetricBatch) func(string, interface{}) {
	return func(metric string, v interface{}) {
		switch v.(type) {
		case *metrics.StandardMeter:
			value := v.(*metrics.StandardMeter)
			batch.Metrics = append(batch.Metrics, legacy.PluginMetric{
				Type:   `float`,
				Metric: fmt.Sprintf("%s/avg/rate/1min", metric),
				Value: legacy.MetricValue{
					FlpVal: value.Rate1(),
				},
			})
		case *metrics.StandardGauge:
			value := v.(*metrics.StandardGauge)
			batch.Metrics = append(batch.Metrics, legacy.PluginMetric{
				Type:   `int`,
				Metric: metric,
				Value: legacy.MetricValue{
					IntVal: value.Value(),
				},
			})
		case *metrics.StandardHistogram:
			value := v.(*metrics.StandardHistogram)
			batch.Metrics = append(batch.Metrics, legacy.PluginMetric{
				Type:   `float`,
				Metric: fmt.Sprintf("%s/mean", metric),
				Value: legacy.MetricValue{
					FlpVal: (value.Mean() / 1000000000),
				},
			}, legacy.PluginMetric{
				Type:   `float`,
				Metric: fmt.Sprintf("%s/p99", metric),
				Value: legacy.MetricValue{
					FlpVal: (value.Percentile(0.99) / 1000000000),
				},
			}, legacy.PluginMetric{
				Type:   `float`,
				Metric: fmt.Sprintf("%s/max", metric),
				Value: legacy.MetricValue{
					FlpVal: (float64(value.Max()) / 1000000000),
				},
			})
			value.Clear()
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
