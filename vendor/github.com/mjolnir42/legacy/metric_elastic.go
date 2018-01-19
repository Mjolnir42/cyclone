/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import (
	"time"
)

// MetricElastic is an intermediate metric representation between
// MetricBatch and the singleton's MetricSplit that de-batches
// suitable for insertion into ElasticSearch
type MetricElastic struct {
	HostID  int                            `json:"hostID"`
	Time    time.Time                      `json:"timestamp"`
	Metrics map[string]map[string]string   `json:"metrics"`
	Collect map[string]map[string][]string `json:"collections"`
}

// ElasticFromBatch converts batch into a slice of MetricElastic
func ElasticFromBatch(batch *MetricBatch) []MetricElastic {
	res := make([]MetricElastic, len(batch.Data))
	for i, data := range batch.Data {
		e := MetricElastic{
			HostID:  batch.HostID,
			Time:    data.Time,
			Metrics: make(map[string]map[string]string),
			Collect: make(map[string]map[string][]string),
		}
		for _, m := range data.StringMetrics {
			switch m.Metric {
			case `/sys/net/ipv4_addr`, `/sys/net/ipv6_addr`:
				if _, ok := e.Collect[m.Metric]; !ok {
					e.Collect[m.Metric] = make(map[string][]string)
				}
				if v := e.Collect[m.Metric][m.Subtype]; v == nil {
					e.Collect[m.Metric][m.Subtype] = []string{}
				}
				e.Collect[m.Metric][m.Subtype] = append(
					e.Collect[m.Metric][m.Subtype],
					m.Value,
				)
			default:
				if _, ok := e.Metrics[m.Metric]; !ok {
					e.Metrics[m.Metric] = make(map[string]string)
				}
				e.Metrics[m.Metric][m.Subtype] = m.Value
			}
		}
		res[i] = e
	}
	return res
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
