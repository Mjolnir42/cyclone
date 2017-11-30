/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import "encoding/json"

// PluginMetricBatch is the format used by plugins to
// send metrics to the main collector
type PluginMetricBatch struct {
	Metrics []PluginMetric `json:"metrics"`
}

// PluginMetric represents an individual metric gathered
// by a plugin
type PluginMetric struct {
	Type   string
	Metric string
	Label  string
	Value  MetricValue
}

// pluginMetricMarshal is a helper struct to marshal a PluginMetric
// into the wire format
type pluginMetricMarshal struct {
	Metric string      `json:"id"`
	Label  string      `json:"tags"`
	Value  interface{} `json:"value"`
}

// MarshalJSON returns the JSON encoding of PluginMetric
func (p *PluginMetric) MarshalJSON() ([]byte, error) {
	m := pluginMetricMarshal{
		Metric: p.Metric,
		Label:  p.Label,
	}
	switch p.Type {
	case `string`:
		m.Value = p.Value.StrVal
	case `int`, `uint`:
		m.Value = float64(p.Value.IntVal)
	case `float`:
		m.Value = p.Value.FlpVal
	}
	return json.Marshal(m)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
