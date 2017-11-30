/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package legacy implements decoding routines for legacy metric
// formats.
package legacy

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// Debug set to true means unexpected input will be skipped and
// spewed to StdErr. When set to false, it will return error instead.
var Debug = false

// MetricBatch represents a single request payload as sent by the
// client application. It can contain multiple measurement cycles,
// each in turn consisting of many individual metrics.
type MetricBatch struct {
	HostID   int          `json:"hostID"`
	Protocol int          `json:"protocol"`
	Data     []MetricData `json:"data"`
}

// MetricData contains the metric data from a single measurement
// time
type MetricData struct {
	Time          time.Time     `json:"time"`
	FloatMetrics  FloatMetrics  `json:"floatMetrics"`
	StringMetrics StringMetrics `json:"stringMetrics"`
	IntMetrics    IntMetrics    `json:"intMetrics"`
}

// FloatMetric represents a single metric value of type float64
type FloatMetric struct {
	Metric  string  `json:"metric"`
	Subtype string  `json:"subtype"`
	Value   float64 `json:"value"`
}

// FloatMetrics is a FloatMetric collection for sort.Interface
type FloatMetrics []FloatMetric

// StringMetric represents a single metric value of type string
type StringMetric struct {
	Metric  string `json:"metric"`
	Subtype string `json:"subtype"`
	Value   string `json:"value"`
}

// StringMetrics is a StringMetric collection for sort.Interface
type StringMetrics []StringMetric

// IntMetric represents a single metric value of type int64
type IntMetric struct {
	Metric  string `json:"metric"`
	Subtype string `json:"subtype"`
	Value   int64  `json:"value"`
}

// IntMetrics is a IntMetric collection for sort.Interface
type IntMetrics []IntMetric

// UnmarshalJSON implements the logic required to parse the JSON
// wireformat into a struct.
func (m *MetricBatch) UnmarshalJSON(data []byte) error {
	var hlp metricBatchParse
	if err := json.Unmarshal(data, &hlp); err != nil {
		return err
	}
	m.HostID = hlp.HostID
	m.Protocol = hlp.Protocol
	m.Data = make([]MetricData, 0)

	for i := range hlp.Data {
		data := MetricData{}
		data.FloatMetrics = make([]FloatMetric, 0)
		data.StringMetrics = make([]StringMetric, 0)
		data.Time = time.Unix(hlp.Data[i].CTime, 0).UTC()

		if hlp.Data[i].Metrics == nil {
			continue
		}
		metrics := hlp.Data[i].Metrics.(map[string]interface{})

		for metric, val := range metrics {
			if err := parseMap(val.(map[string]interface{}), &data, metric); err != nil {
				return err
			}
		}

		m.Data = append(m.Data, data)
	}
	return nil
}

// metricBatchParse is the private struct used by
// MetricBatch.UnmarshalJSON to actually parse the data wireformat
type metricBatchParse struct {
	HostID   int               `json:"host_id"`
	Protocol int               `json:"proto_ver"`
	Data     []metricDataParse `json:"data"`
}

// metricDataParse is another parse helper struct to decode the
// MetricBatch wire format
type metricDataParse struct {
	CTime   int64       `json:"ctime"`
	Metrics interface{} `json:"metrics"`
}

// parseMap is used in MetricBatch.UnmarshalJSON to parse
// encountered map[string]interface{} via reflection
func parseMap(m map[string]interface{}, data *MetricData, metric string) error {
loop:
	for key, val := range m {
		vval := reflect.ValueOf(&val).Elem()
		switch vval.Elem().Kind().String() {
		case `map`:
			if err := parseMap(val.(map[string]interface{}), data, metric); err != nil {
				return err
			}
		case `slice`:
			if err := parseSlice(val.([]interface{}), data, metric, key); err != nil {
				return err
			}
		case `string`:
			parseMapString(key, val.(string), metric, data)
		case `float64`:
			parseMapFloat(key, metric, val.(float64), data)
		case `int64`:
			parseMapInt(key, metric, val.(int64), data)
		case `bool`:
			parseMapString(key, fmt.Sprintf("%t", val.(bool)), metric, data)
		default:
			msg := fmt.Sprintf("parseMap unknown type: %s",
				vval.Elem().Kind().String())
			if Debug {
				fmt.Fprintln(os.Stderr, msg)
				spew.Fdump(os.Stderr, val)
				continue loop
			}
			return fmt.Errorf(msg)
		}
	}
	return nil
}

// parseSlice is used in MetricBatch.UnmarshalJSON to parse
// encountered []interface{} via reflection
func parseSlice(s []interface{}, data *MetricData, metric, key string) error {
loop:
	for _, val := range s {
		vval := reflect.ValueOf(&val).Elem()
		switch vval.Elem().Kind().String() {
		case `map`:
			if err := parseMap(val.(map[string]interface{}), data, metric); err != nil {
				return err
			}
		case `slice`:
			if err := parseSlice(val.([]interface{}), data, metric, key); err != nil {
				return err
			}
		case `string`:
			parseSliceString(key, val.(string), metric, data)
		default:
			msg := fmt.Sprintf("parseSlice unknown type: %s",
				vval.Elem().Kind().String())
			if Debug {
				fmt.Fprintln(os.Stderr, msg)
				spew.Fdump(os.Stderr, val)
				continue loop
			}
			return fmt.Errorf(msg)
		}
	}
	return nil
}

// parseMapString decodes a single key/value pair key, val with a
// string value
func parseMapString(key, val, metric string, data *MetricData) {
	s := StringMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.StringMetrics = append(data.StringMetrics, s)
}

// parseSliceString decodes a string value from a slice
func parseSliceString(key, val, metric string, data *MetricData) {
	s := StringMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.StringMetrics = append(data.StringMetrics, s)
}

// parseMapFloat decodes a single key/value pair key, val with a
// float64 value. It calls parseMapInt if the float64 value can be
// represented by an int64
func parseMapFloat(key, metric string, val float64, data *MetricData) {
	if floatIsInt(val) {
		parseMapInt(key, metric, int64(val), data)
		return
	}
	f := FloatMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.FloatMetrics = append(data.FloatMetrics, f)
}

// parseMapInt decodes a single key/value pair key, val with a
// int64 value.
func parseMapInt(key, metric string, val int64, data *MetricData) {
	i := IntMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.IntMetrics = append(data.IntMetrics, i)
}

// floatIsInt returns true if the float64 value can be represented
// by an int64. False otherwise.
func floatIsInt(f float64) bool {
	if f == float64(int64(f)) {
		return true
	}
	return false
}

// Len returns the length of the collection
func (slice FloatMetrics) Len() int {
	return len(slice)
}

// Less sorts in ascending lexical order
func (slice FloatMetrics) Less(i, j int) bool {
	return slice[i].Metric < slice[j].Metric
}

// Swap switches the position of the slice elements i, j
func (slice FloatMetrics) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// Len returns the length of the collection
func (slice StringMetrics) Len() int {
	return len(slice)
}

// Less sorts in ascending lexical order
func (slice StringMetrics) Less(i, j int) bool {
	return slice[i].Metric < slice[j].Metric
}

// Swap switches the position of the slice elements i, j
func (slice StringMetrics) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// Len returns the length of the collection
func (slice IntMetrics) Len() int {
	return len(slice)
}

// Less sorts in ascending lexical order
func (slice IntMetrics) Less(i, j int) bool {
	return slice[i].Metric < slice[j].Metric
}

// Swap switches the position of the slice elements i, j
func (slice IntMetrics) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// MarshalJSON marshals a struct MetricBatch into the reduced wire
// format expected by downstream consumers
func (m *MetricBatch) MarshalJSON() ([]byte, error) {
	var err error
	var mBuf []byte
	var addComma bool
	var j string

	j = `{"host_id":`
	j += fmt.Sprintf("%.0f", float64(m.HostID)) + `,`
	j += `"proto_ver":`
	j += fmt.Sprintf("%.0f", float64(m.Protocol)) + `,`
	j += `"data":[`

	for i := range m.Data {
		if addComma {
			j += `,`
		}
		if mBuf, err = m.Data[i].MarshalJSON(); err != nil {
			goto fail
		}
		j += string(mBuf)
		addComma = true
	}
	j += `]}`

	return []byte(j), nil

fail:
	return []byte{}, nil
}

// MarshalJSON assembles a JSON string for MetricData
func (m *MetricData) MarshalJSON() ([]byte, error) {
	var err error
	var mBuf []byte
	var hasFloatMetrics, hasStringMetrics bool
	var j string

	j = `{"ctime":`
	j += fmt.Sprintf("%.0f", float64(m.Time.Unix())) + `,`
	j += `{"metrics":{`

	if m.FloatMetrics.Len() > 0 {
		hasFloatMetrics = true
		sort.Sort(m.FloatMetrics)
		if mBuf, err = m.FloatMetrics.MarshalJSON(); err != nil {
			goto fail
		}
		j += string(mBuf)
	}
	if hasFloatMetrics && (m.StringMetrics.Len() > 0 || m.IntMetrics.Len() > 0) {
		j += `,`
	}

	if m.StringMetrics.Len() > 0 {
		hasStringMetrics = true
		sort.Sort(m.StringMetrics)
		if mBuf, err = m.StringMetrics.MarshalJSON(); err != nil {
			goto fail
		}
		j += string(mBuf)
	}
	if hasStringMetrics && m.IntMetrics.Len() > 0 {
		j += `,`
	}

	if m.IntMetrics.Len() > 0 {
		sort.Sort(m.IntMetrics)
		if mBuf, err = m.IntMetrics.MarshalJSON(); err != nil {
			goto fail
		}
		j += string(mBuf)
	}

	j += `}`
	return []byte(j), nil

fail:
	return []byte{}, err
}

// MarshalJSON assembles a JSON substring for FloatMetrics
func (slice FloatMetrics) MarshalJSON() ([]byte, error) {
	var lastMetric string
	var j string

	for i := range slice {
		switch lastMetric {
		case ``:
			// first element
			j = `"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", slice[i].Value)
		case slice[i].Metric:
			// additional value for this metric
			j += `,"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", slice[i].Value)
		default:
			// metric switched
			lastMetric = slice[i].Metric
			j += `},` +
				`"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", slice[i].Value)
		}
	}
	if len(slice) > 0 {
		// close last entry
		j += `}`
	}

	return []byte(j), nil
}

// MarshalJSON assembles a JSON substring for IntMetrics
func (slice IntMetrics) MarshalJSON() ([]byte, error) {
	var lastMetric string
	var j string

	for i := range slice {
		switch lastMetric {
		case ``:
			// first element
			j = `"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", float64(slice[i].Value))
		case slice[i].Metric:
			// additional value for this metric
			j += `,"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", float64(slice[i].Value))
		default:
			// metric switched
			lastMetric = slice[i].Metric
			j += `},` +
				`"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.0f", float64(slice[i].Value))
		}
	}
	if len(slice) > 0 {
		// close last entry
		j += `}`
	}

	return []byte(j), nil
}

// MarshalJSON assembles a JSON substring for StringMetrics
func (slice StringMetrics) MarshalJSON() ([]byte, error) {
	var lastMetric string
	var j string
	ip4Metrics := []StringMetric{}
	ip6Metrics := []StringMetric{}

loop:
	for i := range slice {
		// intercept special formatted metrics
		switch slice[i].Metric {
		case `/sys/net/ipv4_addr`:
			ip4Metrics = append(ip4Metrics, slice[i])
			continue loop
		case `/sys/net/ipv6_addr`:
			ip6Metrics = append(ip6Metrics, slice[i])
			continue loop
		}

		switch lastMetric {
		case ``:
			// first element
			lastMetric = slice[i].Metric
			j = `"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				`"` + slice[i].Value + `"`
		case slice[i].Metric:
			// additional value for this metric
			j += `,"` + slice[i].Subtype + `":` +
				`"` + slice[i].Value + `"`
		default:
			// metric switched
			lastMetric = slice[i].Metric
			j += `},` +
				`"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				`"` + slice[i].Value + `"`
		}
	}
	if len(slice) > 0 {
		// close last entry
		j += `}`
	}

	addComma := false
	if len(ip4Metrics) > 0 {
		j += `,"/sys/net/ipv4_addr":{` +
			`"":{` + `"ips":[`
		for i := range ip4Metrics {
			if addComma {
				j += `,`
			}
			j += `{"` + ip4Metrics[i].Subtype + `":` +
				`"` + ip4Metrics[i].Value + `"}`
			addComma = true
		}
		j += `]}}`
	}

	addComma = false
	if len(ip6Metrics) > 0 {
		j += `,"/sys/net/ipv6_addr":{` +
			`"":{` + `"ips":[`
		for i := range ip6Metrics {
			if addComma {
				j += `,`
			}
			j += `{"` + ip6Metrics[i].Subtype + `":` +
				`"` + ip6Metrics[i].Value + `"}`
			addComma = true
		}
		j += `]}}`
	}

	return []byte(j), nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
