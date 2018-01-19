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
	"regexp"
	"sort"
	"strconv"
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
			if err := parseMap(val.(map[string]interface{}),
				data, metric); err != nil {
				return err
			}
		case `slice`:
			// special snowflake: multiple neighbors will override
			// each other during MarshalJSON()
			if metric == `/sys/net/quagga/bgp/neighbour` {
				if err := parseSliceStrAsBool(val.([]interface{}),
					data, metric, key); err != nil {
					return err
				}
				continue loop
			}
			// special snowflake: slice of one map per metric
			if key == `countlst` {
				if err := parseSliceCountlst(val.([]interface{}),
					data, metric, key); err != nil {
					return err
				}
				continue loop
			}
			if err := parseSlice(val.([]interface{}),
				data, metric, key); err != nil {
				return err
			}
		case `string`:
			parseString(key, val.(string), metric, data)
		case `float64`:
			parseFloat(key, metric, val.(float64), data)
		case `int64`:
			parseInt(key, metric, val.(int64), data)
		case `bool`:
			if val.(bool) {
				parseInt(key, metric, 1, data)
				continue loop
			}
			parseInt(key, metric, 0, data)
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
			if err := parseMap(val.(map[string]interface{}),
				data, metric); err != nil {
				return err
			}
		case `slice`:
			if err := parseSlice(val.([]interface{}),
				data, metric, key); err != nil {
				return err
			}
		case `string`:
			parseString(key, val.(string), metric, data)
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

// parseSliceStrAsBool is used in MetricBatch.UnmarshalJSON to parse
// encountered []interface{} via reflection, but its contained
// string metrics are parsed as integer metrics where the string
// value becomes the metric key
func parseSliceStrAsBool(s []interface{}, data *MetricData, metric, key string) error {
loop:
	for _, val := range s {
		vval := reflect.ValueOf(&val).Elem()
		switch vval.Elem().Kind().String() {
		case `map`:
			if err := parseMap(val.(map[string]interface{}),
				data, metric); err != nil {
				return err
			}
		case `slice`:
			if err := parseSlice(val.([]interface{}),
				data, metric, key); err != nil {
				return err
			}
		case `string`:
			parseInt(val.(string), metric, 1, data)
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

// parseSliceCountlst is used in MetricBatch.UnmarshalJSON to parse
// special countlst slices that contain maps which represent a single
// metric
func parseSliceCountlst(s []interface{}, data *MetricData, metric, key string) error {
	switch metric {
	case `/sys/net/ipvs/conn/vipconns`:
		for _, val := range s {
			mp := val.(map[string]interface{})
			vip, err := formatHexIP4(mp[`to IP`].(string))
			if err != nil {
				return err
			}
			cnt, err := strconv.Atoi(mp[`count`].(string))
			if err != nil {
				return err
			}
			i := IntMetric{
				Metric:  metric,
				Subtype: vip,
				Value:   int64(cnt),
			}
			data.IntMetrics = append(data.IntMetrics, i)
		}
	case `/sys/net/ipvs/conn/vipstatecount`:
		for _, val := range s {
			mp := val.(map[string]interface{})
			vip, err := formatHexIP4(mp[`to IP`].(string))
			if err != nil {
				return err
			}
			cnt, err := strconv.Atoi(mp[`count`].(string))
			if err != nil {
				return err
			}
			state := mp[`state`].(string)
			i := IntMetric{
				Metric:  metric,
				Subtype: fmt.Sprintf("%s/%s", vip, state),
				Value:   int64(cnt),
			}
			data.IntMetrics = append(data.IntMetrics, i)
		}
	case `/sys/net/ipvs/conn/serverstatecount`:
		for _, val := range s {
			mp := val.(map[string]interface{})
			vip, err := formatHexIP4(mp[`destination IP`].(string))
			if err != nil {
				return err
			}
			cnt, err := strconv.Atoi(mp[`count`].(string))
			if err != nil {
				return err
			}
			state := mp[`state`].(string)
			i := IntMetric{
				Metric:  metric,
				Subtype: fmt.Sprintf("%s/%s", vip, state),
				Value:   int64(cnt),
			}
			data.IntMetrics = append(data.IntMetrics, i)
		}
	case `/sys/net/ipvs/conn/statecount`:
		for _, val := range s {
			mp := val.(map[string]interface{})
			cnt, err := strconv.Atoi(mp[`count`].(string))
			if err != nil {
				return err
			}
			state := mp[`state`].(string)
			i := IntMetric{
				Metric:  metric,
				Subtype: state,
				Value:   int64(cnt),
			}
			data.IntMetrics = append(data.IntMetrics, i)
		}
	case `/sys/net/ipvs/conn/servercount`:
		for _, val := range s {
			mp := val.(map[string]interface{})
			vip, err := formatHexIP4(mp[`IP`].(string))
			if err != nil {
				return err
			}
			cnt, err := strconv.Atoi(mp[`count`].(string))
			if err != nil {
				return err
			}
			i := IntMetric{
				Metric:  metric,
				Subtype: vip,
				Value:   int64(cnt),
			}
			data.IntMetrics = append(data.IntMetrics, i)
		}
	default:
		msg := fmt.Sprintf("parseSliceCountlst unknown metric: %s",
			metric)
		if Debug {
			fmt.Fprintln(os.Stderr, msg)
			spew.Fdump(os.Stderr, s)
			return nil
		}
		return fmt.Errorf(msg)
	}
	return nil
}

// parseString decodes a single key/value pair key, val with a
// string value
func parseString(key, val, metric string, data *MetricData) {
	// this metric a number for bgp state active, but a string
	// for others. Convert this to an IntMetric based on the finite
	// state machine states of BGP
	// RFC1163, p.24ff
	// git.savannah.nongnu.org/quagga/quagga/bgpd/bgp_debug.c#L60-70
	if metric == `/sys/net/quagga/bgp/connstate` {
		if _, err := strconv.Atoi(val); err == nil {
			parseInt(key, metric, 6, data)
			return
		}
		// error fallthrough: handle non-number case
		switch val {
		case `Idle`:
			parseInt(key, metric, 1, data)
		case `Connect`:
			parseInt(key, metric, 2, data)
		case `Active`:
			parseInt(key, metric, 3, data)
		case `OpenSent`:
			parseInt(key, metric, 4, data)
		case `OpenConfirm`:
			parseInt(key, metric, 5, data)
		case `Established`:
			parseInt(key, metric, 6, data)
		default:
			parseInt(key, metric, 0, data)
		}
		return
	}

	// this metric has a string value that represents a time
	// duration. It can be converted to seconds.
	if metric == `/sys/net/quagga/bgp/connage` {
		if age, err := durationToSeconds(val); err == nil {
			parseInt(key, metric, age, data)
			return
		}
		// error fallthrough: handle as string metric
	}

	// check if the string is actually a number, parse as IntMetric
	// if it is
	if i, err := strconv.Atoi(val); err == nil {
		parseInt(key, metric, int64(i), data)
		return
	}

	s := StringMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.StringMetrics = append(data.StringMetrics, s)
}

// parseFloat decodes a single key/value pair key, val with a
// float64 value. It calls parseInt if the float64 value can be
// represented by an int64
func parseFloat(key, metric string, val float64, data *MetricData) {
	if floatIsInt(val) {
		parseInt(key, metric, int64(val), data)
		return
	}
	f := FloatMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.FloatMetrics = append(data.FloatMetrics, f)
}

// parseInt decodes a single key/value pair key, val with a
// int64 value.
func parseInt(key, metric string, val int64, data *MetricData) {
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

// formatHexIP converts IP addresses formatted as hexadecimal bytes
// into more regular formats
func formatHexIP(hexIP string) (string, error) {
	switch len([]byte(hexIP)) {
	case 8:
		return formatHexIP4(hexIP)
	case 32:
		return formatHexIP6(hexIP)
	default:
		msg := fmt.Sprintf("hexIP unknown length: %d",
			len([]byte(hexIP)))
		if Debug {
			fmt.Fprintln(os.Stderr, msg)
			spew.Fdump(os.Stderr, hexIP)
			return `-1`, nil
		}
		return ``, fmt.Errorf(msg)
	}
}

// formatHexIP4 converts IP4 addresses from hexadecimal bytes to
// dotted decimal
func formatHexIP4(hexIP string) (string, error) {
	slice := []byte(hexIP)
	if len(slice) != 8 {
		return ``, fmt.Errorf(`not ip4`)
	}
	var x int64
	var err error
	var IP string
	if x, err = strconv.ParseInt(
		string(slice[0:2]), 16, 64,
	); err != nil {
		return ``, err
	}
	IP += strconv.Itoa(int(x)) + `.`
	if x, err = strconv.ParseInt(
		string(slice[2:4]), 16, 64,
	); err != nil {
		return ``, err
	}
	IP += strconv.Itoa(int(x)) + `.`
	if x, err = strconv.ParseInt(
		string(slice[4:6]), 16, 64,
	); err != nil {
		return ``, err
	}
	IP += strconv.Itoa(int(x)) + `.`
	if x, err = strconv.ParseInt(
		string(slice[6:]), 16, 64,
	); err != nil {
		return ``, err
	}
	IP += strconv.Itoa(int(x))
	return IP, nil
}

// formatHexIP6 converts IP6 addresses from hexadecimal bytes to
// colon notation
func formatHexIP6(hexIP string) (string, error) {
	return ``, fmt.Errorf(`formatHexIP6: not implemented`)
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
	j += `"metrics":{`

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

	j += `}}`
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
			lastMetric = slice[i].Metric
			j = `"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", slice[i].Value)
		case slice[i].Metric:
			// additional value for this metric
			j += `,"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", slice[i].Value)
		default:
			// metric switched
			lastMetric = slice[i].Metric
			j += `},` +
				`"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", slice[i].Value)
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
			lastMetric = slice[i].Metric
			j = `"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", float64(slice[i].Value))
		case slice[i].Metric:
			// additional value for this metric
			j += `,"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", float64(slice[i].Value))
		default:
			// metric switched
			lastMetric = slice[i].Metric
			j += `},` +
				`"` + slice[i].Metric + `":{` +
				`"` + slice[i].Subtype + `":` +
				fmt.Sprintf("%.6f", float64(slice[i].Value))
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

// durationToSeconds parses a quagga connection age duration string
// into a number of seconds
func durationToSeconds(duration string) (int64, error) {
	var years, weeks, days, hours, minutes, seconds int
	var err error
	var age time.Duration

	// as defined by quagga
	s := time.Second
	m := s * 60
	h := m * 60
	d := h * 24
	w := d * 7
	y := d * 365

	// format used for more than one year connection age
	regxInf := regexp.MustCompile(
		`^(?P<years>\d+)y(?P<weeks>\d+)w(?P<days>\d+)d$`,
	)
	// format used for less than one year connection age
	regxYear := regexp.MustCompile(
		`^(?P<weeks>\d+)w(?P<days>\d+)d(?P<hours>\d+)h$`,
	)
	// format used for less than one week connection age
	regxWeek := regexp.MustCompile(
		`^(?P<days>\d+)d(?P<hours>\d+)h(?P<minutes>\d+)m$`,
	)
	// format used for less than one day connection age
	regxDay := regexp.MustCompile(
		`^(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$`,
	)

	switch {
	case regxInf.MatchString(duration):
		matches := regxInf.FindStringSubmatch(duration)
		if years, err = strconv.Atoi(matches[1]); err != nil {
			return 0, err
		}
		if weeks, err = strconv.Atoi(matches[2]); err != nil {
			return 0, err
		}
		if days, err = strconv.Atoi(matches[3]); err != nil {
			return 0, err
		}
		age = (itd(years) * y) + (itd(weeks) * w) + (itd(days) * d)
	case regxYear.MatchString(duration):
		matches := regxYear.FindStringSubmatch(duration)
		if weeks, err = strconv.Atoi(matches[1]); err != nil {
			return 0, err
		}
		if days, err = strconv.Atoi(matches[2]); err != nil {
			return 0, err
		}
		if hours, err = strconv.Atoi(matches[3]); err != nil {
			return 0, err
		}
		age = (itd(weeks) * w) + (itd(days) * d) + (itd(hours) * h)
	case regxWeek.MatchString(duration):
		matches := regxWeek.FindStringSubmatch(duration)
		if days, err = strconv.Atoi(matches[1]); err != nil {
			return 0, err
		}
		if hours, err = strconv.Atoi(matches[2]); err != nil {
			return 0, err
		}
		if minutes, err = strconv.Atoi(matches[3]); err != nil {
			return 0, err
		}
		age = (itd(days) * d) + (itd(hours) * h) + (itd(minutes) * m)
	case regxDay.MatchString(duration):
		matches := regxDay.FindStringSubmatch(duration)
		if hours, err = strconv.Atoi(matches[1]); err != nil {
			return 0, err
		}
		if minutes, err = strconv.Atoi(matches[2]); err != nil {
			return 0, err
		}
		if seconds, err = strconv.Atoi(matches[3]); err != nil {
			return 0, err
		}
		age = (itd(hours) * h) + (itd(minutes) * m) + (itd(seconds) * s)
	default:
		return 0, fmt.Errorf("durationToSeconds: no regexp matched %s",
			duration)
	}
	return int64(age.Seconds()), nil
}

// itd converts from int to time.Duration
func itd(num int) time.Duration {
	return time.Duration(num)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
