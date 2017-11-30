/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// MetricSplit represents a de-batched, self-contained metric from
// a MetricBatch, suitable for forwarding towards event processing
type MetricSplit struct {
	AssetID int64
	Path    string
	TS      time.Time
	Type    string
	Unit    string
	Val     MetricValue
	Tags    []string
	Labels  map[string]string
}

// MetricValue contains the value of MetricSplit with support
// for multiple value types
type MetricValue struct {
	IntVal int64
	StrVal string
	FlpVal float64
}

// MarshalJSON marshals a struct MetricSplit into the reduced wire
// format expected by downstream consumers
func (m *MetricSplit) MarshalJSON() ([]byte, error) {
	o := make([]interface{}, 8)
	o[0] = m.AssetID
	o[1] = m.Path
	o[2] = m.TS.Format(time.RFC3339)
	o[3] = m.Type
	o[4] = m.Unit
	o[6] = m.Tags
	o[7] = m.Labels
	switch m.Type {
	case `integer`:
		fallthrough
	case `long`:
		o[5] = m.Val.IntVal
	case `real`:
		o[5] = m.Val.FlpVal
	case `string`:
		o[5] = m.Val.StrVal
	}
	return json.Marshal(&o)
}

// UnmarshalJSON converts the reduced wire format into struct
// MetricSplit
func (m *MetricSplit) UnmarshalJSON(data []byte) error {
	var raw []interface{}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	// build MetricSplit
	m.AssetID = int64(raw[0].(float64))
	m.Path = raw[1].(string)
	m.Type = raw[3].(string)
	m.Unit = raw[4].(string)

	// decode tags array
	m.Tags = []string{}
	for _, t := range raw[6].([]interface{}) {
		m.Tags = append(m.Tags, t.(string))
	}

	// decode label map
	m.Labels = map[string]string{}
	for k, v := range raw[7].(map[string]interface{}) {
		m.Labels[k] = v.(string)
	}

	// decode timestamp
	if m.TS, err = time.Parse(time.RFC3339Nano, raw[2].(string)); err != nil {
		return err
	}

	// decode value
	switch m.Type {
	case `integer`:
		fallthrough
	case `long`:
		if m.Val.IntVal, err = strconv.ParseInt(raw[5].(string), 10, 0); err != nil {
			return err
		}
	case `real`:
		if m.Val.FlpVal, err = strconv.ParseFloat(raw[5].(string), 64); err != nil {
			return err
		}
	case `string`:
		m.Val.StrVal = raw[5].(string)
	default:
		return fmt.Errorf("UnmarshalJSON: ignoring unknown metric type: %s", m.Type)
	}
	return nil
}

// Value returns the value of m
func (m *MetricSplit) Value() interface{} {
	switch m.Type {
	case `integer`:
		fallthrough
	case `long`:
		return m.Val.IntVal
	case `real`:
		return m.Val.FlpVal
	case `string`:
		return m.Val.StrVal
	default:
		return nil
	}
}

// LookupID returns the LookupID hash for m
func (m *MetricSplit) LookupID() string {
	a := strconv.FormatInt(m.AssetID, 10)
	h := sha256.New()
	h.Write([]byte(a))
	h.Write([]byte(m.Path))

	return hex.EncodeToString(h.Sum(nil))
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
