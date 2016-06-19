/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package metric

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type Metric struct {
	AssetId int64
	Path    string
	TS      time.Time
	Type    string
	Unit    string
	Val     MetricValue
	Tags    []string
	Labels  map[string]string
}

type MetricValue struct {
	IntVal int64
	StrVal string
	FlpVal float64
}

func FromBytes(buf []byte) (*Metric, error) {
	raw := make([]interface{}, 0)
	err := json.Unmarshal(buf, &raw)
	if err != nil {
		return nil, err
	}

	// build metric
	m := Metric{
		AssetId: int64(raw[0].(float64)),
		Path:    raw[1].(string),
		Type:    raw[3].(string),
		Unit:    raw[4].(string),
	}

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
		return nil, err
	}

	// decode value
	switch m.Type {
	case `integer`:
		fallthrough
	case `long`:
		if m.Val.IntVal, err = strconv.ParseInt(raw[5].(string), 10, 0); err != nil {
			return nil, err
		}
	case `real`:
		if m.Val.FlpVal, err = strconv.ParseFloat(raw[5].(string), 64); err != nil {
			return nil, err
		}
	case `string`:
		m.Val.StrVal = raw[5].(string)
	default:
		return nil, fmt.Errorf("Ignoring unknown metric type: %s", m.Type)
	}
	return &m, nil
}

func (m *Metric) Value() interface{} {
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

func (m *Metric) LookupID() string {
	a := strconv.FormatInt(m.AssetId, 10)
	h := sha256.New()
	h.Write([]byte(a))
	h.Write([]byte(m.Path))

	return hex.EncodeToString(h.Sum(nil))
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
