/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/internal/cyclone"

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/eyewall"
	"github.com/mjolnir42/legacy"
)

// evaluate tests m.Value against threshold t. It returns the resulting
// alarmlevel and metric value as string as well as the number of
// evalutations that had to be perfomed.
func (c *Cyclone) evaluate(m *legacy.MetricSplit, t eyewall.Threshold) (string, string, int64) {
	logrus.Debugf(
		"[%d]: evaluating metric %s from %d"+
			" against config %s",
		c.Num, m.Path, m.AssetID, t.ID,
	)
	var broken bool
	var evaluations int64
	var value string

lvlloop:
	for _, lvl := range []string{
		`9`, `8`, `7`, `6`, `5`, `4`, `3`, `2`, `1`,
	} {
		thrval, ok := t.Thresholds[lvl]
		if !ok {
			continue lvlloop
		}

		evaluations++
		logrus.Debugf(
			"[%d]: checking %s alarmlevel %s",
			c.Num, t.ID, lvl,
		)
		switch m.Type {
		case `integer`:
			fallthrough
		case `long`:
			broken, value = c.cmpInt(t.Predicate,
				m.Value().(int64),
				thrval)
		case `real`:
			broken, value = c.cmpFlp(t.Predicate,
				m.Value().(float64),
				thrval)
		default:
			continue lvlloop
		}
		if broken {
			return lvl, value, evaluations
		}
	}
	return `0`, value, evaluations
}

// cmpInt compares an integer value against a threshold
func (c *Cyclone) cmpInt(pred string, value, threshold int64) (bool, string) {
	fVal := fmt.Sprintf("%d", value)
	switch pred {
	case `<`:
		return value < threshold, fVal
	case `<=`:
		return value <= threshold, fVal
	case `==`:
		return value == threshold, fVal
	case `>=`:
		return value >= threshold, fVal
	case `>`:
		return value > threshold, fVal
	case `!=`:
		return value != threshold, fVal
	default:
		logrus.Errorf(
			"Cyclone[%d], ERROR unknown predicate: %s",
			c.Num, pred)
		return false, ``
	}
}

// cmpFlp compares a floating point value against a threshold
func (c *Cyclone) cmpFlp(pred string, value float64, threshold int64) (bool, string) {
	fthreshold := float64(threshold)
	fVal := fmt.Sprintf("%.3f", value)
	switch pred {
	case `<`:
		return value < fthreshold, fVal
	case `<=`:
		return value <= fthreshold, fVal
	case `==`:
		return value == fthreshold, fVal
	case `>=`:
		return value >= fthreshold, fVal
	case `>`:
		return value > fthreshold, fVal
	case `!=`:
		return value != fthreshold, fVal
	default:
		logrus.Errorf(
			"Cyclone[%d], ERROR unknown predicate: %s",
			c.Num, pred,
		)
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
