/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cpu // import "github.com/mjolnir42/cyclone/lib/cyclone/cpu"

import (
	"time"

	"github.com/mjolnir42/legacy"
)

type CTX struct {
	AssetID   int64
	CurrValue int64
	NextValue int64
	Cps       float64
	CurrTime  time.Time
	NextTime  time.Time
}

func (c *CTX) Update(m *legacy.MetricSplit) *legacy.MetricSplit {
	// ignore metrics for other paths
	switch m.Path {
	case `/sys/cpu/ctx`:
	default:
		return nil
	}

	if c.AssetID == 0 {
		c.AssetID = m.AssetID
	}
	if c.AssetID != m.AssetID {
		return nil
	}

	if c.CurrTime.IsZero() {
		c.CurrTime = m.TS
		c.CurrValue = m.Value().(int64)
		return nil
	}

	// backwards in time
	if c.CurrTime.After(m.TS) || c.CurrTime.Equal(m.TS) {
		return nil
	}

	c.NextTime = m.TS
	c.NextValue = m.Value().(int64)

	return c.calculate()
}

func (c *CTX) calculate() *legacy.MetricSplit {
	ctx := c.NextValue - c.CurrValue
	delta := c.NextTime.Sub(c.CurrTime).Seconds()

	c.Cps = float64(ctx) / delta
	c.Cps = round(c.Cps, .5, 2)

	c.nextToCurrent()
	return c.emitMetric()
}

func (c *CTX) nextToCurrent() {
	c.CurrValue = c.NextValue
	c.CurrTime = c.NextTime
	c.NextValue = 0
	c.NextTime = time.Time{}
}

func (c *CTX) emitMetric() *legacy.MetricSplit {
	return &legacy.MetricSplit{
		AssetID: c.AssetID,
		Path:    `cpu.ctx.per.second`,
		TS:      c.CurrTime,
		Type:    `real`,
		Unit:    `#`,
		Val: legacy.MetricValue{
			FlpVal: c.Cps,
		},
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
