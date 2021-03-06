/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package cpu provides the following derived metrics:
//	- cpu.ctx.per.second
//	- cpu.usage.percent
package cpu // import "github.com/mjolnir42/cyclone/lib/cyclone/cpu"

import (
	"math"
	"time"

	"github.com/mjolnir42/legacy"
)

// CPU implements the logic to compute derived cpu usage metrics
type CPU struct {
	AssetID  int64
	Curr     counter
	Next     counter
	CurrTime time.Time
	NextTime time.Time
	Idle     int64
	NonIdle  int64
	Total    int64
	Usage    float64
}

// Update adds m to the next counter tracked by c
func (c *CPU) Update(m *legacy.MetricSplit) {
	// ignore metrics for other paths
	switch m.Path {
	case `/sys/cpu/count/idle`:
	case `/sys/cpu/count/iowait`:
	case `/sys/cpu/count/irq`:
	case `/sys/cpu/count/nice`:
	case `/sys/cpu/count/softirq`:
	case `/sys/cpu/count/system`:
	case `/sys/cpu/count/user`:
	default:
		return
	}

	if c.AssetID == 0 {
		c.AssetID = m.AssetID
	}
	if c.AssetID != m.AssetID {
		return
	}

	// only process metrics tagged as cpu, not cpuN
	cpuMetric := false
	for _, t := range m.Tags {
		if t == `cpu` {
			cpuMetric = true
			break
		}
	}
	if !cpuMetric {
		return
	}

processing:
	if c.NextTime.IsZero() {
		c.NextTime = m.TS
	}

	if c.NextTime.Equal(m.TS) {
		switch m.Path {
		case `/sys/cpu/count/idle`:
			c.Next.Idle = m.Value().(int64)
			c.Next.SetIdle = true
		case `/sys/cpu/count/iowait`:
			c.Next.IoWait = m.Value().(int64)
			c.Next.SetIoWait = true
		case `/sys/cpu/count/irq`:
			c.Next.Irq = m.Value().(int64)
			c.Next.SetIrq = true
		case `/sys/cpu/count/nice`:
			c.Next.Nice = m.Value().(int64)
			c.Next.SetNice = true
		case `/sys/cpu/count/softirq`:
			c.Next.SoftIrq = m.Value().(int64)
			c.Next.SetSoftIrq = true
		case `/sys/cpu/count/system`:
			c.Next.System = m.Value().(int64)
			c.Next.SetSystem = true
		case `/sys/cpu/count/user`:
			c.Next.User = m.Value().(int64)
			c.Next.SetUser = true
		}
		return
	}

	// out of order metric for old timestamp
	if c.NextTime.After(m.TS) {
		return
	}

	// abandon current next and start new one
	if c.NextTime.Before(m.TS) {
		c.NextTime = time.Time{}
		c.Next = counter{}
		goto processing
	}
}

// Calculate checks if the next counter has been fully assembled and
// then calculates the derived metrics, moves the counters forward and
// returns he derived metrics. If the next counter is not yet complete,
// it returns nil.
func (c *CPU) Calculate() *legacy.MetricSplit {
	if c.NextTime.IsZero() {
		return nil
	}
	if !c.Next.valid() {
		return nil
	}

	nextIdle := c.Next.Idle + c.Next.IoWait
	nextNonIdle := c.Next.User + c.Next.Nice + c.Next.System + c.Next.Irq + c.Next.SoftIrq

	// this is the first update
	if c.CurrTime.IsZero() {
		c.Idle = nextIdle
		c.NonIdle = nextNonIdle
		c.Total = nextIdle + nextNonIdle

		c.nextToCurrent()
		return nil
	}

	// do not walk backwards in time
	if c.CurrTime.After(c.NextTime) || c.CurrTime.Equal(c.NextTime) {
		return nil
	}

	totalDifference := (nextIdle + nextNonIdle) - c.Total
	idleDifference := nextIdle - c.Idle
	c.Usage = float64((totalDifference - idleDifference)) / float64(totalDifference)
	c.Usage = round(c.Usage, .5, 4) * 100

	c.Idle = nextIdle
	c.NonIdle = nextNonIdle
	c.Total = nextIdle + nextNonIdle

	c.nextToCurrent()
	return c.emitMetric()
}

// nextToCurrent advances the counters within c by one step
func (c *CPU) nextToCurrent() {
	c.CurrTime = c.NextTime
	c.NextTime = time.Time{}

	c.Curr = c.Next
	c.Next = counter{}
}

// emitMetric returns the derived metrics for the current counter
func (c *CPU) emitMetric() *legacy.MetricSplit {
	return &legacy.MetricSplit{
		AssetID: c.AssetID,
		Path:    `cpu.usage.percent`,
		TS:      c.CurrTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: c.Usage,
		},
	}
}

// counter is used to track multiple cpu metrics from the same
// measurement cycle
type counter struct {
	SetIdle    bool
	SetIoWait  bool
	SetIrq     bool
	SetNice    bool
	SetSoftIrq bool
	SetSystem  bool
	SetUser    bool
	Idle       int64
	IoWait     int64
	Irq        int64
	Nice       int64
	SoftIrq    int64
	System     int64
	User       int64
}

// valid checks if a counter has been fully populated
func (c *counter) valid() bool {
	return c.SetIdle && c.SetIoWait && c.SetIrq && c.SetNice &&
		c.SetSoftIrq && c.SetSystem && c.SetUser
}

// https://gist.github.com/DavidVaini/10308388
func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
