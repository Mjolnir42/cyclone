/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package mem provides the following derived metrics:
//	- memory.usage.percent
package mem // import "github.com/mjolnir42/cyclone/lib/cyclone/mem"

import (
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/legacy"
)

// Mem implements the metric evaluation and accounting for monitoring
// of memory metrics
type Mem struct {
	AssetID  int64
	Curr     distribution
	Next     distribution
	CurrTime time.Time
	NextTime time.Time
	Usage    float64
}

// Update adds mtr to the next distribution tracked by Mem
func (m *Mem) Update(mtr *legacy.MetricSplit) {
	// ignore metrics for other paths
	switch mtr.Path {
	case `/sys/memory/active`:
	case `/sys/memory/buffers`:
	case `/sys/memory/cached`:
	case `/sys/memory/free`:
	case `/sys/memory/inactive`:
	case `/sys/memory/swapfree`:
	case `/sys/memory/swaptotal`:
	case `/sys/memory/total`:
	default:
		return
	}

	if m.AssetID == 0 {
		m.AssetID = mtr.AssetID
	}
	if m.AssetID != mtr.AssetID {
		return
	}

processing:
	if m.NextTime.IsZero() {
		m.NextTime = mtr.TS
	}

	if m.NextTime.Equal(mtr.TS) {
		switch mtr.Path {
		case `/sys/memory/active`:
			m.Next.Active = mtr.Value().(int64)
			m.Next.SetActive = true
		case `/sys/memory/buffers`:
			m.Next.Buffers = mtr.Value().(int64)
			m.Next.SetBuffers = true
		case `/sys/memory/cached`:
			m.Next.Cached = mtr.Value().(int64)
			m.Next.SetCached = true
		case `/sys/memory/free`:
			m.Next.Free = mtr.Value().(int64)
			m.Next.SetFree = true
		case `/sys/memory/inactive`:
			m.Next.InActive = mtr.Value().(int64)
			m.Next.SetInActive = true
		case `/sys/memory/swapfree`:
			m.Next.SwapFree = mtr.Value().(int64)
			m.Next.SetSwapFree = true
		case `/sys/memory/swaptotal`:
			m.Next.SwapTotal = mtr.Value().(int64)
			m.Next.SetSwapTotal = true
		case `/sys/memory/total`:
			m.Next.Total = mtr.Value().(int64)
			m.Next.SetTotal = true
		}
		return
	}

	// out of order metric for old timestamp
	if m.NextTime.After(mtr.TS) {
		return
	}

	// abandon current next and start new one
	if m.NextTime.Before(mtr.TS) {
		m.NextTime = time.Time{}
		m.Next = distribution{}
		goto processing
	}
}

// Calculate checks if the next distribution has been fully assembled
// and then calculates the memory usage, moves the distribution forward
// and returns the derived metric. If the distribution is not yet
// complete, it returns nil.
func (m *Mem) Calculate() *legacy.MetricSplit {
	if m.NextTime.IsZero() || !m.Next.valid() {
		return nil
	}

	// do not walk backwards in time
	if m.CurrTime.After(m.NextTime) || m.CurrTime.Equal(m.NextTime) {
		return nil
	}

	usage := big.NewRat(0, 1).SetFrac64(m.Next.Free, m.Next.Total)
	usage.Mul(usage, big.NewRat(100, 1))
	usage.Sub(big.NewRat(100, 1), usage)
	m.Usage, _ = strconv.ParseFloat(usage.FloatString(2), 64)
	m.Usage = round(m.Usage, .5, 2)

	m.nextToCurrent()
	return m.emitMetric()
}

// nextToCurrent advances the distributions within Mem by one step
func (m *Mem) nextToCurrent() {
	m.CurrTime = m.NextTime
	m.NextTime = time.Time{}

	m.Curr = m.Next
	m.Next = distribution{}
}

// emitMetric returns a legacy.MetricSplit for metric path
// memory.usage.percent with the m.Usage as value
func (m *Mem) emitMetric() *legacy.MetricSplit {
	return &legacy.MetricSplit{
		AssetID: m.AssetID,
		Path:    `memory.usage.percent`,
		TS:      m.CurrTime,
		Type:    `real`,
		Unit:    `%`,
		Val: legacy.MetricValue{
			FlpVal: m.Usage,
		},
	}
}

// distribution is used to track multiple memory metrics from the same
// measurement cycle
type distribution struct {
	SetTotal     bool
	SetActive    bool
	SetBuffers   bool
	SetCached    bool
	SetFree      bool
	SetInActive  bool
	SetSwapFree  bool
	SetSwapTotal bool
	Total        int64
	Active       int64
	Buffers      int64
	Cached       int64
	Free         int64
	InActive     int64
	SwapFree     int64
	SwapTotal    int64
}

// valid checks if a distribution has been fully populated
func (m *distribution) valid() bool {
	return m.SetTotal && m.SetActive && m.SetBuffers && m.SetCached &&
		m.SetFree && m.SetInActive && m.SetSwapFree && m.SetSwapTotal
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
