/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package mem // import "github.com/mjolnir42/cyclone/lib/cyclone/mem"

import (
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/legacy"
)

type Mem struct {
	AssetID  int64
	Curr     Distribution
	Next     Distribution
	CurrTime time.Time
	NextTime time.Time
	Usage    float64
}

type Distribution struct {
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

func (m *Distribution) valid() bool {
	return m.SetTotal && m.SetActive && m.SetBuffers && m.SetCached &&
		m.SetFree && m.SetInActive && m.SetSwapFree && m.SetSwapTotal
}

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
		m.Next = Distribution{}
		goto processing
	}
}

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

func (m *Mem) nextToCurrent() {
	m.CurrTime = m.NextTime
	m.NextTime = time.Time{}

	m.Curr = m.Next
	m.Next = Distribution{}
}

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
