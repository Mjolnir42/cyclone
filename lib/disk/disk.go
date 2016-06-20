/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package disk

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/mjolnir42/cyclone/lib/metric"
)

type Disk struct {
	AssetId    int64
	Curr       DiskCounter
	Next       DiskCounter
	CurrTime   time.Time
	NextTime   time.Time
	Mountpoint string
	ReadBps    float64
	WriteBps   float64
	Usage      float64
	BytesFree  int64
}

type DiskCounter struct {
	SetBlkTotal bool
	SetBlkUsed  bool
	SetBlkRead  bool
	SetBlkWrite bool
	BlkTotal    int64
	BlkUsed     int64
	BlkRead     int64
	BlkWrite    int64
}

func (d *DiskCounter) valid() bool {
	return d.SetBlkTotal && d.SetBlkUsed && d.SetBlkRead && d.SetBlkWrite
}

func (d *Disk) Update(m *metric.Metric) {
	// ignore metrics for other paths
	switch m.Path {
	case `/sys/disk/blk_total`:
	case `/sys/disk/blk_used`:
	case `/sys/disk/blk_read`:
	case `/sys/disk/blk_wrtn`:
	default:
		return
	}

	if d.AssetId == 0 {
		d.AssetId = m.AssetId
	}
	if d.AssetId != m.AssetId {
		return
	}

	// can not contain required mount information
	if len(m.Tags) == 0 {
		return
	}

	if d.Mountpoint == `` {
		d.Mountpoint = m.Tags[0]
	}
	if d.Mountpoint != m.Tags[0] {
		return
	}

processing:
	if d.NextTime.IsZero() {
		d.NextTime = m.TS
	}

	if d.NextTime.Equal(m.TS) {
		switch m.Path {
		case `/sys/disk/blk_total`:
			d.Next.BlkTotal = m.Value().(int64) * 1024
			d.Next.SetBlkTotal = true
		case `/sys/disk/blk_used`:
			d.Next.BlkUsed = m.Value().(int64) * 1024
			d.Next.SetBlkUsed = true
		case `/sys/disk/blk_read`:
			d.Next.BlkRead = m.Value().(int64) * 512
			d.Next.SetBlkRead = true
		case `/sys/disk/blk_wrtn`:
			d.Next.BlkWrite = m.Value().(int64) * 512
			d.Next.SetBlkWrite = true
		}
		return
	}

	// out of order metric for old timestamp
	if d.NextTime.After(m.TS) {
		return
	}

	// abandon current next and start new one
	if d.NextTime.Before(m.TS) {
		d.NextTime = time.Time{}
		d.Next = DiskCounter{}
		goto processing
	}
}

func (d *Disk) Calculate() []*metric.Metric {
	if d.NextTime.IsZero() {
		return nil
	}
	if !d.Next.valid() {
		return nil
	}

	usage := big.NewRat(0, 1).SetFrac64(d.Next.BlkUsed, d.Next.BlkTotal)
	usage.Mul(usage, big.NewRat(100, 1))
	usage.Sub(big.NewRat(100, 1), usage)
	floatUsage, _ := strconv.ParseFloat(usage.FloatString(2), 64)
	floatUsage = round(floatUsage, .5, 2)

	bytesFree := d.Next.BlkTotal - d.Next.BlkUsed

	// this is the first update
	if d.CurrTime.IsZero() {
		d.Usage = floatUsage
		d.BytesFree = bytesFree
		d.nextToCurrent()
		return nil
	}

	// do not walk backwards in time
	if d.CurrTime.After(d.NextTime) || d.CurrTime.Equal(d.NextTime) {
		return nil
	}

	d.Usage = floatUsage
	d.BytesFree = bytesFree

	delta := d.NextTime.Sub(d.CurrTime).Seconds()

	reads := d.Next.BlkRead - d.Curr.BlkRead
	d.ReadBps = float64(reads) / delta
	d.ReadBps = round(d.ReadBps, .5, 2)

	writes := d.Next.BlkWrite - d.Curr.BlkWrite
	d.WriteBps = float64(writes) / delta
	d.WriteBps = round(d.WriteBps, .5, 2)

	d.nextToCurrent()
	return d.emitMetric()
}

func (d *Disk) nextToCurrent() {
	d.CurrTime = d.NextTime
	d.NextTime = time.Time{}

	d.Curr = d.Next
	d.Next = DiskCounter{}
}

func (d *Disk) emitMetric() []*metric.Metric {
	return []*metric.Metric{
		&metric.Metric{
			AssetId: d.AssetId,
			Path:    fmt.Sprintf("disk.write.per.second:%s", d.Mountpoint),
			TS:      d.CurrTime,
			Type:    `real`,
			Unit:    `B`,
			Val: metric.MetricValue{
				FlpVal: d.WriteBps,
			},
		},
		&metric.Metric{
			AssetId: d.AssetId,
			Path:    fmt.Sprintf("disk.read.per.second:%s", d.Mountpoint),
			TS:      d.CurrTime,
			Type:    `real`,
			Unit:    `B`,
			Val: metric.MetricValue{
				FlpVal: d.ReadBps,
			},
		},
		&metric.Metric{
			AssetId: d.AssetId,
			Path:    fmt.Sprintf("disk.free:%s", d.Mountpoint),
			TS:      d.CurrTime,
			Type:    `integer`,
			Unit:    `B`,
			Val: metric.MetricValue{
				IntVal: d.BytesFree,
			},
		},
		&metric.Metric{
			AssetId: d.AssetId,
			Path:    fmt.Sprintf("disk.usage.percent:%s", d.Mountpoint),
			TS:      d.CurrTime,
			Type:    `real`,
			Unit:    `%`,
			Val: metric.MetricValue{
				FlpVal: d.Usage,
			},
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
