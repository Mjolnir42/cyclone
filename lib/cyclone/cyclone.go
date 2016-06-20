/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mjolnir42/cyclone/lib/cpu"
	"github.com/mjolnir42/cyclone/lib/mem"
	"github.com/mjolnir42/cyclone/lib/metric"
	"gopkg.in/redis.v3"
)

type Cyclone struct {
	CpuData             map[int64]cpu.Cpu
	MemData             map[int64]mem.Mem
	CtxData             map[int64]cpu.Ctx
	Input               chan *metric.Metric
	Redis               *redis.Client
	CfgRedisConnect     string
	CfgRedisPassword    string
	CfgRedisDB          int64
	CfgAlarmDestination string
	CfgLookupHost       string
	CfgLookupPort       string
	CfgLookupPath       string
}

type AlarmEvent struct {
	Source     string `json:"source"`
	EventId    string `json:"event_id"`
	Version    string `json:"version"`
	Sourcehost string `json:"sourcehost"`
	Oncall     string `json:"oncall"`
	Targethost string `json:"targethost"`
	Message    string `json:"message"`
	Level      int64  `json:"level"`
	Timestamp  string `json:"timestamp"`
	Check      string `json:"check"`
	Monitoring string `json:"monitoring"`
	Team       string `json:"team"`
}

func (cl *Cyclone) Run() {
	cl.CpuData = make(map[int64]cpu.Cpu)
	cl.MemData = make(map[int64]mem.Mem)
	cl.CtxData = make(map[int64]cpu.Ctx)
	cl.Input = make(chan *metric.Metric)
	cl.Redis = redis.NewClient(&redis.Options{
		Addr:     cl.CfgRedisConnect,
		Password: cl.CfgRedisPassword,
		DB:       cl.CfgRedisDB,
	})

	for {
		select {
		case m := <-cl.Input:
			cl.eval(m)
		}
	}
}

func (cl *Cyclone) eval(m *metric.Metric) {
	// Processing
	switch m.Path {
	case `/sys/cpu/ctx`:
		ctx := cpu.Ctx{}
		id := m.AssetId
		if _, ok := cl.CtxData[id]; ok {
			ctx = cl.CtxData[id]
		}
		m = ctx.Update(m)
		cl.CtxData[id] = ctx

	case `/sys/cpu/count/idle`:
		fallthrough
	case `/sys/cpu/count/iowait`:
		fallthrough
	case `/sys/cpu/count/irq`:
		fallthrough
	case `/sys/cpu/count/nice`:
		fallthrough
	case `/sys/cpu/count/softirq`:
		fallthrough
	case `/sys/cpu/count/system`:
		fallthrough
	case `/sys/cpu/count/user`:
		cu := cpu.Cpu{}
		id := m.AssetId
		if _, ok := cl.CpuData[id]; ok {
			cu = cl.CpuData[id]
		}
		cu.Update(m)
		m = cu.Calculate()
		cl.CpuData[id] = cu

	case `/sys/memory/active`:
		fallthrough
	case `/sys/memory/buffers`:
		fallthrough
	case `/sys/memory/cached`:
		fallthrough
	case `/sys/memory/free`:
		fallthrough
	case `/sys/memory/inactive`:
		fallthrough
	case `/sys/memory/swapfree`:
		fallthrough
	case `/sys/memory/swaptotal`:
		fallthrough
	case `/sys/memory/total`:
		mm := mem.Mem{}
		id := m.AssetId
		if _, ok := cl.MemData[id]; ok {
			mm = cl.MemData[id]
		}
		mm.Update(m)
		m = mm.Calculate()
		cl.MemData[id] = mm
	}
	if m == nil {
		return
	}
	thr := cl.Lookup(m.LookupID())
	if thr == nil {
		return
	}

	internalMetric := false
	switch m.Path {
	case `cpu.usage.percent`:
		internalMetric = true
	case `cpu.ctx.per.second`:
		internalMetric = true
	case `memory.usage.percent`:
		internalMetric = true
	}

thrloop:
	for key, _ := range thr {
		var alarmLevel uint16 = 0
		var brokenThr int64 = 0
		dispatchAlarm := false
		broken := false
		fVal := ``
		if internalMetric {
			dispatchAlarm = false
		}
		if len(m.Tags) > 0 && m.Tags[0] == thr[key].Id {
			dispatchAlarm = false
		}
		if !dispatchAlarm {
			continue thrloop
		}
	lvlloop:
		for _, lvl := range []uint16{9, 8, 7, 6, 5, 4, 3, 2, 1, 0} {
			thrval, ok := thr[key].Thresholds[lvl]
			if !ok {
				continue
			}
			switch m.Type {
			case `integer`:
				fallthrough
			case `long`:
				broken, fVal = CmpInt(thr[key].Predicate,
					m.Value().(int64),
					thrval)
			case `real`:
				broken, fVal = CmpFlp(thr[key].Predicate,
					m.Value().(float64),
					thrval)
			}
			if broken {
				alarmLevel = lvl
				brokenThr = thrval
				break lvlloop
			}
		}
		al := AlarmEvent{
			Source:     thr[key].MetaSource,
			EventId:    thr[key].Id,
			Version:    `1.0`,
			Sourcehost: thr[key].MetaTargethost,
			Oncall:     thr[key].Oncall,
			Targethost: thr[key].MetaTargethost,
			Level:      int64(alarmLevel),
			Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
			Check:      fmt.Sprintf("cyclone(%s)", m.Path),
			Monitoring: thr[key].MetaMonitoring,
			Team:       thr[key].MetaTeam,
		}
		if alarmLevel == 0 {
			al.Message = `Ok.`
		} else {
			al.Message = fmt.Sprintf(
				"Metric %s threshold broken. Value %s %s %d",
				m.Path,
				fVal,
				thr[key].Predicate,
				brokenThr,
			)
		}
		go func(a AlarmEvent) {
			b := new(bytes.Buffer)
			json.NewEncoder(b).Encode(a)
			http.Post(
				cl.CfgAlarmDestination,
				`application/json; charset=utf-8`,
				b,
			)
		}(al)
		cl.updateEval(thr[key].Id)
	}
}

func CmpInt(pred string, value, threshold int64) (bool, string) {
	fVal := fmt.Sprintf("%.3d", value)
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
		return false, ``
	}
}

func CmpFlp(pred string, value float64, threshold int64) (bool, string) {
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
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
