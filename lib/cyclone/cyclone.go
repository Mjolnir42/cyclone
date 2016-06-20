/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
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
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/mjolnir42/cyclone/lib/cpu"
	"github.com/mjolnir42/cyclone/lib/disk"
	"github.com/mjolnir42/cyclone/lib/mem"
	"github.com/mjolnir42/cyclone/lib/metric"
	"gopkg.in/redis.v3"
)

type Cyclone struct {
	Num                 int
	CpuData             map[int64]cpu.Cpu
	MemData             map[int64]mem.Mem
	CtxData             map[int64]cpu.Ctx
	DskData             map[int64]map[string]disk.Disk
	Input               chan *metric.Metric
	Redis               *redis.Client
	CfgRedisConnect     string
	CfgRedisPassword    string
	CfgRedisDB          int64
	CfgAlarmDestination string
	CfgLookupHost       string
	CfgLookupPort       string
	CfgLookupPath       string
	internalInput       chan *metric.Metric
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
	cl.DskData = make(map[int64]map[string]disk.Disk)
	cl.internalInput = make(chan *metric.Metric, 32)
	cl.Redis = redis.NewClient(&redis.Options{
		Addr:     cl.CfgRedisConnect,
		Password: cl.CfgRedisPassword,
		DB:       cl.CfgRedisDB,
	})
	defer cl.Redis.Close()
	if _, err := cl.Redis.Ping().Result(); err != nil {
		log.Fatalln(err)
	}

	log.Printf("Cyclone[%d], Handler ready for input\n", cl.Num)

	for {
		select {
		case m := <-cl.internalInput:
			log.Printf(
				"Cyclone[%d], Received metric %s from %d\n",
				cl.Num,
				m.Path,
				m.AssetId,
			)
			cl.eval(m)
		case m := <-cl.Input:
			log.Printf(
				"Cyclone[%d], Received metric %s from %d\n",
				cl.Num,
				m.Path,
				m.AssetId,
			)
			cl.eval(m)
		}
	}
}

func (cl *Cyclone) eval(m *metric.Metric) {
	// Processing
	switch m.Path {
	case `_internal.cyclone.heartbeat`:
		cl.heartbeat()
		return
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

	case `/sys/disk/blk_total`:
		fallthrough
	case `/sys/disk/blk_used`:
		fallthrough
	case `/sys/disk/blk_read`:
		fallthrough
	case `/sys/disk/blk_wrtn`:
		if len(m.Tags) == 0 {
			m = nil
			break
		}
		d := disk.Disk{}
		id := m.AssetId
		mpt := m.Tags[0]
		if cl.DskData[id] == nil {
			cl.DskData[id] = make(map[string]disk.Disk)
		}
		if _, ok := cl.DskData[id][mpt]; !ok {
			cl.DskData[id][mpt] = d
		}
		if _, ok := cl.DskData[id][mpt]; ok {
			d = cl.DskData[id][mpt]
		}
		d.Update(m)
		mArr := d.Calculate()
		if mArr != nil {
			for _, mPtr := range mArr {
				// no deadlock, channel is buffered
				cl.internalInput <- mPtr
			}
		}
		cl.DskData[id][mpt] = d
		m = nil
	}
	if m == nil {
		fmt.Printf("Cyclone[%d], Metric has been consumed", cl.Num)
		return
	}
	lid := m.LookupID()
	thr := cl.Lookup(lid)
	if thr == nil {
		log.Printf("Cyclone[%d], ERROR fetching threshold data. Lookup service available?", cl.Num)
		return
	}
	if len(thr) == 0 {
		log.Printf("Cyclone[%d], No thresholds configured for %s from %d\n", cl.Num, m.Path, m.AssetId)
		return
	}
	log.Printf("Cyclone[%d], Forwarding %s from %d for evaluation (%s)\n", cl.Num, m.Path, m.AssetId, lid)

	internalMetric := false
	switch m.Path {
	case `cpu.usage.percent`:
		internalMetric = true
	case `cpu.ctx.per.second`:
		internalMetric = true
	case `memory.usage.percent`:
		internalMetric = true
	}

	evaluations := 0

thrloop:
	for key, _ := range thr {
		var alarmLevel string = "0"
		var brokenThr int64 = 0
		dispatchAlarm := false
		broken := false
		fVal := ``
		if internalMetric {
			dispatchAlarm = true
		}
		if len(m.Tags) > 0 && m.Tags[0] == thr[key].Id {
			dispatchAlarm = true
		}
		if !dispatchAlarm {
			continue thrloop
		}
		log.Printf("Cyclone[%d], Evaluating metric %s from %d against config %s",
			cl.Num, m.Path, m.AssetId, thr[key].Id)
		evaluations++

	lvlloop:
		for _, lvl := range []string{`9`, `8`, `7`, `6`, `5`, `4`, `3`, `2`, `1`, `0`} {
			thrval, ok := thr[key].Thresholds[lvl]
			if !ok {
				continue
			}
			log.Printf("Cyclone[%d], Checking %s alarmlevel %s\n", cl.Num, thr[key].Id, lvl)
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
			Source:     fmt.Sprintf("%s / %s", thr[key].MetaTargethost, thr[key].MetaSource),
			EventId:    thr[key].Id,
			Version:    `1.0`,
			Sourcehost: thr[key].MetaTargethost,
			Oncall:     thr[key].Oncall,
			Targethost: thr[key].MetaTargethost,
			Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
			Check:      fmt.Sprintf("cyclone(%s)", m.Path),
			Monitoring: thr[key].MetaMonitoring,
			Team:       thr[key].MetaTeam,
		}
		al.Level, _ = strconv.ParseInt(alarmLevel, 10, 64)
		if alarmLevel == `0` {
			al.Message = `Ok.`
		} else {
			al.Message = fmt.Sprintf(
				"Metric %s has broken threshold. Value %s %s %d",
				m.Path,
				fVal,
				thr[key].Predicate,
				brokenThr,
			)
		}
		go func(a AlarmEvent) {
			b := new(bytes.Buffer)
			aSlice := []AlarmEvent{a}
			if err := json.NewEncoder(b).Encode(aSlice); err != nil {
				log.Printf("Cyclone[%d], ERROR json encoding alarm for %s: %s", a.EventId, err)
				return
			}
			resp, err := http.Post(
				cl.CfgAlarmDestination,
				`application/json; charset=utf-8`,
				b,
			)
			if err != nil {
				log.Printf("Cyclone[%d], ERROR sending alarm for %s: %s", a.EventId, err)
				return
			}
			log.Printf("Cyclone[%d], Dispatched alarm for %s at level %d, returncode was %d",
				cl.Num, a.EventId, a.Level, resp.StatusCode)
		}(al)
		cl.updateEval(thr[key].Id)
	}
	if evaluations == 0 {
		log.Printf("Cyclone[%d], metric %s(%d) matched no configurations\n", cl.Num, m.Path, m.AssetId)
	}
}

func CmpInt(pred string, value, threshold int64) (bool, string) {
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
		log.Printf("Cyclone[], ERROR unknown predicate: %s", pred)
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
		log.Printf("Cyclone[], ERROR unknown predicate: %s", pred)
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
