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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/cyclone/lib/cpu"
	"github.com/mjolnir42/cyclone/lib/disk"
	"github.com/mjolnir42/cyclone/lib/mem"
	"github.com/mjolnir42/cyclone/lib/metric"
	metrics "github.com/rcrowley/go-metrics"
	"gopkg.in/redis.v3"
)

type Cyclone struct {
	Num                 int
	CPUData             map[int64]cpu.CPU
	MemData             map[int64]mem.Mem
	CTXData             map[int64]cpu.CTX
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
	CfgAPIVersion       string
	TestMode            bool
	internalInput       chan *metric.Metric
	logger              *logrus.Logger
	Metrics             *metrics.Registry
}

type AlarmEvent struct {
	Source     string `json:"source"`
	EventID    string `json:"event_id"`
	Version    string `json:"version"`
	Sourcehost string `json:"sourcehost"`
	Oncall     string `json:"on_call"`
	Targethost string `json:"targethost"`
	Message    string `json:"message"`
	Level      int64  `json:"level"`
	Timestamp  string `json:"timestamp"`
	Check      string `json:"check"`
	Monitoring string `json:"monitoring"`
	Team       string `json:"team"`
}

func (cl *Cyclone) SetLog(l *logrus.Logger) {
	cl.logger = l
}

func (cl *Cyclone) Run() {
	cl.CPUData = make(map[int64]cpu.CPU)
	cl.MemData = make(map[int64]mem.Mem)
	cl.CTXData = make(map[int64]cpu.CTX)
	cl.DskData = make(map[int64]map[string]disk.Disk)
	cl.internalInput = make(chan *metric.Metric, 32)
	cl.Redis = redis.NewClient(&redis.Options{
		Addr:     cl.CfgRedisConnect,
		Password: cl.CfgRedisPassword,
		DB:       cl.CfgRedisDB,
	})
	defer cl.Redis.Close()
	if _, err := cl.Redis.Ping().Result(); err != nil {
		cl.logger.Fatalln(err)
	}

	cl.logger.Printf("Cyclone[%d], Handler ready for input", cl.Num)

	for {
		select {
		case m := <-cl.internalInput:
			cl.logger.Printf(
				"Cyclone[%d], Received metric %s from %d",
				cl.Num,
				m.Path,
				m.AssetID,
			)
			cl.eval(m)
		case m := <-cl.Input:
			cl.logger.Printf(
				"Cyclone[%d], Received metric %s from %d",
				cl.Num,
				m.Path,
				m.AssetID,
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
		ctx := cpu.CTX{}
		id := m.AssetID
		if _, ok := cl.CTXData[id]; ok {
			ctx = cl.CTXData[id]
		}
		m = ctx.Update(m)
		cl.CTXData[id] = ctx

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
		cu := cpu.CPU{}
		id := m.AssetID
		if _, ok := cl.CPUData[id]; ok {
			cu = cl.CPUData[id]
		}
		cu.Update(m)
		m = cu.Calculate()
		cl.CPUData[id] = cu

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
		id := m.AssetID
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
		id := m.AssetID
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
		cl.logger.Printf("Cyclone[%d], Metric has been consumed\n", cl.Num)
		return
	}
	lid := m.LookupID()
	thr := cl.Lookup(lid)
	if thr == nil {
		cl.logger.Printf("Cyclone[%d], ERROR fetching threshold data. Lookup service available?", cl.Num)
		return
	}
	if len(thr) == 0 {
		cl.logger.Printf("Cyclone[%d], No thresholds configured for %s from %d", cl.Num, m.Path, m.AssetID)
		return
	}
	cl.logger.Printf("Cyclone[%d], Forwarding %s from %d for evaluation (%s)", cl.Num, m.Path, m.AssetID, lid)
	evals := metrics.GetOrRegisterMeter(`/evaluations`, *cl.Metrics)
	evals.Mark(1)

	internalMetric := false
	switch m.Path {
	case
		// internal metrics generated by cyclone
		`cpu.ctx.per.second`,
		`cpu.usage.percent`,
		`memory.usage.percent`:
		internalMetric = true
	case
		// internal metrics sent by main daemon
		`/sys/cpu/blocked`,
		`/sys/cpu/uptime`,
		`/sys/load/300s`,
		`/sys/load/60s`,
		`/sys/load/900s`,
		`/sys/load/running_proc`,
		`/sys/load/total_proc`:
		internalMetric = true
	default:
		switch {
		case
			strings.HasPrefix(m.Path, `disk.free:`),
			strings.HasPrefix(m.Path, `disk.read.per.second:`),
			strings.HasPrefix(m.Path, `disk.usage.percent:`),
			strings.HasPrefix(m.Path, `disk.write.per.second:`):
			internalMetric = true
		}
	}

	evaluations := 0

thrloop:
	for key := range thr {
		var alarmLevel = "0"
		var brokenThr int64
		dispatchAlarm := false
		broken := false
		fVal := ``
		if internalMetric {
			dispatchAlarm = true
		}
		if len(m.Tags) > 0 && m.Tags[0] == thr[key].ID {
			dispatchAlarm = true
		}
		if !dispatchAlarm {
			continue thrloop
		}
		cl.logger.Printf("Cyclone[%d], Evaluating metric %s from %d against config %s",
			cl.Num, m.Path, m.AssetID, thr[key].ID)
		evaluations++

	lvlloop:
		for _, lvl := range []string{`9`, `8`, `7`, `6`, `5`, `4`, `3`, `2`, `1`, `0`} {
			thrval, ok := thr[key].Thresholds[lvl]
			if !ok {
				continue
			}
			cl.logger.Printf("Cyclone[%d], Checking %s alarmlevel %s", cl.Num, thr[key].ID, lvl)
			switch m.Type {
			case `integer`:
				fallthrough
			case `long`:
				broken, fVal = cl.CmpInt(thr[key].Predicate,
					m.Value().(int64),
					thrval)
			case `real`:
				broken, fVal = cl.CmpFlp(thr[key].Predicate,
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
			EventID:    thr[key].ID,
			Version:    cl.CfgAPIVersion,
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
		if al.Oncall == `` {
			al.Oncall = `No oncall information available`
		}
		cl.updateEval(thr[key].ID)
		if cl.TestMode {
			// do not send out alarms in testmode
			continue thrloop
		}
		alrms := metrics.GetOrRegisterMeter(`/alarms`, *cl.Metrics)
		alrms.Mark(1)
		go func(a AlarmEvent) {
			b := new(bytes.Buffer)
			aSlice := []AlarmEvent{a}
			if err := json.NewEncoder(b).Encode(aSlice); err != nil {
				cl.logger.Printf("Cyclone[%d], ERROR json encoding alarm for %s: %s", cl.Num, a.EventID, err)
				return
			}
			resp, err := http.Post(
				cl.CfgAlarmDestination,
				`application/json; charset=utf-8`,
				b,
			)

			if err != nil {
				cl.logger.Printf("Cyclone[%d], ERROR sending alarm for %s: %s", cl.Num, a.EventID, err)
				return
			}
			cl.logger.Printf("Cyclone[%d], Dispatched alarm for %s at level %d, returncode was %d",
				cl.Num, a.EventID, a.Level, resp.StatusCode)
			if resp.StatusCode >= 209 {
				// read response body
				bt, _ := ioutil.ReadAll(resp.Body)
				cl.logger.Printf("Cyclone[%d], ResponseMsg(%d): %s", cl.Num, resp.StatusCode, string(bt))
				resp.Body.Close()

				// reset buffer and encode JSON again so it can be
				// logged
				b.Reset()
				json.NewEncoder(b).Encode(aSlice)
				cl.logger.Printf("Cyclone[%d], RequestJSON: %s", cl.Num, b.String())
				return
			}
			// ensure http.Response.Body is consumed and closed,
			// otherwise it leaks filehandles
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}(al)
	}
	if evaluations == 0 {
		cl.logger.Printf("Cyclone[%d], metric %s(%d) matched no configurations", cl.Num, m.Path, m.AssetID)
	}
}

func (cl *Cyclone) CmpInt(pred string, value, threshold int64) (bool, string) {
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
		cl.logger.Printf("Cyclone[], ERROR unknown predicate: %s", pred)
		return false, ``
	}
}

func (cl *Cyclone) CmpFlp(pred string, value float64, threshold int64) (bool, string) {
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
		cl.logger.Printf("Cyclone[], ERROR unknown predicate: %s", pred)
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
