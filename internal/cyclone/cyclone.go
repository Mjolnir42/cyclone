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
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/cyclone/internal/cyclone/cpu"
	"github.com/mjolnir42/cyclone/internal/cyclone/disk"
	"github.com/mjolnir42/cyclone/internal/cyclone/mem"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
	"gopkg.in/redis.v3"
)

// Handlers is the registry of running application handlers
var Handlers map[int]erebos.Handler

// AgeCutOff is the duration after which back-processed alarms are
// ignored and not alerted
var AgeCutOff time.Duration

func init() {
	Handlers = make(map[int]erebos.Handler)
}

// Cyclone performs threshold evaluation alarming on metrics
type Cyclone struct {
	Num           int
	Input         chan *erebos.Transport
	Shutdown      chan struct{}
	Death         chan error
	Config        *erebos.Config
	Metrics       *metrics.Registry
	CPUData       map[int64]cpu.CPU
	MemData       map[int64]mem.Mem
	CTXData       map[int64]cpu.CTX
	DskData       map[int64]map[string]disk.Disk
	redis         *redis.Client
	internalInput chan *legacy.MetricSplit
}

// AlarmEvent is the datatype for sending out alarm notifications
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

// commit marks a message as fully processed
func (c *Cyclone) commit(msg *erebos.Transport) {
	msg.Commit <- &erebos.Commit{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
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
		logrus.Errorf("Cyclone[%d], ERROR unknown predicate: %s", c.Num, pred)
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
		logrus.Errorf("Cyclone[%d], ERROR unknown predicate: %s", c.Num, pred)
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
