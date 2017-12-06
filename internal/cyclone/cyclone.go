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
	"regexp"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/eyewall"
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
	Num      int
	Input    chan *erebos.Transport
	Shutdown chan struct{}
	Death    chan error
	Config   *erebos.Config
	Metrics  *metrics.Registry
	// unexported
	delay   *delay.Delay
	redis   *redis.Client
	lookup  *eyewall.Lookup
	discard map[string]bool
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

// isUUID validates if a string is one very narrow formatting of a
// UUID. Other valid formats with braces etc are not accepted.
func isUUID(s string) bool {
	const reUUID string = `^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[1-5][[:xdigit:]]{3}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`
	const reUNIL string = `^0{8}-0{4}-0{4}-0{4}-0{12}$`
	re := regexp.MustCompile(fmt.Sprintf("%s|%s", reUUID, reUNIL))

	return re.MatchString(s)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix