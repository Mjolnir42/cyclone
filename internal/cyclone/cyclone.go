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
	"time"

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

// commit marks a message as fully processed
func (c *Cyclone) commit(msg *erebos.Transport) {
	msg.Commit <- &erebos.Commit{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
