/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/go-resty/resty"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/limit"
	metrics "github.com/rcrowley/go-metrics"
	wall "github.com/solnx/eye/lib/eye.wall"
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
	Limit    *limit.Limit
	AppLog   *logrus.Logger
	// unexported
	delay    *delay.Delay
	lookup   *wall.Lookup
	client   *resty.Client
	discard  map[string]bool
	result   chan *alarmResult
	trackID  map[string]int
	trackACK map[string]*erebos.Transport
}

// updateOffset updates the consumer offsets in Kafka once all
// outstanding messages for trackingID have been processed
func (c *Cyclone) updateOffset(trackingID string) {
	if _, ok := c.trackID[trackingID]; !ok {
		c.AppLog.Warnf("Unknown trackingID: %s", trackingID)
		return
	}
	// decrement outstanding successes for trackingID
	c.trackID[trackingID]--
	// check if trackingID has been fully processed
	if c.trackID[trackingID] == 0 {
		// commit processed offset to Zookeeper
		c.commit(c.trackACK[trackingID])
		// cleanup offset tracking
		delete(c.trackID, trackingID)
		delete(c.trackACK, trackingID)
	}
}

// commit marks a message as fully processed
func (c *Cyclone) commit(msg *erebos.Transport) {
	c.delay.Use()
	go func() {
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		c.delay.Done()
	}()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
