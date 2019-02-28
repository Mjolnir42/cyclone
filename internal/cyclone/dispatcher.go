/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"
import (
	"math/rand"
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/d3luxee/schema"
	"github.com/mjolnir42/erebos"
	m2msg "github.com/raintank/schema/msg"
)

// Dispatch implements erebos.Dispatcher
func Dispatch(msg erebos.Transport) error {
	// decode embedded MetricData
	_, isPointMsg := m2msg.IsPointMsg(msg.Value)
	if isPointMsg {
		// mark as processed
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		return nil
	}
	msg.Metric = schema.MetricData{}
	_, err := msg.Metric.UnmarshalMsg(msg.Value)
	if err != nil {
		logrus.Errorf("Invalid data: %s", err.Error())
		return err
	}
	// ignore metrics that are simply too old for useful
	// alerting
	if time.Now().UTC().Add(AgeCutOff).After(time.Unix(msg.Metric.Time, 0).UTC()) {
		// mark as processed
		logrus.Debugln("Ignore metric due to age")
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		return nil
	}
	hand := (rand.Int() % runtime.NumCPU())
	Handlers[hand].InputChannel() <- &msg
	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
