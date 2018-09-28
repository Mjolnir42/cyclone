/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"
import (
	"encoding/json"
	"runtime"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
)

// Dispatch implements erebos.Dispatcher
func Dispatch(msg erebos.Transport) error {
	// decode embedded legacy.MetricSplit
	m := &legacy.MetricSplit{}
	if err := json.Unmarshal(msg.Value, m); err != nil {
		return err
	}
	msg.HostID = int(m.AssetID)

	// ignore metrics that are simply too old for useful
	// alerting
	if time.Now().UTC().Add(AgeCutOff).After(m.TS.UTC()) {
		// mark as processed
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		return nil
	}

	Handlers[msg.HostID%runtime.NumCPU()].InputChannel() <- &msg
	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
