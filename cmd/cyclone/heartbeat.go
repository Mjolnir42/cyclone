/*-
 * Copyright © 2017 Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/mjolnir42/cyclone/cmd/cyclone"

import (
	"encoding/json"

	"github.com/mjolnir42/cyclone/lib/cyclone/metric"
	"github.com/mjolnir42/erebos"
)

// newHeartbeat returns a heartbeat message wrapped for processing by a
// cyclone handler
func newHeartbeat() *erebos.Transport {
	msg := &erebos.Transport{}
	msg.Value, _ = json.Marshal(&metric.Metric{Path: `_internal.cyclone.heartbeat`})
	return msg
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
