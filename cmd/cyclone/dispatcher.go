/*-
 * Copyright © 2017 Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/solnx/cyclone/cmd/cyclone"

import (
	"github.com/mjolnir42/erebos"
	"github.com/rcrowley/go-metrics"
)

// wrappedDispatch wraps a metric update around an erebos.Dispatcher
func wrappedDispatch(reg *metrics.Registry, h erebos.Dispatcher) erebos.Dispatcher {
	return func(msg erebos.Transport) error {
		metrics.GetOrRegisterMeter(`.metrics.consumed.per.second`,
			*reg).Mark(1)
		return h(msg)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
