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

	"github.com/mjolnir42/erebos"
)

// Dispatch implements erebos.Dispatcher
func Dispatch(msg erebos.Transport) error {
	hand := (rand.Int() % runtime.NumCPU())
	Handlers[hand].InputChannel() <- &msg
	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
