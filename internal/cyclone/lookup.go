/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/internal/cyclone"

import (
	"time"
)

// updateEval updates the timestamp of the last evaluation of id inside
// the local cache
func (c *Cyclone) updateEval(id string) {
	c.redis.HSet(`evaluation`, id, time.Now().UTC().Format(time.RFC3339))
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
