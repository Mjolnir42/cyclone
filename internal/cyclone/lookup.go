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
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
)

// updateEval updates the timestamp of the last evaluation of id inside
// the local cache
func (c *Cyclone) updateEval(id string) {
	c.redis.HSet(`evaluation`, id, time.Now().UTC().Format(time.RFC3339))
}

// heartbeat updates the heartbeat record inside the local cache
func (c *Cyclone) heartbeat() {
	logrus.Debugf("Cyclone[%d], Updating cyclone heartbeat", c.Num)
	if _, err := c.redis.HSet(
		`heartbeat`,
		`cyclone-alive`,
		time.Now().UTC().Format(time.RFC3339),
	).Result(); err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR setting heartbeat in redis: %s",
			c.Num, err,
		)
	}
	if _, err := c.redis.HSet(
		`heartbeat`,
		fmt.Sprintf("cyclone-alive-%d", c.Num),
		time.Now().UTC().Format(time.RFC3339),
	).Result(); err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR setting heartbeat in redis: %s",
			c.Num, err,
		)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
