/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/internal/cyclone"

// run is the event loop for Cyclone
func (c *Cyclone) run() {

runloop:
	for {
		select {
		case <-c.Shutdown:
			// received shutdown, drain input channel which will be
			// closed by main
			goto drainloop
		case msg := <-c.Input:
			if msg == nil {
				// this can happen if we read the closed Input channel
				// before the closed Shutdown channel
				continue runloop
			}
			if err := c.process(msg); err != nil {
				c.Death <- err
				<-c.Shutdown
				break runloop
			}
		}
	}

drainloop:
	for {
		select {
		case msg := <-c.Input:
			if msg == nil {
				// channel is closed
				break drainloop
			}
			c.process(msg)
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
