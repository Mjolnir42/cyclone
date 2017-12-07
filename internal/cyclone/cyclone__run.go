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

	"github.com/Sirupsen/logrus"
)

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
		case res := <-c.result:
			if res.err != nil {
				// alarm sending failed due to internal error
				if res.internal {
					c.Death <- res.err
					<-c.Shutdown
					break runloop
				}
				// alarm sending failed from external error
				c.resendAlarm(res.alarm, res.trackingID)
				continue runloop
			}
			c.updateOffset(res.trackingID)
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
			if err := c.process(msg); err != nil {
				logrus.Errorln(err.Error())
			}
		}
	}
	// drain result channel from http goroutines in extra
	// loop since the result channel will not be closed
	// by main
resultdrain:
	for {
		select {
		case res := <-c.result:
			if res.err != nil {
				logrus.Errorln(res.err.Error())
				continue resultdrain
			}
			c.updateOffset(res.trackingID)
		// allow for http timeouts to occur
		case <-time.After(2 * time.Second):
			// TODO: use configurable http timeout
			break resultdrain
		}
	}

	c.delay.Wait()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
