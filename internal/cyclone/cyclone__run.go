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
				c.AppLog.Debugln("Message received via erebos input channel was nil")
				// this can happen if we read the closed Input channel
				// before the closed Shutdown channel
				continue runloop
			}
			if err := c.process(msg); err != nil {
				// process only fails from invalid data or if the profile
				// lookup service is unavailable.
			eyeloop:
				for {
					select {
					// Wait for service to reappear, then try again.
					case <-c.lookup.WaitEye():
						if e := c.process(msg); e != nil {
							// problem is invalid data after all
							c.Death <- e
							<-c.Shutdown
							break runloop
						}
						break eyeloop
					// still handle shutdown requests
					case <-c.Shutdown:
						goto drainloop
					// still handle finished requests
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
							continue eyeloop
						}
						c.updateOffset(res.trackingID)
					}
				}
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
				c.AppLog.Errorln(err.Error())
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
				c.AppLog.Errorln(res.err.Error())
				continue resultdrain
			}
			c.updateOffset(res.trackingID)
		// allow for http timeouts to occur
		case <-time.After(300 +
			time.Duration(c.Config.Cyclone.RequestTimeout)*
				time.Millisecond):
			break resultdrain
		}
	}
	//	c.lookup.Close()
	c.delay.Wait()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
