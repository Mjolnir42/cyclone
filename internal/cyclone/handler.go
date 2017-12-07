/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/internal/cyclone"

import (
	"fmt"

	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/eyewall"
	"gopkg.in/redis.v3"
)

// Implementation of the erebos.Handler interface

//Start sets up the Cyclone application
func (c *Cyclone) Start() {
	if len(Handlers) == 0 {
		c.Death <- fmt.Errorf(`Incorrectly set handlers`)
		<-c.Shutdown
		return
	}

	c.trackID = make(map[string]int)
	c.trackACK = make(map[string]*erebos.Transport)

	c.redis = redis.NewClient(&redis.Options{
		Addr:     c.Config.Redis.Connect,
		Password: c.Config.Redis.Password,
		DB:       c.Config.Redis.DB,
	})
	if _, err := c.redis.Ping().Result(); err != nil {
		c.Death <- err
		<-c.Shutdown
		return
	}
	defer c.redis.Close()

	c.delay = delay.NewDelay()
	c.discard = make(map[string]bool)
	for _, path := range c.Config.Cyclone.DiscardMetrics {
		c.discard[path] = true
	}
	c.lookup = eyewall.NewLookup(c.Config)
	defer c.lookup.Close()

	c.result = make(chan *alarmResult,
		c.Config.Cyclone.HandlerQueueLength,
	)
	c.run()
}

// InputChannel returns the data input channel
func (c *Cyclone) InputChannel() chan *erebos.Transport {
	return c.Input
}

// ShutdownChannel returns the shutdown signal channel
func (c *Cyclone) ShutdownChannel() chan struct{} {
	return c.Shutdown
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
