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

	"github.com/mjolnir42/cyclone/internal/cyclone/cpu"
	"github.com/mjolnir42/cyclone/internal/cyclone/disk"
	"github.com/mjolnir42/cyclone/internal/cyclone/mem"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
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

	c.CPUData = make(map[int64]cpu.CPU)
	c.MemData = make(map[int64]mem.Mem)
	c.CTXData = make(map[int64]cpu.CTX)
	c.DskData = make(map[int64]map[string]disk.Disk)
	c.internalInput = make(chan *legacy.MetricSplit, 32)
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
