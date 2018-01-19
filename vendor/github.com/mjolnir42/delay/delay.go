/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package delay implements a flexible waiting policy
package delay

import (
	"sync"
	"sync/atomic"
)

// Delay can be used for continuous, lazy waiting for a set
// of goroutines.
//
//	d := delay.New()
//	...
//	d.Use()
//	go func() {
//	    defer d.Done()
//	    ...
//	}()
//	d.Wait()
//
// Not calling Done() will cause Wait() to never return
type Delay struct {
	usage uint32
	lock  *sync.RWMutex
	cond  *sync.Cond
}

// New returns a new delay
func New() *Delay {
	d := &Delay{
		lock: &sync.RWMutex{},
	}
	d.cond = sync.NewCond(d.lock.RLocker())
	return d
}

// Use signals d that it is in use by an additional goroutine
func (d *Delay) Use() {
	// acquire write lock
	d.lock.Lock()
	// increment usage
	atomic.AddUint32(&d.usage, 1)
	// release write lock
	d.lock.Unlock()
}

// Done signals d that a goroutine no longer uses it
func (d *Delay) Done() {
	broadcast := false

	d.lock.Lock()
	atomic.AddUint32(&d.usage, ^uint32(0))
	if d.unused() {
		broadcast = true
	}
	d.lock.Unlock()

	if broadcast {
		d.cond.Broadcast()
	}
}

// Go runs f inside a goroutine which marks d as in use. The following
// two examples achieve the same.
//
//	d := delay.New()
//	d.Go(func() {
//		foobar(42)
//	})
//	...
//
//	d.Use()
//	go func() {
//		defer d.Done()
//		foobar(42)
//	}()
func (d *Delay) Go(f func()) {
	d.Use()
	go func(fn func()) {
		defer d.Done()
		fn()
	}(f)
}

// Wait blocks until d is unused. Users of d can change while
// it is blocking.
func (d *Delay) Wait() {
	// check if they pool is available
	d.cond.L.Lock()
	for !d.unused() {
		d.cond.Wait()
	}
	d.cond.L.Unlock()
}

// unused checks if d is no longer in use. It must only be called
// while the lock is already held
func (d *Delay) unused() bool {
	if d.usage == 0 {
		return true
	}
	return false
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
