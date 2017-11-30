/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

// Handler represents an erebos compatible application handler
type Handler interface {
	Start()
	InputChannel() chan *Transport
	ShutdownChannel() chan struct{}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
