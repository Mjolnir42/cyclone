/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

// DisableZKLogger is meant to be called in init() and will
// disable the package logger in github.com/samuel/go-zookeeper/zk
func DisableZKLogger() {
	// Discard logspam from Zookeeper library
	l := logrus.New()
	l.Out = ioutil.Discard
	zk.DefaultLogger = l
}

// SetLogrusOptions is meant to be called in init() and will
// set common Logrus options
func SetLogrusOptions() {
	// set standard logger options
	std := logrus.StandardLogger()
	std.Formatter = &logrus.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
