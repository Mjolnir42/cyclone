/*-
 * Copyright (c) 2017, Jörg Pernfuß
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"os"

	log "github.com/Sirupsen/logrus"
)

// Logrotate reopens the filehandle in conf whenever a signal
// is received via sigChan.
func Logrotate(sigChan chan os.Signal, conf Config) {
	for {
		select {
		case <-sigChan:
			if conf.Log.FH == nil {
				continue
			}
			err := conf.Log.FH.Reopen()
			if err != nil {
				log.SetOutput(os.Stderr)
				log.Fatalf("Error rotating logfile: %s", err)
			}
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
