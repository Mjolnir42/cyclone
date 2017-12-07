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
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/Sirupsen/logrus"
)

// AlarmEvent is the datatype for sending out alarm notifications
type AlarmEvent struct {
	Source     string `json:"source"`
	EventID    string `json:"event_id"`
	Version    string `json:"version"`
	Sourcehost string `json:"sourcehost"`
	Oncall     string `json:"on_call"`
	Targethost string `json:"targethost"`
	Message    string `json:"message"`
	Level      int64  `json:"level"`
	Timestamp  string `json:"timestamp"`
	Check      string `json:"check"`
	Monitoring string `json:"monitoring"`
	Team       string `json:"team"`
}

// process evaluates a metric and raises alarms as required
func (c *Cyclone) sendAlarm(a AlarmEvent) {
	b := new(bytes.Buffer)
	aSlice := []AlarmEvent{a}
	if err := json.NewEncoder(b).Encode(aSlice); err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR json encoding alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		return
	}
	resp, err := http.Post(
		c.Config.Cyclone.DestinationURI,
		`application/json; charset=utf-8`,
		b,
	)

	if err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR sending alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		return
	}
	logrus.Infof(
		"Cyclone[%d], Dispatched alarm for %s at level %d,"+
			" returncode was %d",
		c.Num, a.EventID, a.Level, resp.StatusCode,
	)
	if resp.StatusCode >= 209 {
		// read response body
		bt, _ := ioutil.ReadAll(resp.Body)
		logrus.Errorf(
			"Cyclone[%d], ResponseMsg(%d): %s",
			c.Num, resp.StatusCode, string(bt),
		)
		resp.Body.Close()

		// reset buffer and encode JSON again so it can be
		// logged
		b.Reset()
		json.NewEncoder(b).Encode(aSlice)
		logrus.Errorf(
			"Cyclone[%d], RequestJSON: %s",
			c.Num, b.String(),
		)
		return
	}
	// ensure http.Response.Body is consumed and closed,
	// otherwise it leaks filehandles
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	c.delay.Done()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
