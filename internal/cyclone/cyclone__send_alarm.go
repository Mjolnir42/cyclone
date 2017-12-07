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
	"fmt"
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

// alarmResult encapsulates the result from a HTTP POST request
// to the AlarmAPI
type alarmResult struct {
	trackingID string
	err        error
	internal   bool
	alarm      *AlarmEvent
}

// process evaluates a metric and raises alarms as required. Must
// only be called as goroutine after c.delay.Use()
func (c *Cyclone) sendAlarm(a AlarmEvent, trackingID string) {
	b := new(bytes.Buffer)
	aSlice := []AlarmEvent{a}
	if err := json.NewEncoder(b).Encode(aSlice); err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR json encoding alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			internal:   true,
			alarm:      &a,
		}
		return
	}
	// acquire resource limit before issuing the POST request
	c.Limit.Start()

	resp, err := http.Post(
		c.Config.Cyclone.DestinationURI,
		`application/json; charset=utf-8`,
		b,
	)

	// release resource limit
	c.Limit.Done()

	if err != nil {
		logrus.Errorf(
			"Cyclone[%d], ERROR sending alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			alarm:      &a,
		}
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
		err = fmt.Errorf(
			"Cyclone[%d], ResponseMsg(%d): %s",
			c.Num, resp.StatusCode, string(bt),
		)
		logrus.Errorln(err.Error())
		resp.Body.Close()

		// reset buffer and encode JSON again so it can be
		// logged
		b.Reset()
		json.NewEncoder(b).Encode(aSlice)
		logrus.Errorf(
			"Cyclone[%d], RequestJSON: %s",
			c.Num, b.String(),
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			alarm:      &a,
		}
		return
	}
	// ensure http.Response.Body is consumed and closed,
	// otherwise it leaks filehandles
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	c.delay.Done()
	c.result <- &alarmResult{
		trackingID: trackingID,
		err:        nil,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
