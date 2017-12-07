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
	"time"

	"github.com/Sirupsen/logrus"
	metrics "github.com/rcrowley/go-metrics"
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
	defer c.delay.Done()
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
		// 4xx errors are caused on this side, abort
		if resp.StatusCode < 500 {
			c.result <- &alarmResult{
				trackingID: trackingID,
				err:        err,
				internal:   true,
				alarm:      &a,
			}
			return
		}
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
	c.result <- &alarmResult{
		trackingID: trackingID,
		err:        nil,
	}
}

// resendAlarm raises the alarmapi.error metric and attempts to
// resend a every 5 seconds
func (c *Cyclone) resendAlarm(a *AlarmEvent, trackingID string) {
	metrics.GetOrRegisterGauge(`/alarmapi.error`,
		*c.Metrics).Update(1)
	broken := true

	b := new(bytes.Buffer)
	aSlice := []AlarmEvent{*a}
	// encoding a previously did not cause an internal error
	json.NewEncoder(b).Encode(aSlice)

	// fast first request attempt
	resendDelay := time.Millisecond * 50

	for broken == true {
		select {
		// always listen for shutdown requests
		case <-c.Shutdown:
			return
		// attempt resend every 5 seconds
		case <-time.After(resendDelay):
		}
		// increase resendDelay for after first attempt
		resendDelay = 5 * time.Second

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
			continue
		}
		if resp.StatusCode >= 209 {
			bt, _ := ioutil.ReadAll(resp.Body)
			logrus.Errorf(
				"Cyclone[%d], ResponseMsg(%d): %s",
				c.Num, resp.StatusCode, string(bt),
			)
			resp.Body.Close()
			continue
		}
		broken = false
	}

	// switch error metric off
	metrics.GetOrRegisterGauge(`/alarmapi.error`,
		*c.Metrics).Update(0)

	// update offset directly since the result channel at this
	// point likely blocks
	c.updateOffset(trackingID)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
