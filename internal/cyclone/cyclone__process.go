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
	"regexp"
	"strconv"

	"fmt"
	"time"

	"github.com/mjolnir42/erebos"
	metrics "github.com/rcrowley/go-metrics"
	uuid "github.com/satori/go.uuid"
	wall "github.com/solnx/eye/lib/eye.wall"
)

// process evaluates a metric and raises alarms as required
func (c *Cyclone) process(msg *erebos.Transport) error {
	if msg == nil || msg.Value == nil {
		c.AppLog.Warnf("Ignoring empty message from: %d", msg.Metric.MetricName())
		if msg != nil {
			c.commit(msg)
		}
		return nil
	}

	// handle heartbeat messages
	if erebos.IsHeartbeat(msg) {
		c.delay.Use()
		go func() {
			c.lookup.Heartbeat(func() string {
				switch c.Config.Misc.InstanceName {
				case ``:
					return `cyclone`
				default:
					return fmt.Sprintf("cyclone/%s",
						c.Config.Misc.InstanceName)
				}
			}(), c.Num, msg.Value)
			c.delay.Done()
		}()
		return nil
	}

	// increment the counter of received metrics (proof of work)
	c.lookup.UpdateReceived()

	// unmarshal metric
	m := msg.Metric
	metricname := m.MetricName()
	hostname := m.Hostname()
	// ignore metrics configured to discard
	if c.discard[metricname] {
		metrics.GetOrRegisterMeter(`.metrics.discarded.per.second`,
			*c.Metrics).Mark(1)
		// mark as processed
		c.commit(msg)
		return nil
	}

	// non-heartbeat metrics count towards processed metrics
	metrics.GetOrRegisterMeter(`.metrics.processed.per.second`,
		*c.Metrics).Mark(1)

	// metric has no tags for matching with configuration profiles
	if len(m.Tags) == 0 {
		//lookup possible threshold ids and add them to the metric
		if tags, err := c.lookup.GetConfigurationID(
			m.LookupID(),
		); err == nil {
			for k, v := range tags {
				m.Tags = append(m.Tags, string(k)+"="+v)
			}
		} else {
			c.AppLog.Tracef(
				"Cyclone[%d], No configuration id's found for %s.%s",
				c.Num, hostname, metricname,
			)
			c.commit(msg)
			return nil
		}
	}

	// fetch configuration profile information

	thr, err := c.lookup.LookupThreshold(m.LookupID())
	if err == wall.ErrUnconfigured {
		c.AppLog.Debugf(
			"Cyclone[%d], No thresholds configured for %s.%s",
			c.Num, hostname, metricname,
		)
		c.commit(msg)
		return nil
	} else if err != nil {
		c.AppLog.Errorf(
			"Cyclone[%d], ERROR fetching threshold data."+
				" Lookup service available?",
			c.Num,
		)
		// msg is not committed since processing failed from
		// external error
		return err
	}

	// start metric evaluation
	var evaluations int64
	// panic on entropy generation errors is reasonable.
	trackingID := uuid.Must(uuid.NewV4()).String()

	evals := metrics.GetOrRegisterMeter(
		`.evaluations.per.second`,
		*c.Metrics,
	)
	c.AppLog.Debugf(
		"Cyclone[%d], Forwarding %s from %s for evaluation (%s)",
		c.Num, metricname, hostname, m.LookupID(),
	)

	// loop over all returned threshold definitions
thrloop:
	for key := range thr {
		evalThreshold := false

		// check if this threshold's ID is found in the metric's tags
	tagloop:
		for _, t := range m.GetTagMap() {
			if !isUUID(t) {
				continue tagloop
			}
			if thr[key].ID == t {
				evalThreshold = true
				break tagloop
			}

		}

		// metric is not evaluated against this threshold
		if !evalThreshold {
			continue thrloop
		}

		// this metric matches a threshold definition, mark it as active
		err = c.lookup.Activate(thr[key].ID)
		if err != nil {
			c.AppLog.Errorf(
				"Cyclone[%d], ERROR activating profile %s: %s",
				c.Num,
				thr[key].ID,
				err.Error(),
			)
		}

		// perform threshold evaluation
		alarmLevel, value, ev := c.evaluate(&m, thr[key])
		switch ev {
		case 0:
			// threshold definition has no thresholds... count this
			// as one evaluation since this generated an OK event
			// and the Zookeeper offset commit is handled by
			// sendAlarm
			evaluations++
		default:
			evaluations += ev
		}

		// construct alarm
		al := AlarmEvent{
			Source: fmt.Sprintf("%s / %s",
				thr[key].MetaTargethost,
				thr[key].MetaSource,
			),
			EventID:    thr[key].ID,
			Version:    c.Config.Cyclone.APIVersion,
			Sourcehost: thr[key].MetaTargethost,
			Oncall:     thr[key].Oncall,
			Targethost: thr[key].MetaTargethost,
			Timestamp:  time.Unix(m.Time, 0).UTC().Format(time.RFC3339Nano),
			Check:      fmt.Sprintf("cyclone(%s)", metricname),
			Monitoring: thr[key].MetaMonitoring,
			Team:       thr[key].MetaTeam,
		}
		al.Level, _ = strconv.ParseInt(alarmLevel, 10, 64)
		switch al.Level {
		case 0:
			al.Message = `Ok.`
		default:
			al.Message = fmt.Sprintf(
				"Metric %s has broken threshold. Value %s %s %d",
				metricname,
				value,
				thr[key].Predicate,
				thr[key].Thresholds[alarmLevel],
			)
		}
		if al.Oncall == `` {
			al.Oncall = `No oncall information available`
		}

		// update evalutation timestamp for ID in local cache
		c.lookup.Evaluated(thr[key].ID)

		if c.Config.Cyclone.TestMode {
			// do not send out alarms in testmode
			continue thrloop
		}
		metrics.GetOrRegisterMeter(`.alarms.per.second`,
			*c.Metrics).Mark(1)

		metrics.GetOrRegisterHistogram(
			`/alarm.delay.seconds`,
			*c.Metrics,
			metrics.NewExpDecaySample(1028, 0.03),
		).Update(
			time.Now().UTC().Sub(time.Unix(m.Time, 0).UTC()).Nanoseconds(),
		)
		c.trackID[trackingID]++
		c.trackACK[trackingID] = msg
		c.delay.Use()
		go c.sendAlarm(al, trackingID)
	}
	evals.Mark(evaluations)
	if evaluations == 0 {
		c.AppLog.Debugf(
			"Cyclone[%d], metric %s(%d) matched no configurations",
			c.Num, metricname, hostname,
		)
		// no evaluations means no alarms, commit offset as processed
		c.commit(msg)
	}
	return nil
}

// isUUID validates if a string is one very narrow formatting of a
// UUID. Other valid formats with braces etc are not accepted.
func isUUID(s string) bool {
	const reUUID string = `^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[1-5][[:xdigit:]]{3}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`
	const reUNIL string = `^0{8}-0{4}-0{4}-0{4}-0{12}$`
	re := regexp.MustCompile(fmt.Sprintf("%s|%s", reUUID, reUNIL))

	return re.MatchString(s)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
