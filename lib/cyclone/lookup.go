/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/lib/cyclone"

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
)

type Thresh struct {
	ID             string
	Metric         string
	HostID         uint64
	Oncall         string
	Interval       uint64
	MetaMonitoring string
	MetaTeam       string
	MetaSource     string
	MetaTargethost string
	Predicate      string
	Thresholds     map[string]int64
}

func (c *Cyclone) Lookup(lookup string) map[string]Thresh {
	thr := c.getThreshold(lookup)
	if thr != nil {
		return thr
	}
	dat := c.fetchFromLookupService(lookup)
	if dat == nil {
		logrus.Errorf("Cyclone[%d], ERROR Lookup received nil from fetcher for %s", c.Num, lookup)
		c.storeUnconfigured(lookup)
		return nil
	}
	c.processThresholdData(lookup, dat)
	thr = c.getThreshold(lookup)
	return thr
}

func (c *Cyclone) getThreshold(lookup string) map[string]Thresh {
	res := make(map[string]Thresh)
	mapdata, err := c.redis.HGetAllMap(lookup).Result()
	if err != nil {
		logrus.Errorf("Cyclone[%d], ERROR reading from redis for %s: %s", c.Num, lookup, err)
		return nil
	}
	if len(mapdata) == 0 {
		logrus.Infof("Cyclone[%d], no entry in redis for %s", c.Num, lookup)
		return nil
	}
	for k := range mapdata {
		if k == `unconfigured` {
			logrus.Debugf("Cyclone[%d], Found negative caching in redis for %s", c.Num, lookup)
			continue
		}
		val, err := c.redis.Get(k).Result()
		if err != nil {
			logrus.Errorf("Cyclone[%d], ERROR reading from redis for %s: %s", c.Num, lookup, err)
			return nil
		}
		t := Thresh{}
		err = json.Unmarshal([]byte(val), &t)
		if err != nil {
			logrus.Errorf("Cyclone[%d], ERROR decoding threshold from redis for %s: %s", c.Num, lookup, err)
			return nil
		}
		res[t.ID] = t
	}
	return res
}

func (c *Cyclone) fetchFromLookupService(lookup string) *ThresholdConfig {
	logrus.Debugf("Cyclone[%d], Looking up configuration data for %s", c.Num, lookup)
	client := &http.Client{}
	req, err := http.NewRequest(`GET`, fmt.Sprintf(
		"http://%s:%s/%s/%s",
		c.Config.Cyclone.LookupHost,
		c.Config.Cyclone.LookupPort,
		c.Config.Cyclone.LookupPath,
		lookup,
	), nil)
	if err != nil {
		logrus.Errorf("Cyclone[%d], ERROR assembling lookup request: %s", c.Num, err)
		return nil
	}

	if resp, err := client.Do(req); err != nil {
		logrus.Errorf("Cyclone[%d], ERROR during lookup request: %s", c.Num, err)
		return nil
	} else if resp.StatusCode == 404 {
		logrus.Debugf("Cyclone[%d], no configurations for %s", c.Num, lookup)
		c.storeUnconfigured(lookup)
		return &ThresholdConfig{}
	} else {
		var buf []byte
		buf, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			logrus.Errorf("Cyclone[%d], ERROR reading result body for %s: %s", c.Num, lookup, err)
			return nil
		}
		resp.Body.Close()

		d := &ThresholdConfig{}
		err = json.Unmarshal(buf, d)
		if err != nil {
			logrus.Errorf("Cyclone[%d], ERROR decoding result body for %s: %s", c.Num, lookup, err)
			return nil
		}
		return d
	}
}

func (c *Cyclone) processThresholdData(lookup string, t *ThresholdConfig) {
	if t.Configurations == nil {
		return
	}
	if len(t.Configurations) == 0 {
		c.storeUnconfigured(lookup)
		return
	}
	for _, i := range t.Configurations {
		t := Thresh{
			ID:             i.ConfigurationItemID,
			Metric:         i.Metric,
			HostID:         i.HostID,
			Oncall:         i.Oncall,
			Interval:       i.Interval,
			MetaMonitoring: i.Metadata.Monitoring,
			MetaTeam:       i.Metadata.Team,
			MetaSource:     i.Metadata.Source,
			MetaTargethost: i.Metadata.Targethost,
		}
		t.Thresholds = make(map[string]int64)
		for _, l := range i.Thresholds {
			lvl := strconv.FormatUint(uint64(l.Level), 10)
			t.Predicate = l.Predicate
			t.Thresholds[lvl] = l.Value
		}
		c.storeThreshold(lookup, &t)
	}
}

func (c *Cyclone) storeThreshold(lookup string, t *Thresh) {
	buf, err := json.Marshal(t)
	if err != nil {
		logrus.Errorf("%s: ERROR (storeThreshold) converting threshold data: %s", lookup, err)
		return
	}
	c.redis.Set(t.ID, string(buf), 1440*time.Second)

	c.redis.HSet(lookup, t.ID, time.Now().UTC().Format(time.RFC3339))
	c.redis.Expire(lookup, 1440*time.Second)
}

func (c *Cyclone) updateEval(id string) {
	c.redis.HSet(`evaluation`, id, time.Now().UTC().Format(time.RFC3339))
}

func (c *Cyclone) heartbeat() {
	logrus.Debugf("Cyclone[%d], Updating cyclone heartbeat", c.Num)
	if _, err := c.redis.HSet(`heartbeat`, `cyclone-alive`, time.Now().UTC().Format(time.RFC3339)).Result(); err != nil {
		logrus.Errorf("Cyclone[%d], ERROR setting heartbeat in redis: %s", c.Num, err)
	}
	if _, err := c.redis.HSet(`heartbeat`, fmt.Sprintf("cyclone-alive-%d", c.Num), time.Now().UTC().Format(time.RFC3339)).Result(); err != nil {
		logrus.Errorf("Cyclone[%d], ERROR setting heartbeat in redis: %s", c.Num, err)
	}
}

func (c *Cyclone) storeUnconfigured(lookup string) {
	c.redis.HSet(lookup, `unconfigured`, time.Now().UTC().Format(time.RFC3339))
	c.redis.Expire(lookup, 1440*time.Second)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
