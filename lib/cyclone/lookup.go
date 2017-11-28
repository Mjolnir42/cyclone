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

func (cl *Cyclone) Lookup(lookup string) map[string]Thresh {
	thr := cl.getThreshold(lookup)
	if thr != nil {
		return thr
	}
	dat := cl.fetchFromLookupService(lookup)
	if dat == nil {
		cl.logger.Errorf("Cyclone[%d], ERROR Lookup received nil from fetcher for %s", cl.Num, lookup)
		cl.storeUnconfigured(lookup)
		return nil
	}
	cl.processThresholdData(lookup, dat)
	thr = cl.getThreshold(lookup)
	return thr
}

func (cl *Cyclone) getThreshold(lookup string) map[string]Thresh {
	res := make(map[string]Thresh)
	mapdata, err := cl.Redis.HGetAllMap(lookup).Result()
	if err != nil {
		cl.logger.Errorf("Cyclone[%d], ERROR reading from redis for %s: %s", cl.Num, lookup, err)
		return nil
	}
	if len(mapdata) == 0 {
		cl.logger.Infof("Cyclone[%d], no entry in redis for %s", cl.Num, lookup)
		return nil
	}
	for k := range mapdata {
		if k == `unconfigured` {
			cl.logger.Debugf("Cyclone[%d], Found negative caching in redis for %s", cl.Num, lookup)
			continue
		}
		val, err := cl.Redis.Get(k).Result()
		if err != nil {
			cl.logger.Errorf("Cyclone[%d], ERROR reading from redis for %s: %s", cl.Num, lookup, err)
			return nil
		}
		t := Thresh{}
		err = json.Unmarshal([]byte(val), &t)
		if err != nil {
			cl.logger.Errorf("Cyclone[%d], ERROR decoding threshold from redis for %s: %s", cl.Num, lookup, err)
			return nil
		}
		res[t.ID] = t
	}
	return res
}

func (cl *Cyclone) fetchFromLookupService(lookup string) *ThresholdConfig {
	cl.logger.Debugf("Cyclone[%d], Looking up configuration data for %s", cl.Num, lookup)
	client := &http.Client{}
	req, err := http.NewRequest(`GET`, fmt.Sprintf(
		"http://%s:%s/%s/%s",
		cl.CfgLookupHost,
		cl.CfgLookupPort,
		cl.CfgLookupPath,
		lookup,
	), nil)
	if err != nil {
		cl.logger.Errorf("Cyclone[%d], ERROR assembling lookup request: %s", cl.Num, err)
		return nil
	}

	if resp, err := client.Do(req); err != nil {
		cl.logger.Errorf("Cyclone[%d], ERROR during lookup request: %s", cl.Num, err)
		return nil
	} else if resp.StatusCode == 404 {
		cl.logger.Debugf("Cyclone[%d], no configurations for %s", cl.Num, lookup)
		cl.storeUnconfigured(lookup)
		return &ThresholdConfig{}
	} else {
		var buf []byte
		buf, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			cl.logger.Errorf("Cyclone[%d], ERROR reading result body for %s: %s", cl.Num, lookup, err)
			return nil
		}
		resp.Body.Close()

		d := &ThresholdConfig{}
		err = json.Unmarshal(buf, d)
		if err != nil {
			cl.logger.Errorf("Cyclone[%d], ERROR decoding result body for %s: %s", cl.Num, lookup, err)
			return nil
		}
		return d
	}
}

func (cl *Cyclone) processThresholdData(lookup string, t *ThresholdConfig) {
	if t.Configurations == nil {
		return
	}
	if len(t.Configurations) == 0 {
		cl.storeUnconfigured(lookup)
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
		cl.storeThreshold(lookup, &t)
	}
}

func (cl *Cyclone) storeThreshold(lookup string, t *Thresh) {
	buf, err := json.Marshal(t)
	if err != nil {
		cl.logger.Errorf("%s: ERROR (storeThreshold) converting threshold data: %s", lookup, err)
		return
	}
	cl.Redis.Set(t.ID, string(buf), 1440*time.Second)

	cl.Redis.HSet(lookup, t.ID, time.Now().UTC().Format(time.RFC3339))
	cl.Redis.Expire(lookup, 1440*time.Second)
}

func (cl *Cyclone) updateEval(id string) {
	cl.Redis.HSet(`evaluation`, id, time.Now().UTC().Format(time.RFC3339))
}

func (cl *Cyclone) heartbeat() {
	cl.logger.Debugf("Cyclone[%d], Updating cyclone heartbeat", cl.Num)
	if _, err := cl.Redis.HSet(`heartbeat`, `cyclone-alive`, time.Now().UTC().Format(time.RFC3339)).Result(); err != nil {
		cl.logger.Errorf("Cyclone[%d], ERROR setting heartbeat in redis: %s", cl.Num, err)
	}
	if _, err := cl.Redis.HSet(`heartbeat`, fmt.Sprintf("cyclone-alive-%d", cl.Num), time.Now().UTC().Format(time.RFC3339)).Result(); err != nil {
		cl.logger.Errorf("Cyclone[%d], ERROR setting heartbeat in redis: %s", cl.Num, err)
	}
}

func (cl *Cyclone) storeUnconfigured(lookup string) {
	cl.Redis.HSet(lookup, `unconfigured`, time.Now().UTC().Format(time.RFC3339))
	cl.Redis.Expire(lookup, 1440*time.Second)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
