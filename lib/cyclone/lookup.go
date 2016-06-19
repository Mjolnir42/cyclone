/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Thresh struct {
	Id             string
	Metric         string
	HostId         uint64
	Oncall         string
	Interval       uint64
	MetaMonitoring string
	MetaTeam       string
	MetaSource     string
	MetaTargethost string
	Predicate      string
	Thresholds     map[uint16]int64
}

func (cl *Cyclone) Lookup(lookup string) map[string]Thresh {
	thr := cl.getThreshold(lookup)
	if thr != nil {
		return thr
	}
	dat := cl.fetchFromEye(lookup)
	if dat == nil {
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
		return nil
	}
	for k, _ := range mapdata {
		if k == `unconfigured` {
			continue
		}
		val, err := cl.Redis.Get(k).Result()
		if err != nil {
			return nil
		}
		t := Thresh{}
		err = json.Unmarshal([]byte(val), &t)
		if err != nil {
			return nil
		}
		res[t.Id] = t
	}
	return res
}

func (cl *Cyclone) fetchFromEye(lookup string) *ThresholdConfig {
	client := &http.Client{}
	req, err := http.NewRequest(`GET`, fmt.Sprintf("http://%s:%s/api/v1/configuration/%s", `localhost`, `7777`, lookup), nil)
	if err != nil {
		return nil
	}

	if resp, err := client.Do(req); err != nil {
		return nil
	} else if resp.StatusCode == 404 {
		cl.storeUnconfigured(lookup)
		return &ThresholdConfig{}
	} else {
		var buf []byte
		buf, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil
		} else {
			resp.Body.Close()
		}
		d := &ThresholdConfig{}
		err = json.Unmarshal(buf, d)
		if err != nil {
			return nil
		}
		return d
	}
}

func (cl *Cyclone) processThresholdData(lookup string, t *ThresholdConfig) {
	for _, i := range t.Configurations {
		t := Thresh{
			Id:             i.ConfigurationItemId,
			Metric:         i.Metric,
			HostId:         i.HostId,
			Oncall:         i.Oncall,
			Interval:       i.Interval,
			MetaMonitoring: i.Metadata.Monitoring,
			MetaTeam:       i.Metadata.Team,
			MetaSource:     i.Metadata.Source,
			MetaTargethost: i.Metadata.Targethost,
		}
		for _, l := range i.Thresholds {
			t.Predicate = l.Predicate
			t.Thresholds[l.Level] = l.Value
		}
		cl.storeThreshold(lookup, &t)
	}
}

func (cl *Cyclone) storeThreshold(lookup string, t *Thresh) {
	buf, _ := json.Marshal(t)
	cl.Redis.Set(t.Id, string(buf), 1440*time.Second)

	cl.Redis.HSet(lookup, t.Id, time.Now().UTC().Format(time.RFC3339))
	cl.Redis.Expire(lookup, 1440*time.Second)
}

func (cl *Cyclone) updateEval(id string) {
	cl.Redis.HSet(`evaluation`, id, time.Now().UTC().Format(time.RFC3339))
}

func (cl *Cyclone) storeUnconfigured(lookup string) {
	cl.Redis.HSet(lookup, `unconfigured`, time.Now().UTC().Format(time.RFC3339))
	cl.Redis.Expire(lookup, 1440*time.Second)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
