/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone

type ThresholdConfig struct {
	Configurations []ConfigurationItem `json:"configurations"`
}

type ConfigurationList struct {
	ConfigurationItemIdList []string `json:"configuration_item_id_list"`
}

type ConfigurationItem struct {
	ConfigurationItemId string                   `json:"configuration_item_id"`
	Metric              string                   `json:"metric"`
	HostId              uint64                   `json:"host_id,string"`
	Tags                []string                 `json:"tags,omitempty"`
	Oncall              string                   `json:"oncall"`
	Interval            uint64                   `json:"interval"`
	Metadata            ConfigurationMetaData    `json:"metadata"`
	Thresholds          []ConfigurationThreshold `json:"thresholds"`
}

type ConfigurationMetaData struct {
	Monitoring string `json:"monitoring"`
	Team       string `json:"string"`
	Source     string `json:"source"`
	Targethost string `json:"targethost"`
}

type ConfigurationThreshold struct {
	Predicate string `json:"predicate"`
	Level     uint16 `json:"level"`
	Value     int64  `json:"value"`
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
