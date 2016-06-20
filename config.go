/*-
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 * Written by Jörg Pernfuß <joerg.pernfuss@1und1.de>
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/nahanni/go-ucl"
)

type CycloneConfig struct {
	Zookeeper        string `json:"zookeeper.connect"`
	ZkSync           int    `json:"zookeeper.commit.ms,string"`
	Topics           string `json:"topics"`
	ConsumerGroup    string `json:"consumergroup.name"`
	RedisConnect     string `json:"redis.connect"`
	RedisPassword    string `json:"redis.password"`
	RedisDB          int64  `json:"redis.db,string"`
	AlarmDestination string `json:"alarming.destination"`
	LookupHost       string `json:"lookup.host"`
	LookupPort       string `json:"lookup.port"`
	LookupPath       string `json:"lookup.path"`
}

func (c *CycloneConfig) readConfigFile(fname string) error {
	file, err := ioutil.ReadFile(fname)
	if err != nil {
		return err
	}

	log.Printf("Loading configuration from %s", fname)

	// UCL parses into map[string]interface{}
	fileBytes := bytes.NewBuffer([]byte(file))
	parser := ucl.NewParser(fileBytes)
	uclData, err := parser.Ucl()
	if err != nil {
		log.Fatal("UCL error: ", err)
	}

	// take detour via JSON to load UCL into struct
	uclJson, err := json.Marshal(uclData)
	if err != nil {
		log.Fatal(err)
	}
	return json.Unmarshal([]byte(uclJson), &c)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
