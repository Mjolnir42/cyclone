/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mjolnir42/cyclone/lib/cyclone"
	"github.com/mjolnir42/cyclone/lib/metric"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

func main() {
	conf := CycloneConfig{}
	if err := conf.readConfigFile(`cyclone.conf`); err != nil {
		log.Fatalln(err)
	}

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	var zkNodes []string

	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(conf.Zookeeper)

	topic := strings.Split(conf.Topics, `,`)

	consumer, err := consumergroup.JoinConsumerGroup(conf.ConsumerGroup, topic, zkNodes, config)
	if err != nil {
		log.Fatalln(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	eventCount := 0
	offsets := make(map[string]map[int32]int64)
	handlers := make(map[int]cyclone.Cyclone)

	for i := 0; i < runtime.NumCPU(); {
		cl := cyclone.Cyclone{
			CfgRedisConnect:     conf.RedisConnect,
			CfgRedisPassword:    conf.RedisPassword,
			CfgRedisDB:          conf.RedisDB,
			CfgAlarmDestination: conf.AlarmDestination,
			CfgLookupHost:       conf.LookupHost,
			CfgLookupPort:       conf.LookupPort,
			CfgLookupPath:       conf.LookupPath,
		}
		handlers[i] = cl
		go cl.Run()
	}

runloop:
	for {
		select {
		case <-c:
			break runloop
		case e := <-consumer.Errors():
			log.Println(e)
		case message := <-consumer.Messages():
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}

			eventCount += 1
			if offsets[message.Topic][message.Partition] != 0 &&
				offsets[message.Topic][message.Partition] != message.Offset-1 {
				log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic, message.Partition,
					offsets[message.Topic][message.Partition]+1, message.Offset,
					message.Offset-offsets[message.Topic][message.Partition]+1,
				)
			}

			m, err := metric.FromBytes(message.Value)
			if err != nil {
				log.Println(err)
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			// ignored metrics
			switch m.Path {
			case `/sys/disk/blk_read`:
				fallthrough
			case `/sys/disk/blk_total`:
				fallthrough
			case `/sys/disk/blk_used`:
				fallthrough
			case `/sys/disk/blk_wrtn`:
				fallthrough
			case `/sys/disk/fs`:
				fallthrough
			case `/sys/disk/mounts`:
				fallthrough
			case `/sys/net/mac`:
				fallthrough
			case `/sys/net/rx_bytes`:
				fallthrough
			case `/sys/net/rx_packets`:
				fallthrough
			case `/sys/net/tx_bytes`:
				fallthrough
			case `/sys/net/tx_packets`:
				m = nil
			}
			if m == nil {
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			handlers[int(m.AssetId)%runtime.NumCPU()].Input <- m
			log.Printf("Sent %s/%d/%d\n to processing", message.Topic, message.Partition, message.Offset)

			offsets[message.Topic][message.Partition] = message.Offset
			consumer.CommitUpto(message)
		}
	}
	if err := consumer.Close(); err != nil {
		sarama.Logger.Println("Error closing the consumer", err)
	}

	log.Printf("Processed %d events.", eventCount)
	log.Printf("%+v", offsets)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
