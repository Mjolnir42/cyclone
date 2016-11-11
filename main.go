/*-
 * Copyright © 2016, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/client9/reopen"
	"github.com/mjolnir42/cyclone/lib/cyclone"
	"github.com/mjolnir42/cyclone/lib/metric"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

var githash, shorthash, builddate, buildtime string

func main() {
	var (
		err                 error
		configFlag, logFlag string
		configFile, logFile string
		logFH               *reopen.FileWriter
		versionFlag         bool
	)
	flag.StringVar(&configFlag, `config`, `cyclone.conf`, `Configuration file location`)
	flag.StringVar(&logFlag, `log`, `cyclone.log`, `Logfile location`)
	flag.BoolVar(&versionFlag, `version`, false, `Print version information`)
	flag.Parse()

	// only provide version information if --version was specified
	if versionFlag {
		fmt.Fprintln(os.Stderr, `Cyclone Metric Monitoring System`)
		fmt.Fprintf(os.Stderr, "Version  : %s-%s\n", builddate, shorthash)
		fmt.Fprintf(os.Stderr, "Git Hash : %s\n", githash)
		fmt.Fprintf(os.Stderr, "Timestamp: %s\n", buildtime)
		os.Exit(0)
	}

	// load configuration file
	if configFile, err = filepath.Abs(configFlag); err != nil {
		log.Fatalln(err)
	}
	if configFile, err = filepath.EvalSymlinks(configFile); err != nil {
		log.Fatalln(err)
	}
	conf := CycloneConfig{}
	if err = conf.readConfigFile(configFile); err != nil {
		log.Fatalln(err)
	}

	// set logfile
	if logFile, err = filepath.Abs(logFlag); err != nil {
		log.Fatalln(`abs`, err)
	}
	if logFH, err = reopen.NewFileWriter(logFile); err != nil {
		log.Fatalln(`reopen`, err)
	}
	log.SetOutput(logFH)

	// register signal handler for logrotate on SIGUSR2
	sigChanLogRotate := make(chan os.Signal, 1)
	signal.Notify(sigChanLogRotate, syscall.SIGUSR2)
	go func(sigChan chan os.Signal, fh *reopen.FileWriter) {
		for {
			select {
			case <-sigChan:
				if fail := fh.Reopen(); fail != nil {
					fmt.Fprintln(os.Stderr, `FATAL - Failed to rotate logfile.`, err)
					os.Exit(2)
				}
			}
		}
	}(sigChanLogRotate, logFH)

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.CommitInterval = time.Duration(conf.ZkSync) * time.Millisecond
	config.Offsets.ResetOffsets = conf.ZkResetOffset
	var zkNodes []string

	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(conf.Zookeeper)
	log.Println(`Using ZK chroot: `, config.Zookeeper.Chroot)

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

	for i := 0; i < runtime.NumCPU(); i++ {
		log.Printf("MAIN, Starting cyclone handler %d", i)
		cChan := make(chan *metric.Metric)
		cl := cyclone.Cyclone{
			Num:                 i,
			Input:               cChan,
			CfgRedisConnect:     conf.RedisConnect,
			CfgRedisPassword:    conf.RedisPassword,
			CfgRedisDB:          conf.RedisDB,
			CfgAlarmDestination: conf.AlarmDestination,
			CfgLookupHost:       conf.LookupHost,
			CfgLookupPort:       conf.LookupPort,
			CfgLookupPath:       conf.LookupPath,
			CfgApiVersion:       conf.ApiVersion,
			TestMode:            conf.TestMode,
		}
		handlers[i] = cl
		go cl.Run()
	}

	heartbeat := time.Tick(5 * time.Second)

	ageCutOff := time.Duration(conf.MetricsMaxAge) * time.Minute * -1

runloop:
	for {
		select {
		case <-c:
			break runloop
		case <-heartbeat:
			handlers[0].Input <- &metric.Metric{
				Path: `_internal.cyclone.heartbeat`,
			}
			continue runloop
		case e := <-consumer.Errors():
			log.Println(e)
		case message := <-consumer.Messages():
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}

			log.Printf("MAIN, Received topic:%s/partition:%d/offset:%d",
				message.Topic, message.Partition, message.Offset)

			eventCount += 1
			if offsets[message.Topic][message.Partition] != 0 &&
				offsets[message.Topic][message.Partition] != message.Offset-1 {
				log.Printf("MAIN ERROR, Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic, message.Partition,
					offsets[message.Topic][message.Partition]+1, message.Offset,
					message.Offset-offsets[message.Topic][message.Partition]+1,
				)
			}

			m, err := metric.FromBytes(message.Value)
			if err != nil {
				log.Printf("MAIN ERROR, Decoding metric data: %s\n", err)
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			// ignored metrics
			switch m.Path {
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
				fallthrough
			case `/sys/memory/swapcached`:
				fallthrough
			case `/sys/load/last_pid`:
				fallthrough
			case `/sys/cpu/idletime`:
				fallthrough
			case `/sys/cpu/MHz`:
				fallthrough
			case `/sys/net/bondslave`:
				fallthrough
			case `/sys/net/connstates/ipv4`:
				fallthrough
			case `/sys/net/connstates/ipv6`:
				fallthrough
			case `/sys/net/duplex`:
				fallthrough
			case `/sys/net/ipv4_addr`:
				fallthrough
			case `/sys/net/ipv6_addr`:
				fallthrough
			case `/sys/net/speed`:
				fallthrough
			case `/sys/net/ipvs/conn/count`:
				fallthrough
			case `/sys/net/ipvs/conn/servercount`:
				fallthrough
			case `/sys/net/ipvs/conn/serverstatecount`:
				fallthrough
			case `/sys/net/ipvs/conn/statecount`:
				fallthrough
			case `/sys/net/ipvs/conn/vipconns`:
				fallthrough
			case `/sys/net/ipvs/conn/vipstatecount`:
				fallthrough
			case `/sys/net/ipvs/count`:
				fallthrough
			case `/sys/net/ipvs/detail`:
				fallthrough
			case `/sys/net/ipvs/state`:
				fallthrough
			case `/sys/net/quagga/bgp/announce`:
				fallthrough
			case `/sys/net/quagga/bgp/connage`:
				fallthrough
			case `/sys/net/quagga/bgp/connstate`:
				fallthrough
			case `/sys/net/quagga/bgp/neighbour`:
				m = nil
			}
			if m == nil {
				log.Println(`MAIN, Ignoring received metric`)
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			// ignore metrics that are simply too old for useful
			// alerting
			if time.Now().UTC().Add(ageCutOff).After(m.TS.UTC()) {
				log.Printf("MAIN ERROR, Skipping metric due to age: %s", m.TS.UTC().Format(time.RFC3339))
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			handlers[int(m.AssetId)%runtime.NumCPU()].Input <- m

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
