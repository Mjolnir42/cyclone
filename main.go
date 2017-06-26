/*-
 * Copyright © 2016,2017 Jörg Pernfuß <code.jpe@gmail.com>
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
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/client9/reopen"
	"github.com/mjolnir42/cyclone/lib/cyclone"
	"github.com/mjolnir42/cyclone/lib/metric"
	"github.com/mjolnir42/erebos"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

var githash, shorthash, builddate, buildtime string

func init() {
	// Discard logspam from Zookeeper library
	erebos.DisableZKLogger()

	// set standard logger options
	erebos.SetLogrusOptions()

	// redirect go default logger to /dev/null
	log.SetOutput(ioutil.Discard)
}

func main() {
	var (
		err         error
		configFlag  string
		logFH       *reopen.FileWriter
		versionFlag bool
	)
	flag.StringVar(&configFlag, `config`, `cyclone.conf`, `Configuration file location`)
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
	conf := erebos.Config{}
	if err = conf.FromFile(configFlag); err != nil {
		logrus.Fatalf("Could not open configuration: %s", err)
	}

	// setup logfile
	if logFH, err = reopen.NewFileWriter(
		filepath.Join(conf.Log.Path, conf.Log.File),
	); err != nil {
		logrus.Fatalf("Unable to open logfile: %s", err)
	} else {
		conf.Log.FH = logFH
	}
	logrus.SetOutput(conf.Log.FH)

	// register signal handler for logrotate on SIGUSR2
	if conf.Log.Rotate {
		sigChanLogRotate := make(chan os.Signal, 1)
		signal.Notify(sigChanLogRotate, syscall.SIGUSR2)
		go erebos.Logrotate(sigChanLogRotate, conf)
	}

	// register signal handler for shutdown on SIGINT/SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	config := consumergroup.NewConfig()
	switch conf.Kafka.ConsumerOffsetStrategy {
	case `Oldest`, `oldest`:
	case `Newest`, `newest`:
		config.Offsets.Initial = sarama.OffsetNewest
	default:
		logrus.Fatalf("Invalid consumer strategy: %s",
			conf.Kafka.ConsumerOffsetStrategy)
	}
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.CommitInterval = time.Duration(
		conf.Zookeeper.CommitInterval,
	) * time.Millisecond
	config.Offsets.ResetOffsets = conf.Zookeeper.ResetOffset
	var zkNodes []string

	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(conf.Zookeeper.Connect)
	logrus.Println(`Using ZK chroot: `, config.Zookeeper.Chroot)

	topic := strings.Split(conf.Kafka.ConsumerTopics, `,`)

	consumer, err := consumergroup.JoinConsumerGroup(conf.Kafka.ConsumerGroup, topic, zkNodes, config)
	if err != nil {
		logrus.Fatalln(err)
	}

	eventCount := 0
	offsets := make(map[string]map[int32]int64)
	handlers := make(map[int]cyclone.Cyclone)

	for i := 0; i < runtime.NumCPU(); i++ {
		logrus.Printf("MAIN, Starting cyclone handler %d", i)
		cChan := make(chan *metric.Metric)
		cl := cyclone.Cyclone{
			Num:                 i,
			Input:               cChan,
			CfgRedisConnect:     conf.Redis.Connect,
			CfgRedisPassword:    conf.Redis.Password,
			CfgRedisDB:          conf.Redis.DB,
			CfgAlarmDestination: conf.Cyclone.DestinationURI,
			CfgLookupHost:       conf.Cyclone.LookupHost,
			CfgLookupPort:       conf.Cyclone.LookupPort,
			CfgLookupPath:       conf.Cyclone.LookupPath,
			CfgAPIVersion:       conf.Cyclone.APIVersion,
			TestMode:            conf.Cyclone.TestMode,
		}
		cl.SetLog(logrus.StandardLogger())
		handlers[i] = cl
		go cl.Run()
	}

	heartbeat := time.Tick(5 * time.Second)
	beatcount := 0

	ageCutOff := time.Duration(conf.Cyclone.MetricsMaxAge) * time.Minute * -1

runloop:
	for {
		select {
		case <-c:
			// SIGINT/SIGTERM
			break runloop
		case <-heartbeat:
			// 32bit time_t held 68years at one tick per second. This should
			// hold 2^32 * 5 * 68 years till overflow
			num := beatcount % runtime.NumCPU()
			beatcount++
			handlers[num].Input <- &metric.Metric{
				Path: `_internal.cyclone.heartbeat`,
			}
			continue runloop
		case e := <-consumer.Errors():
			logrus.Println(e)
		case message := <-consumer.Messages():
			if offsets[message.Topic] == nil {
				offsets[message.Topic] = make(map[int32]int64)
			}

			logrus.Printf("MAIN, Received topic:%s/partition:%d/offset:%d",
				message.Topic, message.Partition, message.Offset)

			eventCount++
			if offsets[message.Topic][message.Partition] != 0 &&
				offsets[message.Topic][message.Partition] != message.Offset-1 {
				logrus.Printf("MAIN ERROR, Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n",
					message.Topic, message.Partition,
					offsets[message.Topic][message.Partition]+1, message.Offset,
					message.Offset-offsets[message.Topic][message.Partition]+1,
				)
			}

			m, err := metric.FromBytes(message.Value)
			if err != nil {
				logrus.Printf("MAIN ERROR, Decoding metric data: %s\n", err)
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
				logrus.Println(`MAIN, Ignoring received metric`)
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			// ignore metrics that are simply too old for useful
			// alerting
			if time.Now().UTC().Add(ageCutOff).After(m.TS.UTC()) {
				logrus.Printf("MAIN ERROR, Skipping metric due to age: %s", m.TS.UTC().Format(time.RFC3339))
				offsets[message.Topic][message.Partition] = message.Offset
				consumer.CommitUpto(message)
				continue
			}

			handlers[int(m.AssetID)%runtime.NumCPU()].Input <- m

			offsets[message.Topic][message.Partition] = message.Offset
			consumer.CommitUpto(message)
		}
	}
	if err := consumer.Close(); err != nil {
		logrus.Println("Error closing the consumer", err)
	}

	// give handler routines a chance to finish their work
	time.Sleep(2 * time.Second)
	logrus.Printf("Processed %d events.", eventCount)
	logrus.Printf("%+v", offsets)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
