/*-
 * Copyright © 2016,2017 Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/mjolnir42/cyclone/cmd/cyclone"

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/client9/reopen"
	"github.com/mjolnir42/cyclone/lib/cyclone"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	"github.com/rcrowley/go-metrics"
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
	flag.StringVar(&configFlag, `config`, `cyclone.conf`,
		`Configuration file location`)
	flag.BoolVar(&versionFlag, `version`, false,
		`Print version information`)
	flag.Parse()

	// only provide version information if --version was specified
	if versionFlag {
		fmt.Fprintln(os.Stderr, `Cyclone Metric Monitoring System`)
		fmt.Fprintf(os.Stderr, "Version  : %s-%s\n", builddate,
			shorthash)
		fmt.Fprintf(os.Stderr, "Git Hash : %s\n", githash)
		fmt.Fprintf(os.Stderr, "Timestamp: %s\n", buildtime)
		os.Exit(0)
	}

	// read runtime configuration
	cyConf := erebos.Config{}
	if err = cyConf.FromFile(configFlag); err != nil {
		logrus.Fatalf("Could not open configuration: %s", err)
	}

	// setup logfile
	if logFH, err = reopen.NewFileWriter(
		filepath.Join(cyConf.Log.Path, cyConf.Log.File),
	); err != nil {
		logrus.Fatalf("Unable to open logfile: %s", err)
	} else {
		cyConf.Log.FH = logFH
	}
	logrus.SetOutput(cyConf.Log.FH)
	logrus.Infoln(`Starting CYCLONE...`)

	// switch to requested loglevel
	if cyConf.Log.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.WarnLevel)
	}

	// signal handler will reopen logfile on USR2 if requested
	if cyConf.Log.Rotate {
		sigChanLogRotate := make(chan os.Signal, 1)
		signal.Notify(sigChanLogRotate, syscall.SIGUSR2)
		go erebos.Logrotate(sigChanLogRotate, cyConf)
	}

	// setup signal receiver for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// this channel is used by the handlers on error
	handlerDeath := make(chan error)
	// this channel is used to signal the consumer to stop
	consumerShutdown := make(chan struct{})
	// this channel will be closed by the consumer
	consumerExit := make(chan struct{})

	// setup goroutine waiting policy
	waitdelay := delay.NewDelay()

	// setup metrics
	var metricPrefix string
	switch cyConf.Misc.InstanceName {
	case ``:
		metricPrefix = `/cyclone`
	default:
		metricPrefix = fmt.Sprintf("/cyclone/%s",
			cyConf.Misc.InstanceName)
	}
	pfxRegistry := metrics.NewPrefixedRegistry(metricPrefix)
	metrics.NewRegisteredMeter(`/metrics/consumed.per.second`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`/metrics/processed.per.second`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`/evaluations.per.second`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`/alarms.per.second`,
		pfxRegistry)

	// start metric socket
	ms := legacy.NewMetricSocket(&cyConf, &pfxRegistry, handlerDeath,
		cyclone.FormatMetrics)
	if cyConf.Misc.ProduceMetrics {
		logrus.Info(`Launched metrics producer socket`)
		waitdelay.Use()
		go func() {
			defer waitdelay.Done()
			ms.Run()
		}()
	}

	cyclone.AgeCutOff = time.Duration(
		cyConf.Cyclone.MetricsMaxAge,
	) * time.Minute * -1

	for i := 0; i < runtime.NumCPU(); i++ {
		h := cyclone.Cyclone{
			Num: i,
			Input: make(chan *erebos.Transport,
				cyConf.Cyclone.HandlerQueueLength),
			Shutdown: make(chan struct{}),
			Death:    handlerDeath,
			Config:   &cyConf,
			Metrics:  &pfxRegistry,
		}
		cyclone.Handlers[i] = &h
		waitdelay.Use()
		go func() {
			defer waitdelay.Done()
			h.Start()
		}()
		logrus.Infof("Launched Cyclone handler #%d", i)
	}

	// start kafka consumer
	waitdelay.Use()
	go func() {
		defer waitdelay.Done()
		erebos.Consumer(
			&conf,
			wrappedDispatch(&pfxRegistry, cyclone.Dispatch),
			consumerShutdown,
			consumerExit,
			handlerDeath,
		)
	}()

	heartbeat := time.Tick(5 * time.Second)
	beatcount := 0

	// the main loop
	fault := false
runloop:
	for {
		select {
		case err := <-ms.Errors:
			logrus.Errorf("Socket error: %s", err.Error())
		case <-c:
			logrus.Infoln(`Received shutdown signal`)
			break runloop
		case err := <-handlerDeath:
			logrus.Errorf("Handler died: %s", err.Error())
			fault = true
			break runloop
		case <-heartbeat:
			// 32bit time_t held 68years at one tick per second. This
			// should hold 2^32 * 5 * 68 years till overflow
			cyclone.Handlers[beatcount%runtime.NumCPU()].
				InputChannel() <- newHeartbeat()
			beatcount++
		}
	}

	// close all handlers
	close(ms.Shutdown)
	close(consumerShutdown)
	<-consumerExit // not safe to close InputChannel before consumer is gone
	for i := range cyclone.Handlers {
		close(cyclone.Handlers[i].ShutdownChannel())
		close(cyclone.Handlers[i].InputChannel())
	}

	// read all additional handler errors if required
drainloop:
	for {
		select {
		case err := <-ms.Errors:
			logrus.Errorf("Socket error: %s", err.Error())
		case err := <-handlerDeath:
			logrus.Errorf("Handler died: %s", err.Error())
		case <-time.After(time.Millisecond * 10):
			break drainloop
		}
	}

	// give goroutines that were blocked on handlerDeath channel
	// a chance to exit
	waitdelay.Wait()
	logrus.Infoln(`CYCLONE shutdown complete`)
	if fault {
		os.Exit(1)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
