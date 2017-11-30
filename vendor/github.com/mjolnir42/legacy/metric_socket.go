/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import (
	"encoding/json"
	"net"
	"os"

	"github.com/mjolnir42/erebos"
	metrics "github.com/rcrowley/go-metrics"
)

// MetricSocket provides a socket on which clients can connect and receive a
// current metrics export in PluginMetricBatch format
type MetricSocket struct {
	Errors   chan error
	Shutdown chan struct{}
	death    chan error
	config   *erebos.Config
	registry *metrics.Registry
	format   Formatter
	fetching bool
	fetch    Fetcher
}

// Formatter is a function that will format the metrics Registry metrics
// into PluginMetricBatch. The returned function will be called via
// metrics.Registry.Each()
type Formatter func(*PluginMetricBatch) func(string, interface{})

// Fetcher is a function that will fill the metrics Registry with current
// values.
type Fetcher func() error

// NewFetchingMetricSocket returns a new MetricSocket that updates the
// metrics when called. To create the socket and start the listener, the
// Run() method must be called. Returns nil if the provided configuration
// contains an empty SocketPath
func NewFetchingMetricSocket(conf *erebos.Config, reg *metrics.Registry,
	death chan error, format Formatter, fetch Fetcher) *MetricSocket {
	if conf.Legacy.SocketPath == `` {
		return nil
	}

	s := MetricSocket{
		config:   conf,
		registry: reg,
		death:    death,
		format:   format,
		fetching: true,
		fetch:    fetch,
	}
	s.Errors = make(chan error)
	return &s
}

// NewMetricSocket returns a new MetricSocket. To create the socket and
// start the listener, the Run() method must be called. Returns nil if
// the provided configuration contains an empty SocketPath
func NewMetricSocket(conf *erebos.Config, reg *metrics.Registry,
	death chan error, format Formatter) *MetricSocket {
	if conf.Legacy.SocketPath == `` {
		return nil
	}

	s := MetricSocket{
		config:   conf,
		registry: reg,
		death:    death,
		format:   format,
	}
	s.Errors = make(chan error)
	return &s
}

// Run creates the socket, opens the listener and runs accept on incoming
// connections. If Run is called, the Errors channel must be read from or
// the MetricSocket will get stuck.
func (s *MetricSocket) Run() {
	sock, err := net.Listen(`unixpacket`, s.config.Legacy.SocketPath)
	if err != nil {
		s.death <- err
		return
	}
	// clean up socket file
	defer os.Remove(s.config.Legacy.SocketPath)

	connections := make(chan net.Conn)
	acceptStopped := make(chan struct{})

	// run Accept() on the socket in a goroutine and push the accepted
	// connections into a channel. this makes it possible to select{} on
	// them
	go func() {
	acceptloop:
		for {
			conn, e := sock.Accept()
			if e != nil {
				if op, ok := e.(*net.OpError); ok {
					// this error combination is generated if Accept() is
					// called on a closed socket, ie. inside the shutdown
					// path
					if op.Op == `accept` && op.Err.Error() == `use of closed network connection` {
						close(s.Shutdown)
						break acceptloop
					}
				}
				s.Errors <- e
				continue acceptloop
			}
			connections <- conn
		}
		close(acceptStopped)
	}()

runloop:
	for {
		select {
		case conn := <-connections:
			go s.handleConn(conn)
		case <-s.Shutdown:
			break runloop
		}
	}
	// close socket
	sock.Close()
	// wait for acceptloop to terminate
	<-acceptStopped
}

// handleConn processes a client connection
func (s *MetricSocket) handleConn(conn net.Conn) {
	defer conn.Close()

	msg, err := s.metrics()
	if err != nil {
		s.Errors <- err
		return
	}

	_, err = conn.Write(msg)
	if err != nil {
		s.Errors <- err
	}
}

// metrics returns the current metrics as []byte with a json marshalled
// PluginMetricBatch inside
func (s *MetricSocket) metrics() ([]byte, error) {
	if s.fetching {
		if err := s.fetch(); err != nil {
			return nil, err
		}
	}
	m := PluginMetricBatch{
		Metrics: []PluginMetric{},
	}
	(*s.registry).Each(s.format(&m))
	return json.Marshal(&m)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
