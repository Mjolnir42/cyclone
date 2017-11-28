/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/lib/cyclone"
import (
	"runtime"
	"time"

	"github.com/mjolnir42/cyclone/lib/cyclone/metric"
	"github.com/mjolnir42/erebos"
)

// Dispatch implements erebos.Dispatcher
func Dispatch(msg erebos.Transport) error {
	// decode embedded cyclone.Metric
	m, err := metric.FromBytes(msg.Value)
	if err != nil {
		return err
	}
	msg.HostID = int(m.AssetID)

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
		// mark as processed
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		return nil
	}

	// ignore metrics that are simply too old for useful
	// alerting
	if time.Now().UTC().Add(AgeCutOff).After(m.TS.UTC()) {
		// mark as processed
		msg.Commit <- &erebos.Commit{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		return nil
	}

	Handlers[msg.HostID%runtime.NumCPU()].InputChannel() <- &msg
	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
