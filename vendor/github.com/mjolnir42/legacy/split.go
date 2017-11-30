/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import "sync"

// Split breaks up a MetricBatch into a slice of individual
// MetricSplit message
func (m *MetricBatch) Split() []MetricSplit {
	res := []MetricSplit{}
	for iter := range m.Data {
		// size the channel the split up structs will be put into
		length := len(m.Data[iter].FloatMetrics) +
			len(m.Data[iter].StringMetrics) +
			len(m.Data[iter].IntMetrics)
		collect := make(chan MetricSplit, length)

		// collector go routine that will run indefinitly until the
		// channel is closed
		cwg := sync.WaitGroup{}
		cwg.Add(1)
		go func(c chan MetricSplit) {
			for elem := range c {
				res = append(res, elem)
			}
			cwg.Done()
		}(collect)

		// feeder go routines that will convert the slices and push
		// the result into channel collect
		wg := sync.WaitGroup{}
		// convert float metrics
		wg.Add(1)
		go func(c chan MetricSplit) {
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, fMetric := range m.Data[iter].FloatMetrics {
				split.Type = `real`
				split.Path = fMetric.Metric
				split.Val.FlpVal = fMetric.Value
				split.Tags = []string{fMetric.Subtype}
				c <- split
			}
			wg.Done()
		}(collect)

		// convert string metrics
		wg.Add(1)
		go func(c chan MetricSplit) {
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, sMetric := range m.Data[iter].StringMetrics {
				split.Type = `string`
				split.Path = sMetric.Metric
				split.Val.StrVal = sMetric.Value
				split.Tags = []string{sMetric.Subtype}
				c <- split
			}
			wg.Done()
		}(collect)

		// convert integer metrics
		wg.Add(1)
		go func(c chan MetricSplit) {
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, iMetric := range m.Data[iter].IntMetrics {
				split.Type = `integer`
				split.Path = iMetric.Metric
				split.Val.IntVal = iMetric.Value
				split.Tags = []string{iMetric.Subtype}
				c <- split
			}
			wg.Done()
		}(collect)
		// wait for feeder
		wg.Wait()
		// close collect channel to unblock the collector
		close(collect)
		// wait for collector
		cwg.Wait()
	}
	return res
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
