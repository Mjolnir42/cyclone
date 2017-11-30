/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import "encoding/json"

// metricBatchPeek is a helper struct to unmarshal the HostID of
// a MetricBatch
type metricBatchPeek struct {
	HostID int `json:"host_id"`
}

// PeekHostID returns the HostID from a MetricBatch
func PeekHostID(data []byte) (int, error) {
	peek := metricBatchPeek{}
	if err := json.Unmarshal(data, &peek); err != nil {
		return 0, err
	}
	return peek.HostID, nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
