// Copyright 2025 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"testing"
	"time"
)

func TestTx_TTLRescheduleThenPersistSurvivesOldDeadline(t *testing.T) {
	opts := DefaultOptions
	opts.TTLConfig.EnableTimingWheel = true
	opts.TTLConfig.WheelSlotDuration = 10 * time.Millisecond
	opts.TTLConfig.WheelSize = 60
	opts.TTLConfig.QueueSize = 100
	opts.TTLConfig.BatchSize = 1
	opts.TTLConfig.BatchTimeout = 20 * time.Millisecond

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "ttl-reschedule-persist"
		key := []byte("same-key")

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, []byte("v1"), 1, nil, nil)
		txPut(t, db, bucket, key, []byte("v2"), 10, nil, nil)
		txPersist(t, db, bucket, key, nil)

		time.Sleep(1300 * time.Millisecond)

		txGet(t, db, bucket, key, []byte("v2"), nil)
	})
}
