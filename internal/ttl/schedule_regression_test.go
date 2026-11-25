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

package ttl

import (
	"testing"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRegressionWheelManager(t *testing.T, start bool) *TimingWheelManager {
	t.Helper()

	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 10 * time.Millisecond,
		WheelSize:         60,
		QueueSize:         100,
	}
	queue := newExpirationQueue(config.QueueSize)
	manager := NewTimingWheelManager(config, queue)
	if start {
		require.NoError(t, manager.Start(t.Context()))
		t.Cleanup(func() {
			_ = manager.Stop(time.Second)
		})
	}
	return manager
}

func TestTimingWheelManagerRescheduleReplacesSingleTimer(t *testing.T) {
	manager := newRegressionWheelManager(t, false)

	const bucketID = uint64(1)
	key := []byte("reschedule-key")
	keyHash := hashKey(key)
	registryKey := makeRegistryKey(bucketID, keyHash)

	require.True(t, manager.RegisterKey(bucketID, key, DataStructureBTree, 1, 100))
	firstValue, ok := manager.registry.timers.Load(registryKey)
	require.True(t, ok)
	firstTimer := firstValue.(*timingwheel.Timer)

	require.True(t, manager.ApplyKeyExpiration(bucketID, key, DataStructureBTree, 10, 200))

	metrics := manager.GetMetrics()
	assert.Equal(t, int64(1), metrics.RegisteredKeys)

	secondValue, ok := manager.registry.timers.Load(registryKey)
	require.True(t, ok)
	secondTimer := secondValue.(*timingwheel.Timer)
	assert.NotSame(t, firstTimer, secondTimer)
	assert.False(t, firstTimer.Stop(), "superseded timer must already be stopped")

	assert.True(t, manager.DeregisterKey(bucketID, key))
	assert.Equal(t, int64(0), manager.GetMetrics().RegisteredKeys)
	_, exists := manager.keyCache.Load(keyHash)
	assert.False(t, exists)
}

func TestTimingWheelManagerApplyKeyExpirationCancel(t *testing.T) {
	manager := newRegressionWheelManager(t, false)

	const bucketID = uint64(1)
	key := []byte("cancel-key")

	assert.True(t, manager.ApplyKeyExpiration(bucketID, key, DataStructureBTree, 1, 100))
	assert.Equal(t, int64(1), manager.GetMetrics().RegisteredKeys)

	assert.True(t, manager.ApplyKeyExpiration(bucketID, key, DataStructureBTree, Persistent, 0))
	assert.Equal(t, int64(0), manager.GetMetrics().RegisteredKeys)
	assert.False(t, manager.ApplyKeyExpiration(bucketID, key, DataStructureBTree, Persistent, 0))
}

func TestTimingWheelManagerRescheduleSkipsOldExpirationPoint(t *testing.T) {
	manager := newRegressionWheelManager(t, true)

	const bucketID = uint64(1)
	key := []byte("old-deadline-key")

	require.True(t, manager.RegisterKey(bucketID, key, DataStructureBTree, 1, 100))
	require.True(t, manager.RegisterKey(bucketID, key, DataStructureBTree, 10, 200))

	time.Sleep(1150 * time.Millisecond)

	select {
	case event := <-manager.queue.events:
		t.Fatalf("superseded expiration schedule produced event: %+v", event)
	default:
	}

	assert.Equal(t, int64(1), manager.GetMetrics().RegisteredKeys)
	assert.True(t, manager.ApplyKeyExpiration(bucketID, key, DataStructureBTree, Persistent, 0))
	assert.Equal(t, int64(0), manager.GetMetrics().RegisteredKeys)
}
