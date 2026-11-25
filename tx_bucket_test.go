// Copyright 2022 The nutsdb Author. All rights reserved.
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

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	setBucketName    = "set_bucket"
	zSetBucketName   = "zset_bucket"
	listBucketName   = "list_bucket"
	stringBucketName = "string_bucket"
)

func setupBucket(t *testing.T, db *DB) {
	key := testutils.GetTestBytes(0)
	val := testutils.GetTestBytes(1)
	txCreateBucket(t, db, DataStructureBTree, stringBucketName, nil)
	txCreateBucket(t, db, DataStructureSet, setBucketName, nil)
	txCreateBucket(t, db, DataStructureSortedSet, zSetBucketName, nil)
	txCreateBucket(t, db, DataStructureList, listBucketName, nil)

	txSAdd(t, db, setBucketName, key, val, nil, nil)
	txZAdd(t, db, zSetBucketName, key, val, 80, nil, nil)
	txPush(t, db, listBucketName, key, val, true, nil, nil)
	txPut(t, db, stringBucketName, key, val, Persistent, nil, nil)
}

func TestBucket_IterateBuckets(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txIterateBuckets(t, db, DataStructureSet, "*", func(bucket string) bool {
			return true
		}, nil, setBucketName)

		txIterateBuckets(t, db, DataStructureSortedSet, "*", func(bucket string) bool {
			return true
		}, nil, zSetBucketName)

		txIterateBuckets(t, db, DataStructureList, "*", func(bucket string) bool {
			return true
		}, nil, listBucketName)

		txIterateBuckets(t, db, DataStructureBTree, "*", func(bucket string) bool {
			return true
		}, nil, stringBucketName)

		matched := false
		txIterateBuckets(t, db, DataStructureBTree, "str*", func(bucket string) bool {
			matched = true
			return true
		}, nil)
		assert.Equal(t, true, matched)

		matched = false
		txIterateBuckets(t, db, DataStructureList, "str*", func(bucket string) bool {
			return true
		}, nil)
		assert.Equal(t, false, matched)

		// delete a bucket in the middle, the remaining buckets should still
		// be iterated in the same order as before the deletion
		orderedBuckets := []string{"set_bucket_1", "set_bucket_2", "set_bucket_3", "set_bucket_4", "set_bucket_5"}
		for _, name := range orderedBuckets {
			txCreateBucket(t, db, DataStructureSet, name, nil)
		}

		deletedId, err := db.bucketMgr.GetBucketID(DataStructureSet, "set_bucket_3")
		assert.NoError(t, err)
		txDeleteBucket(t, db, DataStructureSet, "set_bucket_3", nil)

		// the deleted bucket id should be removed from the index as well
		_, exist := db.Index.Set.Idx[deletedId]
		assert.False(t, exist)

		var iterated []string
		err = db.View(func(tx *Tx) error {
			return tx.IterateBuckets(DataStructureSet, "set_bucket_*", func(bucket string) bool {
				iterated = append(iterated, bucket)
				return true
			})
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"set_bucket_1", "set_bucket_2", "set_bucket_4", "set_bucket_5"}, iterated)
	})
}

func TestBucket_IterateBucketsWithStaleBucketId(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		orderedBuckets := []string{"set_bucket_1", "set_bucket_2", "set_bucket_3"}
		for _, name := range orderedBuckets {
			txCreateBucket(t, db, DataStructureSet, name, nil)
		}

		// delete the middle bucket, then put its id back into the index to
		// simulate a stale index entry whose bucket metadata is already gone
		deletedId, err := db.bucketMgr.GetBucketID(DataStructureSet, "set_bucket_2")
		assert.NoError(t, err)
		txDeleteBucket(t, db, DataStructureSet, "set_bucket_2", nil)
		db.Index.Set.GetWithDefault(deletedId)

		// the stale bucket id should be skipped without aborting the iteration,
		// and the buckets after it should still be visited in order
		var iterated []string
		err = db.View(func(tx *Tx) error {
			return tx.IterateBuckets(DataStructureSet, "set_bucket_*", func(bucket string) bool {
				iterated = append(iterated, bucket)
				return true
			})
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"set_bucket_1", "set_bucket_3"}, iterated)
	})
}

func TestBucket_DeleteBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		setupBucket(t, db)

		txDeleteBucket(t, db, DataStructureSet, setBucketName, nil)
		txDeleteBucket(t, db, DataStructureSortedSet, zSetBucketName, nil)
		txDeleteBucket(t, db, DataStructureList, listBucketName, nil)
		txDeleteBucket(t, db, DataStructureBTree, stringBucketName, nil)

	})
}
