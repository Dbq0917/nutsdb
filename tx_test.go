// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/fileio"
	"github.com/stretchr/testify/assert"
)

// todo to check is there any deadlock here?
func TestTx_Rollback(t *testing.T) {

	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_rollback_test"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		tx, err := db.Begin(true)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			key := []byte("key_" + fmt.Sprintf("%03d", i))
			val := []byte("val_" + fmt.Sprintf("%03d", i))
			if i == 7 {
				key = []byte("") // set error key to make tx rollback
			}
			if err = tx.Put(bucket, key, val, Persistent); err != nil {
				// tx rollback
				_ = tx.Rollback()

				if i < 7 {
					t.Fatal("err TestTx_Rollback")
				}
			}
		}

		// no one found
		for i := 0; i <= 10; i++ {
			tx, err = db.Begin(false)
			assert.NoError(t, err)

			key := []byte("key_" + fmt.Sprintf("%03d", i))
			if _, err := tx.Get(bucket, key); err != nil {
				// tx rollback
				_ = tx.Rollback()
			} else {
				t.Fatal("err TestTx_Rollback")
			}
		}

	})
}

type failOnceReleaseManager struct {
	fileio.RWManager
	failed bool
}

func (m *failOnceReleaseManager) Release() error {
	if !m.failed {
		m.failed = true
		return fmt.Errorf("injected segment rotation failure")
	}
	return m.RWManager.Release()
}

func TestTx_CommitKeepsBatchInOneSegment(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-segment-batch")
	opts.SegmentSize = 4 * KB
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	db, err := Open(opts)
	assert.NoError(t, err)
	defer func() {
		if db != nil && !db.IsClose() {
			assert.NoError(t, db.Close())
		}
	}()

	bucket := "segment_batch"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	err = db.Update(func(tx *Tx) error {
		return tx.Put(bucket, []byte("filler"), make([]byte, 3000), Persistent)
	})
	assert.NoError(t, err)

	startFileID := db.ActiveFile.fileID
	startOffset := db.ActiveFile.ActualSize

	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 30; i++ {
			key := []byte(fmt.Sprintf("batch-%02d", i))
			if err := tx.Put(bucket, key, []byte("01234567890123456789"), Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)

	assert.Greater(t, db.ActiveFile.fileID, startFileID)
	assert.Greater(t, startOffset, int64(0))

	err = db.View(func(tx *Tx) error {
		_, b := tx.getBucketAndItsStatus(DataStructureBTree, bucket)
		idx, ok := tx.db.Index.BTree.exist(b.Id)
		if !ok {
			return fmt.Errorf("bucket index not found")
		}
		for i := 0; i < 30; i++ {
			record, found := idx.Find([]byte(fmt.Sprintf("batch-%02d", i)))
			if !found {
				return fmt.Errorf("batch-%02d not found", i)
			}
			if record.FileID != db.ActiveFile.fileID {
				return fmt.Errorf("batch-%02d stored in file %d, want %d", i, record.FileID, db.ActiveFile.fileID)
			}
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestTx_CommitFailureRollsBackWholeBatchAcrossSegments(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-segment-rollback")
	opts.SegmentSize = 2 * KB
	opts.EntryIdxMode = HintKeyAndRAMIdxMode

	db, err := Open(opts)
	assert.NoError(t, err)
	defer func() {
		if db != nil && !db.IsClose() {
			assert.NoError(t, db.Close())
		}
	}()

	bucket := "segment_rollback"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	err = db.Update(func(tx *Tx) error {
		return tx.Put(bucket, []byte("filler"), make([]byte, 1500), Persistent)
	})
	assert.NoError(t, err)

	startFileID := db.ActiveFile.fileID
	startOffset := db.ActiveFile.ActualSize

	tx, err := db.Begin(true)
	assert.NoError(t, err)
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("batch-%02d", i))
		err = tx.Put(bucket, key, make([]byte, 64), Persistent)
		assert.NoError(t, err)
	}

	realManager := db.ActiveFile.rwManager
	db.ActiveFile.rwManager = &failOnceReleaseManager{RWManager: realManager}

	err = tx.Commit()
	assert.Error(t, err)
	assert.Equal(t, startFileID, db.ActiveFile.fileID)
	assert.Equal(t, startOffset, db.ActiveFile.ActualSize)
	assert.Equal(t, startFileID, db.MaxFileID)

	err = db.View(func(tx *Tx) error {
		for i := 0; i < 30; i++ {
			key := []byte(fmt.Sprintf("batch-%02d", i))
			if _, err := tx.Get(bucket, key); err == nil {
				return fmt.Errorf("%s survived a failed commit", key)
			}
		}
		if _, err := tx.RangeScan(bucket, []byte("batch-00"), []byte("batch-99")); err == nil {
			return fmt.Errorf("range scan found entries from a failed commit")
		}
		return nil
	})
	assert.NoError(t, err)

	err = db.Update(func(tx *Tx) error {
		return tx.Put(bucket, []byte("after"), []byte("visible"), Persistent)
	})
	assert.NoError(t, err)

	assert.NoError(t, db.Close())
	db, err = Open(opts)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		value, err := tx.Get(bucket, []byte("filler"))
		if err != nil {
			return err
		}
		if len(value) != 1500 {
			return fmt.Errorf("filler value length = %d, want 1500", len(value))
		}
		value, err = tx.Get(bucket, []byte("after"))
		if err != nil {
			return err
		}
		if string(value) != "visible" {
			return fmt.Errorf("after = %q, want visible", value)
		}
		for i := 0; i < 30; i++ {
			key := []byte(fmt.Sprintf("batch-%02d", i))
			if _, err := tx.Get(bucket, key); err == nil {
				return fmt.Errorf("%s survived reopen after a failed commit", key)
			}
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestRecoveryDoesNotCommitPartialTxFromPreviousSegment(t *testing.T) {
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-segment-recovery")
	opts.SegmentSize = KB

	db, err := Open(opts)
	assert.NoError(t, err)
	defer func() {
		if db != nil && !db.IsClose() {
			assert.NoError(t, db.Close())
		}
	}()

	bucket := "segment_recovery"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	bucketMeta, err := db.bucketMgr.GetBucket(DataStructureBTree, bucket)
	assert.NoError(t, err)
	bucketID := bucketMeta.Id
	assert.NoError(t, db.Close())

	encodeEntry := func(txID uint64, key, value string, status uint16) []byte {
		meta := core.NewMetaData().
			WithTimeStamp(1).
			WithKeySize(uint32(len(key))).
			WithValueSize(uint32(len(value))).
			WithFlag(DataSetFlag).
			WithTTL(Persistent).
			WithStatus(status).
			WithDs(DataStructureBTree).
			WithTxID(txID).
			WithBucketId(bucketID)
		return core.NewEntry().WithKey([]byte(key)).WithValue([]byte(value)).WithMeta(meta).Encode()
	}

	partial := encodeEntry(101, "partial-0", "v0", UnCommitted)
	partial = append(partial, encodeEntry(101, "partial-1", "v1", UnCommitted)...)
	assert.NoError(t, os.WriteFile(getDataPath(0, opts.Dir), partial, 0o644))

	committed := encodeEntry(202, "committed", "ok", Committed)
	assert.NoError(t, os.WriteFile(getDataPath(1, opts.Dir), committed, 0o644))

	db, err = Open(opts)
	assert.NoError(t, err)

	err = db.View(func(tx *Tx) error {
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("partial-%d", i))
			if _, err := tx.Get(bucket, key); err == nil {
				return fmt.Errorf("%s from an incomplete transaction was recovered", key)
			}
		}
		value, err := tx.Get(bucket, []byte("committed"))
		if err != nil {
			return err
		}
		if string(value) != "ok" {
			return fmt.Errorf("committed = %q, want ok", value)
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestTx_Begin(t *testing.T) {
	t.Run("Begin with default options, with only read", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(false)
			assert.NoError(t, err)

			err = tx.Rollback()
			assert.NoError(t, err)

			err = db.Close()
			assert.NoError(t, err)
		})
	})

	t.Run("Begin with default options, with writable", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			_ = tx.Rollback()

			err = db.Close()
			assert.NoError(t, err)
		})
	})

	t.Run("Begin with error: begin the closed db", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			_ = tx.Rollback() // for unlock mutex

			err = db.Close()
			assert.NoError(t, err)

			_, err = db.Begin(false)
			assert.Error(t, err)
		})
	})
}

func TestTx_Close(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		tx, err := db.Begin(false)
		assert.NoError(t, err)

		err = tx.Rollback()
		assert.NoError(t, err)

		bucket := "bucket_tx_close_test"

		_, err = tx.Get(bucket, []byte("foo"))
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.RangeScan(bucket, []byte("foo0"), []byte("foo1"))
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.PrefixScan(bucket, []byte("foo"), 0, 1)
		assert.Errorf(t, err, "err TestTx_Close")

		_, err = tx.PrefixSearchScan(bucket, []byte("f"), "oo", 0, 1)
		assert.Errorf(t, err, "err TestTx_Close")
	})
}

func TestTx_CommittedStatus(t *testing.T) {

	withRAMIdxDB(t, func(t *testing.T, db *DB) {

		bucket := "bucket_committed_status"

		{ // setup data
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			tx, err := db.Begin(true)
			assert.NoError(t, err)

			err = tx.Put(bucket, []byte("key1"), []byte("value1"), 0)
			assert.NoError(t, err)

			err = tx.Put(bucket, []byte("key2"), []byte("value2"), 0)
			assert.NoError(t, err)

			err = tx.Commit()
			assert.NoError(t, err)
		}
	})
}

func TestTx_PutWithTimestamp(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "bucket_put_with_timestamp"

		timestamps := []uint64{1547707905, 1547707910, uint64(time.Now().Unix())}

		{ // put with timestamp
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)

			tx, err := db.Begin(true)
			assert.NoError(t, err)
			for i, timestamp := range timestamps {
				key := []byte("key_" + fmt.Sprintf("%03d", i))
				val := []byte("val_" + fmt.Sprintf("%03d", i))

				err = tx.PutWithTimestamp(bucket, key, val, 0, timestamp)
				assert.NoError(t, err)

			}
			err = tx.Commit()
			assert.NoError(t, err)
		}
	})
}

func TestTx_Commit(t *testing.T) {
	t.Run("build_bucket_indexes_after_commit", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			bucket := "bucket1"

			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			assert.Equal(t, 1, db.Index.BTree.getIdxLen())
			txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
			assert.Equal(t, 0, db.Index.BTree.getIdxLen())

			txCreateBucket(t, db, DataStructureSortedSet, bucket, nil)
			assert.Equal(t, 1, db.Index.SortedSet.getIdxLen())
			txDeleteBucket(t, db, DataStructureSortedSet, bucket, nil)
			assert.Equal(t, 0, db.Index.SortedSet.getIdxLen())

			txCreateBucket(t, db, DataStructureList, bucket, nil)
			assert.Equal(t, 1, db.Index.List.getIdxLen())
			txDeleteBucket(t, db, DataStructureList, bucket, nil)
			assert.Equal(t, 0, db.Index.List.getIdxLen())

			txCreateBucket(t, db, DataStructureSet, bucket, nil)
			assert.Equal(t, 1, db.Index.Set.getIdxLen())
			txDeleteBucket(t, db, DataStructureSet, bucket, nil)
			assert.Equal(t, 0, db.Index.Set.getIdxLen())

		})
	})

}

func TestRunTxnCallback(t *testing.T) {
	t.Run("nil callback panics", func(t *testing.T) {
		assert.Panics(t, func() {
			runTxnCallback(nil)
		})
	})

	t.Run("nil user callback panics", func(t *testing.T) {
		assert.Panics(t, func() {
			runTxnCallback(&txnCb{user: nil, commit: nil})
		})
	})

	t.Run("error in callback", func(t *testing.T) {
		testErr := fmt.Errorf("test error")
		userCalled := false
		var receivedErr error
		runTxnCallback(&txnCb{
			user: func(err error) {
				userCalled = true
				receivedErr = err
			},
			err: testErr,
		})
		assert.True(t, userCalled)
		assert.Equal(t, testErr, receivedErr)
	})

	t.Run("commit callback with error", func(t *testing.T) {
		testErr := fmt.Errorf("commit error")
		userCalled := false
		var receivedErr error
		runTxnCallback(&txnCb{
			user: func(err error) {
				userCalled = true
				receivedErr = err
			},
			commit: func() error {
				return testErr
			},
		})
		assert.True(t, userCalled)
		assert.Equal(t, testErr, receivedErr)
	})

	t.Run("commit callback success", func(t *testing.T) {
		userCalled := false
		var receivedErr error
		runTxnCallback(&txnCb{
			user: func(err error) {
				userCalled = true
				receivedErr = err
			},
			commit: func() error {
				return nil
			},
		})
		assert.True(t, userCalled)
		assert.Nil(t, receivedErr)
	})

	t.Run("default case with nil commit and nil error", func(t *testing.T) {
		userCalled := false
		var receivedErr error
		runTxnCallback(&txnCb{
			user:   func(err error) { userCalled = true; receivedErr = err },
			err:    nil,
			commit: nil,
		})
		assert.True(t, userCalled)
		assert.Nil(t, receivedErr)
	})
}

func TestTx_CommitWith_PendingWrites(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		tx, err := db.Begin(true)
		assert.NoError(t, err)

		err = tx.Put(bucket, []byte("key1"), []byte("value1"), 0)
		assert.NoError(t, err)

		done := make(chan error, 1)
		tx.CommitWith(func(err error) {
			done <- err
		})

		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for CommitWith callback")
		}

		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(bucket, []byte("key1"))
			if err != nil {
				return err
			}
			assert.Equal(t, []byte("value1"), v)
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestTx_CommitWith_NoPendingWrites(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		tx, err := db.Begin(false)
		assert.NoError(t, err)

		done := make(chan error, 1)
		tx.CommitWith(func(err error) {
			done <- err
		})

		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for CommitWith callback")
		}

		err = db.View(func(tx *Tx) error { return nil })
		assert.NoError(t, err)
	})
}

func TestTx_CommitWith_NoPendingWrites_ReleasesReadLock(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		tx, err := db.Begin(false)
		assert.NoError(t, err)

		cbDone := make(chan error, 1)
		tx.CommitWith(func(err error) {
			cbDone <- err
		})

		select {
		case err := <-cbDone:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for CommitWith callback")
		}

		updateDone := make(chan error, 1)
		go func() {
			updateDone <- db.Update(func(tx *Tx) error { return nil })
		}()

		select {
		case err := <-updateDone:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Update after CommitWith")
		}
	})
}

func TestTx_CommitWith_PendingWrites_DoesNotDeadlockOtherWriters(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		bucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		tx, err := db.Begin(true)
		assert.NoError(t, err)

		err = tx.Put(bucket, []byte("key1"), []byte("value1"), 0)
		assert.NoError(t, err)

		cbDone := make(chan error, 1)
		tx.CommitWith(func(err error) {
			cbDone <- err
		})

		otherDone := make(chan error, 1)
		go func() {
			otherDone <- db.Update(func(tx *Tx) error {
				return tx.Put(bucket, []byte("key2"), []byte("value2"), 0)
			})
		}()

		select {
		case err := <-cbDone:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for CommitWith callback")
		}

		select {
		case err := <-otherDone:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for concurrent Update after CommitWith")
		}

		err = db.View(func(tx *Tx) error {
			v1, err := tx.Get(bucket, []byte("key1"))
			if err != nil {
				return err
			}
			assert.Equal(t, []byte("value1"), v1)

			v2, err := tx.Get(bucket, []byte("key2"))
			if err != nil {
				return err
			}
			assert.Equal(t, []byte("value2"), v2)
			return nil
		})
		assert.NoError(t, err)
	})
}

func TestTx_allocCommitBuffer(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		tx, err := db.Begin(true)
		assert.NoError(t, err)

		t.Run("small transaction uses shared buffer", func(t *testing.T) {
			tx.size = 100 // Less than CommitBufferSize
			buff := tx.allocCommitBuffer()
			assert.Equal(t, db.commitBuffer, buff)
		})

		t.Run("large transaction creates new buffer", func(t *testing.T) {
			// Set size larger than CommitBufferSize
			tx.size = int64(db.opt.CommitBufferSize) + 1000
			buff := tx.allocCommitBuffer()
			// Use pointer comparison since assert.NotEqual uses reflect.DeepEqual
			// which compares buffer contents, not pointers
			assert.True(t, buff != db.commitBuffer, "expected new buffer for large transaction")
			assert.NotNil(t, buff)
			// buffer capacity should be at least tx.size (Grow may overallocate)
			assert.GreaterOrEqual(t, buff.Cap(), int(tx.size))
		})

		_ = tx.Rollback()
	})
}

func TestTx_putBucket(t *testing.T) {
	withDefaultDB(t, func(t *testing.T, db *DB) {
		tx, err := db.Begin(true)
		assert.NoError(t, err)

		// Create a new bucket using the same structure as recovery_reader_test.go
		bucket := &core.Bucket{
			Meta: &core.BucketMeta{},
			Id:   db.bucketMgr.Gen.GenId(),
			Ds:   core.DataStructureBTree,
			Name: "new_bucket",
		}

		t.Run("put bucket in pending list", func(t *testing.T) {
			err := tx.putBucket(bucket)
			assert.NoError(t, err)

			// Verify bucket is in pending list
			assert.Contains(t, tx.pendingBucketList[core.DataStructureBTree], bucket.Name)
			assert.Equal(t, bucket, tx.pendingBucketList[core.DataStructureBTree][bucket.Name])
		})

		t.Run("put multiple buckets in same DS", func(t *testing.T) {
			bucket2 := &core.Bucket{
				Meta: &core.BucketMeta{},
				Id:   db.bucketMgr.Gen.GenId(),
				Ds:   core.DataStructureBTree,
				Name: "new_bucket2",
			}

			err := tx.putBucket(bucket2)
			assert.NoError(t, err)

			// Verify both buckets are in pending list
			assert.Len(t, tx.pendingBucketList[core.DataStructureBTree], 2)
			assert.Contains(t, tx.pendingBucketList[core.DataStructureBTree], bucket.Name)
			assert.Contains(t, tx.pendingBucketList[core.DataStructureBTree], bucket2.Name)
		})

		t.Run("put bucket in different DS", func(t *testing.T) {
			bucket3 := &core.Bucket{
				Meta: &core.BucketMeta{},
				Id:   db.bucketMgr.Gen.GenId(),
				Ds:   core.DataStructureSortedSet,
				Name: "new_bucket3",
			}

			err := tx.putBucket(bucket3)
			assert.NoError(t, err)

			// Verify bucket is in sorted set pending list
			assert.Contains(t, tx.pendingBucketList[core.DataStructureSortedSet], bucket3.Name)
			assert.Len(t, tx.pendingBucketList[core.DataStructureSortedSet], 1)
		})

		_ = tx.Rollback()
	})
}
