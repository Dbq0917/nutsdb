package nutsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentRolloverKeepsRecordOffsets(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		for _, perCommit := range []bool{false, true} {
			name := fmt.Sprintf("mode-%d/per-commit-%t", mode, perCommit)
			t.Run(name, func(t *testing.T) {
				testSegmentRolloverKeepsRecordOffsets(t, mode, perCommit)
			})
		}
	}
}

func TestSegmentRolloverKeepsRecordOffsetsWithWriteBatch(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			opts := DefaultOptions
			opts.SegmentSize = 512
			opts.EntryIdxMode = mode
			opts.MaxBatchCount = 8

			runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
				bucket := "orders"
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)

				const count = 200
				keys, values := orderKeysAndValues(count)
				wb, err := db.NewWriteBatch()
				require.NoError(t, err)
				for i := 0; i < count; i++ {
					require.NoError(t, wb.Put(bucket, keys[i], values[i], Persistent))
				}
				require.NoError(t, wb.Flush())
				require.Greater(t, db.MaxFileID, int64(0))

				assertOrdersReadable(t, db, bucket, keys, values)
				require.NoError(t, db.Close())

				var openErr error
				db, openErr = Open(opts)
				require.NoError(t, openErr)
				assertOrdersReadable(t, db, bucket, keys, values)
			})
		})
	}
}

func TestSegmentRolloverKeepsRecordOffsetsAfterHintFileRecovery(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			testSegmentRolloverKeepsRecordOffsetsAfterHintFileRecovery(t, mode)
		})
	}
}

func testSegmentRolloverKeepsRecordOffsetsAfterHintFileRecovery(t *testing.T, mode EntryIdxMode) {
	opts := DefaultOptions
	opts.SegmentSize = 512
	opts.EntryIdxMode = mode
	opts.EnableHintFile = true

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "orders"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		const initial = 100
		keys, values := orderKeysAndValues(initial + 150)
		for i := 0; i < initial; i++ {
			txPut(t, db, bucket, keys[i], values[i], Persistent, nil, nil)
		}
		require.Greater(t, db.MaxFileID, int64(1))
		require.NoError(t, db.Merge())
		mergedMaxFileID := db.MaxFileID
		require.NoError(t, db.Close())

		var openErr error
		db, openErr = Open(opts)
		require.NoError(t, openErr)
		require.Equal(t, db.ActiveFile.ActualSize, db.ActiveFile.writeOff)
		require.Greater(t, db.ActiveFile.writeOff, int64(0))

		for i := initial; i < len(keys); i++ {
			txPut(t, db, bucket, keys[i], values[i], Persistent, nil, nil)
		}
		require.Greater(t, db.MaxFileID, mergedMaxFileID)
		assertOrdersReadable(t, db, bucket, keys, values)
	})
}

func testSegmentRolloverKeepsRecordOffsets(t *testing.T, mode EntryIdxMode, perCommit bool) {
	opts := DefaultOptions
	opts.SegmentSize = 512
	opts.EntryIdxMode = mode

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "orders"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		const count = 200
		keys, values := orderKeysAndValues(count)
		if perCommit {
			for i := 0; i < count; i++ {
				err := db.Update(func(tx *Tx) error {
					return tx.Put(bucket, keys[i], values[i], Persistent)
				})
				require.NoError(t, err)
			}
		} else {
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < count; i++ {
					if err := tx.Put(bucket, keys[i], values[i], Persistent); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)
		}
		require.Greater(t, db.MaxFileID, int64(0))

		assertOrdersReadable(t, db, bucket, keys, values)
		require.NoError(t, db.Close())

		var openErr error
		db, openErr = Open(opts)
		require.NoError(t, openErr)
		assertOrdersReadable(t, db, bucket, keys, values)
	})
}

func orderKeysAndValues(count int) (keys, values [][]byte) {
	keys = make([][]byte, count)
	values = make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("order-%04d", i))
		values[i] = []byte(fmt.Sprintf("value-%04d", i))
	}
	return keys, values
}

func assertOrdersReadable(t *testing.T, db *DB, bucket string, keys, values [][]byte) {
	t.Helper()
	for i := range keys {
		txGet(t, db, bucket, keys[i], values[i], nil)
	}
}
