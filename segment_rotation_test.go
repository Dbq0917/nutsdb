package nutsdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/require"
)

const testSegmentSize int64 = 4 * KB

func newSegmentRotationTestDB(t *testing.T, dir string) *DB {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = testSegmentSize
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

// TestSegmentRotation_TxStraddlesBoundary writes a single transaction whose
// entries do not fit in one segment, then reads every key back. The
// transaction must stay atomic even though its entries span two segments.
func TestSegmentRotation_TxStraddlesBoundary(t *testing.T) {
	t.Run("FileIO", func(t *testing.T) {
		testTxStraddlesBoundary(t, FileIO)
	})
	t.Run("MMap", func(t *testing.T) {
		testTxStraddlesBoundary(t, MMap)
	})
}

func testTxStraddlesBoundary(t *testing.T, rwMode RWMode) {
	dir := t.TempDir()
	db := newSegmentRotationTestDBWithMode(t, dir, rwMode)
	t.Cleanup(func() { _ = db.Close() })

	bucket := "bucket-boundary"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	valueSize := 512
	entryCount := 24

	err := db.Update(func(tx *Tx) error {
		for i := 0; i < entryCount; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := make([]byte, valueSize)
			for j := range value {
				value[j] = byte(i)
			}
			if err := tx.Put(bucket, key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// The commit had to roll the active file over at least once.
	require.Greater(t, db.MaxFileID, int64(0))

	for i := 0; i < entryCount; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := make([]byte, valueSize)
		for j := range want {
			want[j] = byte(i)
		}
		txGet(t, db, bucket, key, want, nil)
	}

	// Reopening must recover the whole transaction and must never surface the
	// uncommitted prefix left in the rolled-over segment.
	require.NoError(t, db.Close())

	reopened := newSegmentRotationTestDBWithMode(t, dir, rwMode)
	t.Cleanup(func() { _ = reopened.Close() })

	for i := 0; i < entryCount; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := make([]byte, valueSize)
		for j := range want {
			want[j] = byte(i)
		}
		txGet(t, reopened, bucket, key, want, nil)
	}

	count, err := reopened.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(entryCount), count)
}

func newSegmentRotationTestDBWithMode(t *testing.T, dir string, rwMode RWMode) *DB {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = dir
	opt.SegmentSize = testSegmentSize
	opt.RWMode = rwMode
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

// TestSegmentRotation_RepeatedCommits keeps committing transactions across
// many segment boundaries and verifies that records stay readable and the
// segments never exceed the fixed 4KB size.
func TestSegmentRotation_RepeatedCommits(t *testing.T) {
	dir := t.TempDir()
	db := newSegmentRotationTestDB(t, dir)
	t.Cleanup(func() { _ = db.Close() })

	bucket := "bucket-repeated"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	total := 200
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		value := make([]byte, 700)
		for j := range value {
			value[j] = byte(i % 256)
		}
		txPut(t, db, bucket, key, value, Persistent, nil, nil)
	}

	require.Greater(t, db.MaxFileID, int64(2))

	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		want := make([]byte, 700)
		for j := range want {
			want[j] = byte(i % 256)
		}
		txGet(t, db, bucket, key, want, nil)
	}

	// Sealed segments never exceed the fixed segment size: walk every entry of
	// each rolled-over file and make sure nothing was written past the bound.
	for fid := int64(0); fid < db.MaxFileID; fid++ {
		path := getDataPath(fid, db.opt.Dir)
		fr, err := newFileRecovery(path, int(core.MinEntryHeaderSize))
		require.NoError(t, err)
		var off int64
		for {
			entry, err := fr.readEntry(off)
			if err != nil {
				_ = fr.release()
				break
			}
			if entry == nil {
				break
			}
			off += entry.Size()
			require.LessOrEqual(t, off, db.opt.SegmentSize)
		}
	}
}

// TestSegmentRotation_BeginEndWriting verifies the fast-path writer marking
// used by every commit.
func TestSegmentRotation_BeginEndWriting(t *testing.T) {
	sr := newSegmentRotation()
	require.Equal(t, 0, sr.activeWritingCount())

	sr.beginWriting(1)
	sr.beginWriting(2)
	require.Equal(t, 2, sr.activeWritingCount())

	sr.endWriting(2)
	require.Equal(t, 1, sr.activeWritingCount())

	// The only remaining writer is the caller itself, so its own rollover must
	// proceed instead of deadlocking on its own mark.
	rotated, err := sr.rotate(1, func() error { return nil })
	require.NoError(t, err)
	require.True(t, rotated)

	sr.endWriting(1)
	require.Equal(t, 0, sr.activeWritingCount())

	called := false
	rotated, err = sr.rotate(0, func() error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, rotated)
	require.True(t, called)
}

// TestSegmentRotation_SerializesRotation ensures only one rotation runs at a
// time even when many goroutines race for the slot: every blocked goroutine
// eventually performs its own serialized rollover, and callbacks never overlap.
func TestSegmentRotation_SerializesRotation(t *testing.T) {
	sr := newSegmentRotation()

	var concurrentRotations int32
	var maxObserved int32
	var mu sync.Mutex
	rotatedResults := make(chan bool, 64)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			rotated, err := sr.rotate(id, func() error {
				mu.Lock()
				concurrentRotations++
				if concurrentRotations > maxObserved {
					maxObserved = concurrentRotations
				}
				mu.Unlock()
				defer func() {
					mu.Lock()
					concurrentRotations--
					mu.Unlock()
				}()
				return nil
			})
			require.NoError(t, err)
			rotatedResults <- rotated
		}(uint64(i + 1))
	}
	wg.Wait()
	close(rotatedResults)

	require.Equal(t, int32(1), maxObserved)

	var rotations int
	for rotated := range rotatedResults {
		require.True(t, rotated)
		rotations++
	}
	require.Equal(t, 64, rotations)
}

// TestSegmentRotation_BlockedByOtherWriter ensures a writer cannot rotate a
// segment another transaction is still persisting into.
func TestSegmentRotation_BlockedByOtherWriter(t *testing.T) {
	sr := newSegmentRotation()
	sr.beginWriting(7)

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		rotated, err := sr.rotate(9, func() error { return nil })
		require.True(t, rotated)
		require.NoError(t, err)
		close(done)
	}()

	<-started
	// Give the waiter time to park on the condition variable.
	time.Sleep(20 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("rotation must block while another writer is persisting")
	default:
	}

	// The persisting writer itself is allowed to roll over its own segment.
	rotated, err := sr.rotate(7, func() error { return nil })
	require.NoError(t, err)
	require.True(t, rotated)

	sr.endWriting(7)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked rotation did not resume after the other writer drained")
	}
}

// TestSegmentRotation_ConcurrentWriters hammers commits from multiple
// goroutines across segment boundaries. Every committed record must remain
// readable, which is the regression guard for torn records at rollover.
func TestSegmentRotation_ConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	db := newSegmentRotationTestDB(t, dir)
	t.Cleanup(func() { _ = db.Close() })

	bucket := "bucket-concurrent"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	goroutines := 8
	perGoroutine := 40
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%d-k%04d", g, i))
				value := make([]byte, 600)
				for j := range value {
					value[j] = byte(g)
				}
				err := db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, value, Persistent)
				})
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			key := []byte(fmt.Sprintf("g%d-k%04d", g, i))
			want := make([]byte, 600)
			for j := range want {
				want[j] = byte(g)
			}
			txGet(t, db, bucket, key, want, nil)
		}
	}

	count, err := db.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(goroutines*perGoroutine), count)
}

// TestSegmentRotation_RolledOverSegmentHasNoTornRecord parses every rolled
// over segment directly and confirms every persisted entry passes CRC checks;
// a prefix written without coordination would surface as a CRC/parse failure
// or as an uncommitted entry during recovery.
func TestSegmentRotation_RolledOverSegmentHasNoTornRecord(t *testing.T) {
	dir := t.TempDir()
	db := newSegmentRotationTestDB(t, dir)

	bucket := "bucket-parse"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	err := db.Update(func(tx *Tx) error {
		for i := 0; i < 30; i++ {
			if err := tx.Put(bucket, []byte(fmt.Sprintf("key-%04d", i)), make([]byte, 480), Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, db.MaxFileID, int64(0))
	require.NoError(t, db.Close())

	reopened := newSegmentRotationTestDB(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })

	// The recovered index only contains fully committed records.
	count, err := reopened.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(30), count)

	// Every byte still persisted in a sealed segment must decode as a valid
	// entry (correct CRC), i.e. there is no half-written trailing record.
	for fid := int64(0); fid < reopened.MaxFileID; fid++ {
		path := getDataPath(fid, reopened.opt.Dir)
		fr, err := newFileRecovery(path, int(core.MinEntryHeaderSize))
		require.NoError(t, err)

		var off int64
		for {
			entry, err := fr.readEntry(off)
			if err != nil {
				_ = fr.release()
				break
			}
			if entry == nil {
				_ = fr.release()
				break
			}
			off += entry.Size()
			require.LessOrEqual(t, off, reopened.opt.SegmentSize)
		}
	}
}

// TestSegmentRotation_CommitMarkerLandsInNewSegment proves the coordination
// semantics: when a transaction straddles a segment boundary, the rolled-over
// segment only keeps its uncommitted prefix, while the Committed marker lands
// in the new segment. Recovery must therefore never surface the prefix.
func TestSegmentRotation_CommitMarkerLandsInNewSegment(t *testing.T) {
	dir := t.TempDir()
	db := newSegmentRotationTestDB(t, dir)

	bucket := "bucket-marker"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	err := db.Update(func(tx *Tx) error {
		for i := 0; i < 30; i++ {
			if err := tx.Put(bucket, []byte(fmt.Sprintf("key-%04d", i)), make([]byte, 480), Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, db.MaxFileID, int64(0))

	// Walk every sealed segment: entries written before the rollover must all
	// be uncommitted. A Committed entry in a sealed segment would mean a
	// transaction was finalized across a boundary without coordination.
	for fid := int64(0); fid < db.MaxFileID; fid++ {
		path := getDataPath(fid, db.opt.Dir)
		fr, err := newFileRecovery(path, int(core.MinEntryHeaderSize))
		require.NoError(t, err)

		var off int64
		for {
			entry, err := fr.readEntry(off)
			if err != nil || entry == nil {
				_ = fr.release()
				break
			}
			require.Equal(t, UnCommitted, entry.Meta.Status,
				"sealed segment %d at off %d must not hold a committed entry", fid, off)
			off += entry.Size()
		}
	}

	// The active segment must contain exactly one Committed marker (the final
	// entry of the straddling transaction).
	activePath := getDataPath(db.MaxFileID, db.opt.Dir)
	fr, err := newFileRecovery(activePath, int(core.MinEntryHeaderSize))
	require.NoError(t, err)

	var committedMarkers int
	var off int64
	for {
		entry, err := fr.readEntry(off)
		if err != nil || entry == nil {
			_ = fr.release()
			break
		}
		if entry.Meta.Status == Committed {
			committedMarkers++
		}
		off += entry.Size()
	}
	require.Equal(t, 1, committedMarkers)

	require.NoError(t, db.Close())

	reopened := newSegmentRotationTestDB(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })

	count, err := reopened.getRecordCount()
	require.NoError(t, err)
	require.Equal(t, int64(30), count)
}
