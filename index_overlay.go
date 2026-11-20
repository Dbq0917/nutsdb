package nutsdb

import (
	"sync"
	"sync/atomic"

	"github.com/nutsdb/nutsdb/internal/core"
)

type indexOverlayKey struct {
	bucketId core.BucketId
	key      string
}

type indexOverlayWrite struct {
	version uint64
	entries map[indexOverlayKey]*core.Entry
}

// indexOverlay exposes writes from an asynchronous commit to read transactions
// that started after the write was published. The base index remains the source
// of truth; overlay entries are removed once the commit has applied them there.
type indexOverlay struct {
	mu           sync.RWMutex
	writeVersion atomic.Uint64
	writes       map[uint64]*indexOverlayWrite
}

func newIndexOverlay() *indexOverlay {
	return &indexOverlay{
		writes: make(map[uint64]*indexOverlayWrite),
	}
}

func (o *indexOverlay) currentVersion() uint64 {
	if o == nil {
		return 0
	}
	return o.writeVersion.Load()
}

func (o *indexOverlay) publish(txID uint64, pending *pendingEntryList) {
	if o == nil || pending == nil || len(pending.entriesInBTree) == 0 {
		return
	}

	entries := make(map[indexOverlayKey]*core.Entry)
	for _, entriesInBucket := range pending.entriesInBTree {
		for key, entry := range entriesInBucket {
			entries[indexOverlayKey{
				bucketId: entry.Meta.BucketId,
				key:      key,
			}] = entry
		}
	}
	if len(entries) == 0 {
		return
	}

	o.mu.Lock()
	o.writes[txID] = &indexOverlayWrite{
		version: o.writeVersion.Add(1),
		entries: entries,
	}
	o.mu.Unlock()
}

func (o *indexOverlay) clear(txID uint64) {
	if o == nil {
		return
	}

	o.mu.Lock()
	delete(o.writes, txID)
	o.mu.Unlock()
}

func (o *indexOverlay) lookup(bucketId core.BucketId, key []byte, readVersion uint64) (*core.Entry, bool) {
	if o == nil {
		return nil, false
	}

	overlayKey := indexOverlayKey{bucketId: bucketId, key: string(key)}
	var (
		latest        *core.Entry
		latestVersion uint64
	)

	o.mu.RLock()
	for _, write := range o.writes {
		if write.version > readVersion || write.version <= latestVersion {
			continue
		}
		entry, ok := write.entries[overlayKey]
		if !ok {
			continue
		}
		latest = entry
		latestVersion = write.version
	}
	o.mu.RUnlock()

	return latest, latest != nil
}

func (tx *Tx) lookupIndexOverlay(bucketId core.BucketId, key []byte) (*core.Entry, bool) {
	if tx.db == nil {
		return nil, false
	}
	return tx.db.indexOverlay.lookup(bucketId, key, tx.readIndexVersion)
}

func (tx *Tx) indexOverlayEntryVisible(entry *core.Entry) bool {
	if entry == nil || entry.Meta.Flag == DataDeleteFlag {
		return false
	}
	return tx.db.ttlService == nil || !tx.db.ttlService.IsExpired(entry.Meta.TTL, entry.Meta.Timestamp)
}
