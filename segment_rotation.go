package nutsdb

import "sync"

// segmentRotation coordinates data segment rollover with in-flight write
// transactions.
//
// A transaction may span a segment boundary: its earlier entries are already
// persisted in the segment that runs out of space while the remaining entries
// land in the next segment. Rolling the active file over while another writer
// is still persisting such a transaction would leave the old segment holding a
// torn, never-committed prefix that readers (and recovery on reopen) could
// mistake for valid records.
//
// The coordinator makes rotation an explicit, serialized gate:
//   - A writer marks itself as persisting before the first write and clears
//     the mark once the whole transaction (including every cross-segment part)
//     is durable. The fast path is one short critical section per commit, so
//     ordinary writes take no I/O-path lock and keep their original throughput.
//   - A writer that needs more space calls rotate, which serializes rollovers
//     and waits until every other persisting writer has drained. The caller's
//     own write mark is ignored so a transaction can roll over the segment it
//     just filled instead of waiting forever on itself.
//
// A slow transaction never stalls unrelated writes: transactions whose
// entries fit in the active segment never enter rotate, and the gate only
// blocks a writer that actually needs a fresh segment, parking it on a
// condition variable rather than spinning.
type segmentRotation struct {
	mu      sync.Mutex
	cond    *sync.Cond
	writers map[uint64]struct{}
	// rotating is true while doRotate is running.
	rotating bool
}

func newSegmentRotation() *segmentRotation {
	sr := &segmentRotation{
		writers: make(map[uint64]struct{}),
	}
	sr.cond = sync.NewCond(&sr.mu)
	return sr
}

// beginWriting marks a transaction as persisting entries. It must be paired
// with endWriting, including on error paths.
func (sr *segmentRotation) beginWriting(txID uint64) {
	sr.mu.Lock()
	sr.writers[txID] = struct{}{}
	sr.mu.Unlock()
}

// endWriting clears the persisting mark of a transaction and wakes any writer
// waiting to roll the segment over.
func (sr *segmentRotation) endWriting(txID uint64) {
	sr.mu.Lock()
	delete(sr.writers, txID)
	if len(sr.writers) == 0 {
		sr.cond.Broadcast()
	}
	sr.mu.Unlock()
}

// activeWritingCount returns the number of writers currently persisting.
func (sr *segmentRotation) activeWritingCount() int {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return len(sr.writers)
}

// rotate serializes segment rollovers and runs doRotate once every other
// persisting writer has drained. The caller's own write mark (selfWriteID) is
// excluded from the drain check, allowing a transaction to cross the boundary
// of the segment it filled without deadlocking on its own mark.
//
// It parks on a condition variable (never spins) while another rollover is
// running or another writer is still persisting, and returns true once
// doRotate has installed a fresh active file. The caller therefore always
// retries its write against a segment with enough space.
func (sr *segmentRotation) rotate(selfWriteID uint64, doRotate func() error) (bool, error) {
	sr.mu.Lock()
	for sr.rotating || sr.otherWritersActiveLocked(selfWriteID) {
		sr.cond.Wait()
	}
	sr.rotating = true
	sr.mu.Unlock()

	var err error
	defer func() {
		sr.mu.Lock()
		sr.rotating = false
		sr.cond.Broadcast()
		sr.mu.Unlock()
	}()

	if err = doRotate(); err != nil {
		return false, err
	}
	return true, nil
}

// otherWritersActiveLocked reports whether any writer other than self is
// persisting. The caller must hold sr.mu.
func (sr *segmentRotation) otherWritersActiveLocked(selfWriteID uint64) bool {
	for id := range sr.writers {
		if id != selfWriteID {
			return true
		}
	}
	return false
}
