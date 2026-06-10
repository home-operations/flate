package blob

import "sync"

// gcMu coordinates the GC mark↔sweep window against concurrent blob
// writes (Store.PutBytes).
//
// PutBytes refreshes a reused blob's mtime so an age-pruning Sweep keeps
// it — a live caller is about to use the directory it returns. Without
// coordination this interleaving deletes a freshly-touched blob:
//
//  1. PutBytes finds the blob present (Exists) and is about to refresh
//     its mtime.
//  2. GC's sweep age-reads the blob's STALE (old) mtime, finds it
//     unreferenced and older than MaxAge, and RemoveAll's it.
//  3. PutBytes returns the now-deleted blob directory to a caller that
//     trusts it.
//
// Sweep takes the exclusive lock for the duration of mark + sweep, so a
// blob write can't interleave with the age-read/remove. PutBytes takes
// the shared lock so concurrent writes to different blobs stay parallel
// — they only serialize against the (rare) GC sweep.
var gcMu sync.RWMutex

// rLockGC acquires a shared lock against the GC sweep. Caller must
// invoke the returned function to release. Internal to the blob
// package — Store.PutBytes holds it across its check→refresh→finalize;
// callers outside blob use WithSweepLock.
func rLockGC() func() {
	gcMu.RLock()
	return gcMu.RUnlock
}

// WithSweepLock acquires the exclusive sweep lock, calls fn, then
// releases the lock. Held across mark + sweep so no blob write can
// finalize within the window. The error returned by fn is propagated
// unchanged; the lock is always released.
func WithSweepLock(fn func() error) error {
	gcMu.Lock()
	defer gcMu.Unlock()
	return fn()
}
