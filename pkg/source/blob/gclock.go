package blob

import "sync"

// gcMu coordinates the GC mark↔sweep window against concurrent
// Refs.Put.
//
// Without it, this interleaving deletes a freshly-referenced blob:
//
//  1. GC's mark walk runs over refs/<category>/ and snapshots the
//     live digest set. A Put that lands its atomic rename after the
//     walk reads that subdirectory is invisible to mark.
//  2. GC's sweep then scans blobs/. For each blob older than MaxAge,
//     it consults the live set. A blob the new ref points at is
//     missing from live (mark didn't see it) and gets removed.
//  3. The Put completes successfully — but its ref now points at a
//     blob that was just deleted.
//
// Sweep takes the exclusive lock for the duration of mark + sweep,
// freezing Put visibility. Put takes the shared lock so concurrent
// Puts to different keys stay parallel — they only serialize against
// the (rare) GC sweep.
var gcMu sync.RWMutex

// RLockGC acquires a shared lock against the GC sweep. Caller must
// invoke the returned function to release.
func RLockGC() func() {
	gcMu.RLock()
	return gcMu.RUnlock
}

// LockGC acquires the exclusive sweep lock. Held across mark + sweep
// so no Refs.Put can finalize within the window. Caller must invoke
// the returned function to release.
func LockGC() func() {
	gcMu.Lock()
	return gcMu.Unlock
}
