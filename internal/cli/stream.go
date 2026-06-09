package cli

import (
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// streamEmitter implements `flate build --stream`: each resource's rendered
// docs are written to stdout as multi-doc YAML the moment the resource is
// known-done (reaches a terminal status), instead of buffering until the
// whole reconcile fixpoint. Emission order is completion order — the doc SET
// matches the default buffered output, but not its global byte order; CI
// that diffs output should keep the default mode.
//
// Failed-but-rendered resources emit too, mirroring the buffered path
// (Result.Manifests keeps artifacts of failed resources). finish runs after
// Render to catch up anything that never streamed — most notably
// ResourceSet-extension docs, which are attributed to their owning
// Kustomization only after the run (expandResourceSetsPostRun) — and to
// preserve the explicit-name typo error.
type streamEmitter struct {
	out    io.Writer
	errOut io.Writer // stale-stream warnings (stderr; never stdout)
	o      *orchestrator.Orchestrator
	c      *commonFlags
	b      *buildFlags
	kinds  []string
	name   string
	// skipKinds is precomputed once; emissionDocs applies it per batch.
	skipKinds []string

	mu sync.Mutex
	// streamed records, per emitted id, the artifact fingerprint and raw
	// (pre-drop) doc count at stream time — finish uses them to detect a
	// post-stream re-render (stale warning) and to locate the
	// ResourceSet-extension tail appended to Result.Manifests after Run.
	streamed map[manifest.NamedResource]streamedState
	err      error // first stdout write failure; surfaced by finish
}

type streamedState struct {
	fingerprint string
	emitted     int // docs written for this id (post-filter)
}

func newStreamEmitter(out, errOut io.Writer, o *orchestrator.Orchestrator, kinds []string, name string, c *commonFlags, b *buildFlags) *streamEmitter {
	return &streamEmitter{
		out:       out,
		errOut:    errOut,
		o:         o,
		c:         c,
		b:         b,
		kinds:     kinds,
		name:      name,
		skipKinds: c.skipResourceKinds(),
		streamed:  map[manifest.NamedResource]streamedState{},
	}
}

// attach subscribes the emitter to s's status events; call before Render so
// terminal statuses observed during the run stream immediately.
func (se *streamEmitter) attach(s *store.Store) store.Unsubscribe {
	return s.OnStatus(se.onStatus, false)
}

func (se *streamEmitter) onStatus(id manifest.NamedResource, info store.StatusInfo) {
	if info.Status != store.StatusReady && info.Status != store.StatusFailed {
		return
	}
	if !slices.Contains(se.kinds, id.Kind) ||
		(se.name != "" && id.Name != se.name) ||
		!se.c.includeNamespace(se.o.Filter(), id.Namespace) {
		return
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	if prior, done := se.streamed[id]; done && (prior.emitted > 0 || prior.fingerprint != "") {
		// First doc-producing terminal wins: written docs cannot be
		// retracted, so a later flip (a changed-only resurrection ending
		// differently) is left to finish, which detects the changed
		// artifact and warns. A prior terminal that emitted NOTHING (failed
		// before rendering) doesn't block — the re-run's docs stream.
		return
	}
	// Controllers SetArtifact before the terminal status write, so a
	// rendered resource's artifact is visible here. No artifact (failed
	// before render, suspended, zero docs) → record the id as handled so
	// finish doesn't re-emit it, but write nothing.
	state := streamedState{}
	if art, ok := se.o.Store().GetArtifact(id).(store.RenderedArtifact); ok {
		docs := emissionDocs(art.RenderedManifests(), se.b, se.skipKinds)
		state.fingerprint = art.RenderedFingerprint()
		state.emitted = len(docs)
		if len(docs) > 0 && se.err == nil {
			se.err = format.YAMLMulti(se.out, docs)
		}
	}
	se.streamed[id] = state
}

// finish completes the stream after Render: it emits every in-scope resource
// that never streamed (and the ResourceSet-extension tails appended to
// already-streamed Kustomizations after the run), reproduces the buffered
// path's explicit-name typo error, warns on stderr about resources whose
// artifact changed after streaming, and surfaces the first stdout write
// error.
func (se *streamEmitter) finish(res *orchestrator.Result) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	matched := 0
	for _, kind := range se.kinds {
		for _, obj := range se.o.Store().ListObjects(kind) {
			id := obj.Named()
			if se.name != "" && id.Name != se.name {
				continue
			}
			if !se.c.includeNamespace(se.o.Filter(), id.Namespace) {
				continue
			}
			matched++
			mans := res.Manifests[id]
			state, done := se.streamed[id]
			if !done {
				// Never streamed (terminal before attach, or only became
				// renderable post-Run): emit its full buffered batch.
				if docs := emissionDocs(mans, se.b, se.skipKinds); len(docs) > 0 && se.err == nil {
					se.err = format.YAMLMulti(se.out, docs)
				}
				continue
			}
			art, ok := se.o.Store().GetArtifact(id).(store.RenderedArtifact)
			if ok && art.RenderedFingerprint() != state.fingerprint {
				_, _ = fmt.Fprintf(se.errOut, "flate: %s re-rendered after streaming; streamed output may be stale (re-run without --stream for the final render)\n", id)
				continue
			}
			// Result.Manifests[id] = dropped(artifact docs) ++ dropped(RS
			// extensions); DropKinds filters element-wise, so anything past
			// the artifact's own post-drop count is the extension tail.
			var artDropped int
			if ok {
				artDropped = len(manifest.DropKinds(slices.Clone(art.RenderedManifests()), se.skipKinds))
			}
			if tail := mans[min(artDropped, len(mans)):]; len(tail) > 0 {
				if docs := emissionDocs(tail, se.b, se.skipKinds); len(docs) > 0 && se.err == nil {
					se.err = format.YAMLMulti(se.out, docs)
				}
			}
		}
	}
	if se.name != "" && matched == 0 {
		// Mirror collectRendered: a typo'd explicit name must error, not
		// silently emit an empty render. kinds is 1-element for named builds.
		return noNamedError(se.kinds[0], se.name)
	}
	return se.err
}
