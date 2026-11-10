// Package depwait resolves a controller's NamedResource dependencies
// for the dag scheduler: Classify decides, WITHOUT blocking, whether
// each dep is Ready, terminally Failed, or still blocked (parkable).
// The shared readiness predicates (readyNow, the ReadyExpr
// evaluator, discovery-record promotion) live here so the gate semantics
// are single-sourced.
package depwait

import (
	"context"
	"errors"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type stateReader interface {
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetStatus(manifest.NamedResource) (store.StatusInfo, bool)
	Snapshot(manifest.NamedResource, bool) (manifest.BaseManifest, store.Observation)
	Preflight() string
	UnsettledProducers(manifest.NamedResource) []manifest.NamedResource
}

// Waiter holds the parameters for one dependency-classification operation.
type Waiter struct {
	Admission selection.PrepareRequest
	Store     stateReader
	Parent    manifest.NamedResource
	Missing   *MissingPolicy

	// Materializer requires readiness even for existence-only dependency kinds.
	Materializer bool

	// Existence, when non-nil, lets Classify lazy-promote a missing dep
	// from the loader's discovery records before deciding it is
	// absent. The orchestrator wires this against the discovery index;
	// tests supply stubs. When nil, an absent dep is simply absent.
	//
	// See ExistenceLookup for the decision matrix.
	Existence ExistenceLookup
}

// ExistenceLookup is the seam Classify uses to resolve missing
// dependencies. Bundled into one interface so the orchestrator
// (and any future embedder) wires a single object through the
// controller Options pipe instead of two parallel closures.
//
// The orchestrator's implementation reads from the loader's
// discovery index — file-indexed objects (CMs/Secrets/HRs the
// discovery loader kept out of the Store) get lazy-promoted
// the moment a depwait edge needs them, while render-only ids
// (no file record) stay absent until a parent render's
// emitRenderedChildren chain produces them.
type ExistenceLookup interface {
	// IsFileIndexed reports whether id has a raw-file discovery record.
	IsFileIndexed(id manifest.NamedResource) bool
	// Promote attempts to materialize id into the Store from the
	// discovery index. Returns true when promotion succeeded
	// and id is now reachable via store.GetObject.
	Promote(context.Context, selection.PrepareRequest, manifest.NamedResource) (bool, error)
}

type expressionReadiness struct {
	ready   bool
	failure string
}

func (w *Waiter) tryReadyExpr(
	dep *manifest.DependencyRef,
	obj manifest.BaseManifest,
	conds []store.Condition,
) expressionReadiness {
	ok, err := w.evaluateReadyExpr(*dep.ReadyExpr, dep.NamedResource, obj, conds)
	if err != nil {
		if _, isCompile := errors.AsType[*celCompileError](err); isCompile {
			return expressionReadiness{failure: "readyExpr: " + err.Error()}
		}
		// Eval error: transient, treat as not-yet-ready.
		return expressionReadiness{}
	}
	return expressionReadiness{ready: ok}
}
