// Package base owns shared controller lifecycle, execution acceptance and publication.
package base

import (
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller owns shared reconciliation lifecycle, dependency gates and publication.
// Configure MUST complete before StartLifecycle or dispatch.
type Controller struct {
	Store *store.Store
	Tasks *task.Service
	// SubstituteFrom is the immutable bootstrap-approved substitution snapshot.
	SubstituteFrom map[manifest.NamedResource]manifest.BaseManifest

	started   atomic.Bool
	filter    *change.Filter
	skipKinds []string

	dependencySubstitutions values.DependencySubstitutions
	existence               depwait.ExistenceLookup
	allowMissingSecrets     bool
	allowMissingCRDs        bool
	preflight               func(manifest.NamedResource) (string, bool)
}

// New constructs a controller with the shared store and task service.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{Store: s, Tasks: t}
}

// requireNotStarted panics if the started gate is set, enforcing the
// invariant that reconcile-shaping config is frozen once dispatch
// begins. method is the calling setter's name for the panic message.
func (c *Controller) requireNotStarted(method string) {
	if c.started.Load() {
		panic("controller: " + method + " called after dispatch begins")
	}
}

// Options is the post-bootstrap, reconcile-shaping config common to every
// render controller (Kustomization, HelmRelease, ResourceSet). The
// orchestrator wires it once before dispatch; the source controller is fetch-only
// and configures just its filter directly, so it does NOT embed this. A
// concrete controller embeds Options in its own option struct and forwards it
// to Configure, then assigns its controller-specific fields.
type Options struct {
	// SkipKinds is the immutable visible-output exclusion list.
	SkipKinds []string
	// SubstituteFrom is the immutable bootstrap-approved substitution snapshot.
	SubstituteFrom map[manifest.NamedResource]manifest.BaseManifest
	// DependencySubstitutions is the frozen discovered lookup used before child publication.
	DependencySubstitutions values.DependencySubstitutions
	// Filter narrows reconciliation to changed resources in changed-only mode.
	Filter *change.Filter
	// Index contains immutable source-qualified discovery evidence.
	Index *manifest.DiscoveryIndex
	// Existence is the file-existence lookup depwait uses to lazy-promote
	// file-indexed deps and to distinguish a render-only dep still in flight
	// from a typo'd one. See depwait.ExistenceLookup.
	Existence depwait.ExistenceLookup
	// AllowMissingCRDs accepts CRD dependencies absent from the offline inputs
	// because the cluster installs them at runtime.
	AllowMissingCRDs bool
	// AllowMissingSecrets permits unresolved required Helm values at the fixpoint.
	AllowMissingSecrets bool
	// PreflightFailure reports dependency-graph errors discovered before
	// reconcile; when set for an id the controller marks it Failed and renders
	// nothing.
	PreflightFailure func(manifest.NamedResource) (string, bool)
}

// Configure installs immutable setup before dispatch.
func (c *Controller) Configure(opts Options) {
	c.requireNotStarted("Configure")
	c.dependencySubstitutions = opts.DependencySubstitutions
	c.filter = opts.Filter
	c.SubstituteFrom = opts.SubstituteFrom
	c.skipKinds = opts.SkipKinds
	c.existence = opts.Existence
	c.preflight = opts.PreflightFailure
	c.Store.SetDiscoveryIndex(opts.Index)
	c.allowMissingCRDs = opts.AllowMissingCRDs
	c.allowMissingSecrets = opts.AllowMissingSecrets
}

// KeepEmitted extends the change filter's keep set so render-emitted
// children pass the changed-only filter. Without this, a
// parent whose render emits a child that wasn't on disk at filter-build
// time (kustomize component+replacement KSes, charts that render source
// CRs) would silently drop that child from the diff comparison. Routed
// through Filter.AddEmitted so an ancestor-only parent doesn't cascade
// unrelated file-loaded children into keep (#204/#260/#308).
//
// MUST be called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set.
func (c *Controller) KeepEmitted(parent manifest.NamedResource, child manifest.BaseManifest, reads *store.ReadSet) {
	if f := c.filter; f != nil {
		var objects change.ObjectLister = c.Store
		if reads != nil {
			objects = reads
		}
		for _, id := range f.AddEmitted(parent, child, objects) {
			c.Store.SetIncluded(id, true)
		}
	}
}

// SetPendingUnlessReady initializes progress without changing an existing condition.
func (c *Controller) SetPendingUnlessReady(id manifest.NamedResource, msg string) {
	if info, ok := c.Store.GetStatus(id); ok {
		if info.Status == store.StatusPending {
			c.Store.ReportProgress(id, msg)
		}
		return
	}
	c.Store.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: msg})
}

// StartLifecycle freezes reconcile-shaping configuration before dispatch.
func (c *Controller) StartLifecycle() {
	c.started.Store(true)
}

// preGate marks suspended objects Ready; refresh MUST never apply changed-only filtering.
func (c *Controller) preGate(obj manifest.BaseManifest) bool {
	if obj.Suspended() {
		c.Store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"})
		return true
	}
	return false
}

// filterActive reports whether a non-nil, enabled change filter is configured.
func (c *Controller) filterActive() bool {
	return c.filter != nil && c.filter.Enabled()
}
