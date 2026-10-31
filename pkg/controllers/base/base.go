// Package base owns shared controller lifecycle, execution acceptance and publication.
package base

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/loader"
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

	started atomic.Bool
	filter  *change.Filter
	loader  *loader.Loader
	// SkipKinds is the immutable visible-output exclusion list.
	SkipKinds []string

	// DependencySubstitutions binds emitted dependency projection to accepted history.
	DependencySubstitutions values.DependencySubstitutions
	existence               depwait.ExistenceLookup
	// AllowMissingKinds permits unavailable Helm valuesFrom kinds and absent CRDs.
	AllowMissingKinds []string
	allowMissingCRDs  bool
	preflight         func(manifest.NamedResource) (string, bool)
}

// New constructs a controller with the shared store and task service.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{Store: s, Tasks: t}
}

// MissingCRDsAllowed reports whether unresolved CRD dependencies are allowed.
func (c *Controller) MissingCRDsAllowed() bool { return c.allowMissingCRDs }

// requireNotStarted panics if the started gate is set, enforcing the
// invariant that reconcile-shaping config is frozen once dispatch
// begins. method is the calling setter's name for the panic message.
func (c *Controller) requireNotStarted(method string) {
	if c.started.Load() {
		panic("controller: " + method + " called after dispatch begins")
	}
}

// Options contains immutable controller configuration installed before dispatch.
type Options struct {
	// Loader supplies the invocation's declaration admission boundary.
	Loader *loader.Loader
	// SkipKinds is the immutable visible-output exclusion list.
	SkipKinds []string
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
	// AllowMissingKinds permits unavailable Helm valuesFrom kinds and absent CRDs.
	AllowMissingKinds []string
	// PreflightFailure reports dependency-graph errors discovered before
	// reconcile; when set for an id the controller marks it Failed and renders
	// nothing.
	PreflightFailure func(manifest.NamedResource) (string, bool)
}

// Configure installs immutable setup before dispatch.
func (c *Controller) Configure(opts Options) {
	c.requireNotStarted("Configure")
	c.DependencySubstitutions = opts.DependencySubstitutions
	c.filter = opts.Filter
	c.loader = opts.Loader
	c.SkipKinds = opts.SkipKinds
	c.existence = opts.Existence
	c.preflight = opts.PreflightFailure
	c.Store.SetDiscoveryIndex(opts.Index)
	c.allowMissingCRDs = slices.Contains(opts.AllowMissingKinds, manifest.KindCustomResourceDefinition)
	c.AllowMissingKinds = opts.AllowMissingKinds
}

// KeepEmitted admits the incoming child and reports whether it is selected.
// Admission MUST precede publication; a nil child admits the captured required inputs only.
func (c *Controller) KeepEmitted(ctx context.Context, parent manifest.NamedResource, child store.Child, reads *store.ReadSet) (bool, error) {
	if f := c.filter; f.Enabled() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if c.loader == nil {
			return false, manifest.NewError(manifest.ErrInput, nil, "restricted controller requires its invocation loader")
		}
		if reads == nil {
			reads = &store.ReadSet{}
			reads.Init(c.Store, parent, nil)
			defer reads.Retire()
		}
		incoming := child
		if child.Obj != nil && parent != (manifest.NamedResource{}) && parent != child.Obj.Named() {
			incoming.Productions = append(slices.Clone(child.Productions), manifest.Production{Owner: parent})
		}
		var proof *store.ReadSet
		defer func() { proof.Retire() }()
	prepare:
		for {
			proof.Retire()
			proof = &store.ReadSet{}
			proof.Init(c.Store, reads.ID(), nil)
			for id := range reads.Required() {
				proof.RequireData(id)
			}
			if incoming.Obj != nil && parent == (manifest.NamedResource{}) && len(child.Productions) == 0 {
				incoming.Productions = proof.Promotions(incoming.Obj.Named(), nil)
			}
			proposal, err := c.loader.Prepare(ctx, incoming, proof, nil)
			if err != nil {
				return false, err
			}
			if !reads.Valid() {
				return false, nil
			}
			if !proof.Valid() {
				continue
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			added, retry := f.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
			if retry {
				continue
			}
			for _, id := range added {
				c.Store.SetIncluded(id, true)
			}
			for _, input := range proposal.Objects {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if !reads.Valid() {
					return false, nil
				}
				if !proof.Valid() {
					continue prepare
				}
				source := manifest.BootstrapSourceID
				for _, proof := range input.Productions {
					if proof.Source != (manifest.NamedResource{}) {
						source = proof.Source
						break
					}
				}
				proof.Admit(input.Obj, source, manifest.NamedResource{}, input.Doc)
			}
			if !proof.Valid() {
				continue
			}
			break
		}
		if child.Obj == nil {
			return true, nil
		}
		c.Store.SetIncluded(child.Obj.Named(), f.ShouldReconcile(child.Obj.Named()))
		return f.ShouldReconcile(child.Obj.Named()), nil
	}
	return true, nil
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
