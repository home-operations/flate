// Package base owns shared controller lifecycle, execution acceptance and publication.
package base

import (
	"context"
	"fmt"
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
//
//nolint:gocritic // Configure snapshots the public value options before concurrent dispatch.
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
//
//nolint:gocritic // Immutable value inputs preserve snapshot semantics.
func (c *Controller) KeepEmitted(
	ctx context.Context,
	parent manifest.NamedResource,
	child store.Child,
	reads *store.ReadSet,
) (bool, error) {
	f := c.filter
	if !f.Enabled() {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	if c.loader == nil {
		return false, manifest.NewError(
			manifest.ErrInput,
			nil,
			"restricted controller requires its invocation loader",
		)
	}
	if reads == nil {
		reads = &store.ReadSet{}
		reads.Init(c.Store, parent, nil)
		defer reads.Retire()
	}
	admission := emittedAdmission{
		controller:       c,
		filter:           f,
		reads:            reads,
		incoming:         child,
		inferProductions: child.Obj != nil && parent == (manifest.NamedResource{}) && len(child.Productions) == 0,
	}
	if child.Obj != nil && parent != (manifest.NamedResource{}) && parent != child.Obj.Named() {
		admission.incoming.Productions = append(slices.Clone(child.Productions), manifest.Production{Owner: parent})
	}
	defer func() { admission.proof.Retire() }()
	for {
		prepareResult1, prepareResult1Err := admission.prepare(ctx)
		ready, retry, err := prepareResult1.committed, prepareResult1.retry, prepareResult1Err
		if retry {
			continue
		}
		if err != nil || !ready {
			return false, err
		}
		break
	}
	if child.Obj == nil {
		return true, nil
	}
	c.Store.SetIncluded(child.Obj.Named(), f.ShouldReconcile(child.Obj.Named()))
	return f.ShouldReconcile(child.Obj.Named()), nil
}

func (c *Controller) admissionProof(reads *store.ReadSet) *store.ReadSet {
	proof := &store.ReadSet{}
	proof.Init(c.Store, reads.ID(), nil)
	for id := range reads.Required() {
		proof.RequireData(id)
	}
	return proof
}

func admitEmittedInput(proof *store.ReadSet, input *store.Child) {
	source := manifest.BootstrapSourceID
	for itemIndex := range input.Productions {
		proof := input.Productions[itemIndex]

		if proof.Source != (manifest.NamedResource{}) {
			source = proof.Source
			break
		}
	}
	proof.Admit(input.Obj, source, manifest.NamedResource{}, input.Doc)
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
		c.Store.UpdateStatus(
			obj.Named(),
			store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"},
		)
		return true
	}
	return false
}

// filterActive reports whether a non-nil, enabled change filter is configured.
func (c *Controller) filterActive() bool {
	return c.filter != nil && c.filter.Enabled()
}

type emittedAdmission struct {
	controller       *Controller
	filter           *change.Filter
	reads            *store.ReadSet
	incoming         store.Child
	proof            *store.ReadSet
	inferProductions bool
}

type admissionState struct {
	committed bool
	retry     bool
}

func (a *emittedAdmission) prepare(ctx context.Context) (admissionState, error) {
	a.proof.Retire()
	a.proof = a.controller.admissionProof(a.reads)
	if a.inferProductions {
		a.incoming.Productions = a.proof.Promotions(a.incoming.Obj.Named(), nil)
	}
	proposal, err := a.controller.loader.Prepare(ctx, a.incoming, a.proof, nil)
	if err != nil {
		return admissionState{committed: false, retry: false}, err
	}
	if !a.reads.Valid() {
		return admissionState{committed: false, retry: false}, nil
	}
	if !a.proof.Valid() {
		return admissionState{committed: false, retry: true}, nil
	}
	if err := ctx.Err(); err != nil {
		return admissionState{committed: false, retry: false}, fmt.Errorf("context: %w", err)
	}
	added, retry := a.filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
	if retry {
		return admissionState{committed: false, retry: true}, nil
	}
	for _, id := range added {
		a.controller.Store.SetIncluded(id, true)
	}
	for _, input := range proposal.Objects {
		if err := ctx.Err(); err != nil {
			return admissionState{committed: false, retry: false}, fmt.Errorf("context: %w", err)
		}
		if !a.reads.Valid() {
			return admissionState{committed: false, retry: false}, nil
		}
		if !a.proof.Valid() {
			return admissionState{committed: false, retry: true}, nil
		}
		admitEmittedInput(a.proof, &input)
	}
	if !a.proof.Valid() {
		return admissionState{committed: false, retry: true}, nil
	}
	return admissionState{committed: true, retry: false}, nil
}
