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

// KeepEmitted MUST admit every child in the immutable generation before publication.
//
//nolint:gocritic // Admission retains the immutable generation and required inputs.
func (c *Controller) KeepEmitted(
	ctx context.Context,
	parent manifest.NamedResource,
	request loader.PrepareRequest,
) (bool, error) {
	reads := request.Reads
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
	request.Reads, request.Owner = reads, parent
	admission := emittedAdmission{controller: c, reads: reads, request: request}
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
	selected := len(request.Generation) == 0
	for i := range request.Generation {
		if child := &request.Generation[i]; child.Obj != nil {
			keep := f.ShouldReconcile(child.Obj.Named())
			c.Store.SetIncluded(child.Obj.Named(), keep, f.OrdinaryDemand(child.Obj.Named()))
			selected = selected || keep
		}
	}
	return selected, nil
}

func (c *Controller) admissionProof(reads *store.ReadSet) *store.ReadSet {
	proof := &store.ReadSet{}
	proof.Init(c.Store, reads.ID(), nil)
	proof.SetWork(reads.Work())
	for id, qualification := range reads.Required() {
		if qualification {
			proof.RequireQualification(id)
		} else {
			proof.RequireData(id)
		}
	}
	return proof
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
	controller *Controller
	reads      *store.ReadSet
	proof      *store.ReadSet
	request    loader.PrepareRequest
}

type admissionState struct {
	committed bool
	retry     bool
}

func (a *emittedAdmission) prepare(ctx context.Context) (admissionState, error) {
	a.proof.Retire()
	a.proof = a.controller.admissionProof(a.reads)
	a.request.Reads = a.proof
	proposal, err := a.controller.loader.Prepare(ctx, a.request)
	if err != nil {
		return admissionState{committed: false, retry: false}, err
	}
	defer proposal.Retire()
	if !a.reads.Valid() {
		return admissionState{committed: false, retry: false}, nil
	}
	if !a.proof.Valid() {
		return admissionState{committed: false, retry: true}, nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return admissionState{committed: false, retry: false}, fmt.Errorf("context: %w", contextErr)
	}
	committed, err := a.controller.loader.Commit(ctx, proposal, loader.Execution)
	if err != nil {
		return admissionState{}, err
	}
	if !committed {
		return admissionState{retry: a.reads.Valid()}, nil
	}
	return admissionState{committed: true, retry: false}, nil
}
