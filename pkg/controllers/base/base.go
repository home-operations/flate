// Package base owns shared controller lifecycle, execution acceptance and publication.
package base

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller owns shared reconciliation lifecycle, dependency gates and publication.
// Configure MUST complete before StartLifecycle or dispatch.
type Controller struct {
	Store *store.Store
	Tasks *task.Service

	started   atomic.Bool
	selection *selection.Session
	loader    *loader.Loader
	// SkipKinds is the immutable visible-output exclusion list.
	SkipKinds []string

	existence depwait.ExistenceLookup
	Missing   *depwait.MissingPolicy
}

// New MUST install a real empty policy before standalone assessment.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{Store: s, Tasks: t, Missing: depwait.NewMissingPolicy(nil)}
}

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
	// Session owns restricted reconciliation membership.
	Session *selection.Session
	// Index contains immutable source-qualified discovery evidence.
	Index *manifest.DiscoveryIndex
	// Existence is the file-existence lookup depwait uses to lazy-promote
	// file-indexed deps and to distinguish a render-only dep still in flight
	// from a typo'd one. See depwait.ExistenceLookup.
	Existence depwait.ExistenceLookup
	Missing   *depwait.MissingPolicy
}

// Configure MUST install the invocation policy before dispatch.
func (c *Controller) Configure(opts Options) {
	c.requireNotStarted("Configure")
	c.selection = opts.Session
	c.loader = opts.Loader
	c.SkipKinds = opts.SkipKinds
	c.existence = opts.Existence
	c.Store.SetDiscoveryIndex(opts.Index)
	if opts.Missing == nil {
		opts.Missing = depwait.NewMissingPolicy(nil)
	}
	c.Missing = opts.Missing
}

// KeepEmitted MUST admit every child in the immutable generation before publication.
//
//nolint:funlen,gocritic,gocognit // One capture MUST cover the immutable generation across acquisition and publication.
func (c *Controller) KeepEmitted(
	ctx context.Context,
	parent manifest.NamedResource,
	request selection.PrepareRequest,
) (bool, error) {
	if c.selection == nil {
		return true, nil
	}
	if c.loader == nil {
		return false, manifest.NewError(manifest.ErrInput, nil, "restricted controller requires its invocation loader")
	}
	reads := request.Reads
	if reads == nil {
		reads = &store.ReadSet{}
		reads.Init(c.Store, parent, nil)
		defer reads.Retire()
	}
	request.Missing = c.Missing
	request.Owner = parent
	var local *selection.Preparation
	if request.Preparation == nil {
		request.Preparation = &local
		defer func() { local.Retire() }()
	}
	for {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("context: %w", err)
		}
		if !reads.Valid() {
			return false, nil
		}
		proof := c.admissionProof(reads)
		request.Reads = proof
		proposal, err := c.selection.Prepare(ctx, request)
		if err != nil {
			proof.Retire()
			return false, err
		}
		var committed bool
		for {
			var acquired []selection.Acquisition
			for demand := range proposal.Demands() {
				result := selection.Acquisition{Demand: demand, Reads: demand.Reads}
				if demand.History && demand.Root != "" {
					result.Root, result.Available, err = c.loader.ResolveRoot(
						ctx,
						demand.Reads,
						demand.Source,
						demand.Root,
					)
				} else if demand.Occurrence.Identity != (manifest.NamedResource{}) {
					result.Child, err = c.loader.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
					result.Available = result.Child.Obj != nil
				}
				if err != nil {
					proposal.Retire()
					c.Store.Retire(proof)
					return false, err
				}
				acquired = append(acquired, result)
			}
			if len(acquired) != 0 {
				request = proposal.Request
				request.Acquired = acquired
				previous := proposal
				proposal, err = c.selection.Prepare(ctx, request)
				previous.Retire()
				if err != nil {
					c.Store.Retire(proof)
					return false, err
				}
				continue
			}
			proposal, committed, err = c.selection.Commit(ctx, proposal, selection.Execution)
			if err != nil || committed || proposal == nil {
				break
			}
		}
		c.Store.Retire(proof)
		if err != nil {
			return false, err
		}
		if !committed {
			continue
		}
		decision := c.selection.Snapshot()
		selected := len(request.Generation) == 0
		for _, child := range request.Generation {
			if child.Obj != nil {
				c.selection.Include(child.Obj.Named())
				selected = selected || decision.Reconcile(child.Obj.Named())
			}
		}
		return selected, nil
	}
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
