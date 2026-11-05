package base

import (
	"context"
	"errors"
	"log/slog"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Acquisition retains credential policy and continuations for acquired content.
type Acquisition struct {
	// AllowMissingSecrets permits unavailable authored Secret material.
	AllowMissingSecrets bool
	// AllowAnonymousDenied permits a proven registry denial without a credential.
	AllowAnonymousDenied bool
	// Fetch acquires content with this execution's captured inputs.
	Fetch   func(context.Context) (source.PreparedFetch, error)
	Select  func(context.Context) (bool, error)
	Resolve func(context.Context, manifest.DrainLevel) (*store.SourceArtifact, error)
	// Complete receives acquired content or an allowed unavailable-material error.
	Complete func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error
}

// Acquire fetches content or retains credential settlement before completing its consumer.
func (e *Execution) Acquire(
	ctx context.Context,
	obj manifest.BaseManifest,
	acquisition Acquisition,
	level manifest.DrainLevel,
) (resultErr error) {
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, manifest.ErrSourceSkipped) {
			if _, blocked := errors.AsType[*depwait.ErrBlocked](resultErr); !blocked {
				e.Reads.SetAcquisition(store.AcquisitionPending)
			}
		}
	}()
	e.Reads.SetAcquisition(store.AcquisitionPending)
	if err := e.Checkpoint(); err != nil {
		return err
	}
	p := &pendingAcquisitionError{execution: e, object: obj, acquisition: acquisition}
	e.Tasks.YieldSlot(func() { p.prepared, p.cause = acquisition.Fetch(ctx) })
	if errors.Is(p.cause, context.Canceled) || errors.Is(p.cause, context.DeadlineExceeded) {
		return p.cause
	}
	if len(p.prepared.Inputs) != 0 {
		if err := e.Require(ctx, p.prepared.Inputs, level); err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
				return p
			}
			return err
		}
		return e.Checkpoint()
	}
	if missing, ok := errors.AsType[*source.MissingSecretError](p.cause); ok {
		p.missing = missing
		if level == manifest.DrainNone && missing.Secret != (manifest.NamedResource{}) {
			e.Store.UpdateStatus(
				e.Reads.ID(),
				store.StatusInfo{Status: store.StatusPending, Message: "waiting for source Secret"},
			)
			return p
		}
		return p.Settle(ctx, level)
	}
	if p.cause != nil {
		return p.complete(ctx, nil, p.cause, level)
	}
	return p.advance(ctx, level)
}

type pendingAcquisitionError struct {
	execution       *Execution
	object          manifest.BaseManifest
	acquisition     Acquisition
	prepared        source.PreparedFetch
	missing         *source.MissingSecretError
	cause           error
	ambientConsumed bool
	selected        bool
	local           bool
}

func (p *pendingAcquisitionError) Error() string { return p.cause.Error() }
func (p *pendingAcquisitionError) Unwrap() error {
	if len(p.prepared.Inputs) != 0 || p.missing == nil || p.ambientConsumed {
		return p.cause
	}
	return &depwait.ErrBlocked{Deps: []manifest.NamedResource{p.missing.Secret}}
}

func (p *pendingAcquisitionError) Settle(ctx context.Context, level manifest.DrainLevel) (resultErr error) {
	e := p.execution
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, manifest.ErrSourceSkipped) {
			if _, blocked := errors.AsType[*depwait.ErrBlocked](resultErr); !blocked {
				e.Reads.SetAcquisition(store.AcquisitionPending)
			}
		}
	}()
	if err := e.Checkpoint(); err != nil {
		return err
	}
	if len(p.prepared.Inputs) != 0 {
		if err := e.classify(ctx, level); err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
				return p
			}
			return err
		}
		return e.Checkpoint()
	}
	if p.missing != nil && !p.ambientConsumed {
		if level == manifest.DrainNone && p.missing.Secret != (manifest.NamedResource{}) {
			return p
		}
		p.ambientConsumed = true
		e.Tasks.YieldSlot(func() { p.prepared, p.cause = p.missing.RetryMachine(ctx, &e.Reads) })
		if p.cause != nil {
			return p.complete(ctx, nil, p.cause, level)
		}
	}
	return p.advance(ctx, level)
}

func (p *pendingAcquisitionError) advance(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	if err := e.Checkpoint(); err != nil {
		return err
	}
	if !p.selected {
		if p.acquisition.Select != nil {
			local, err := p.acquisition.Select(ctx)
			if err != nil {
				return err
			}
			p.local = local
		}
		p.selected = true
	}
	if p.local && p.prepared.LocalUnavailable != nil {
		return p.complete(ctx, nil, p.prepared.LocalUnavailable, level)
	}
	e.Reads.SetAcquisition(store.AcquisitionPrepared)
	var local *store.SourceArtifact
	if p.acquisition.Resolve != nil {
		var err error
		local, err = p.acquisition.Resolve(ctx, level)
		if err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.ErrBlocked](err); blocked {
				return p
			}
			return err
		}
	}
	if err := e.Checkpoint(); err != nil {
		return err
	}
	var artifact *store.SourceArtifact
	var err error
	e.Tasks.YieldSlot(func() { artifact, err = p.prepared.Fetch(ctx, local) })
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && !p.ambientConsumed {
		p.missing, p.cause = missing, err
		return p.Settle(ctx, level)
	}
	return p.complete(ctx, artifact, err, level)
}

func (p *pendingAcquisitionError) complete(
	ctx context.Context,
	artifact *store.SourceArtifact,
	err error,
	level manifest.DrainLevel,
) error {
	e, obj := p.execution, p.object
	if err != nil && p.missing != nil && !errors.Is(err, p.missing) {
		err = manifest.NewError(nil, err, p.missing.Error())
	}
	if err != nil {
		missing, hasMissing := errors.AsType[*source.MissingSecretError](err)
		anonymousDenied := hasMissing && p.acquisition.AllowAnonymousDenied && missing.AnonymousDenied &&
			missing.Secret == (manifest.NamedResource{})
		allowed := p.acquisition.AllowMissingSecrets || anonymousDenied ||
			hasMissing && missing.ProducerBacked(&e.Reads)
		if !errors.Is(err, manifest.ErrMissingSecret) || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			!allowed {
			return err
		}
		if e.Reads.Valid() {
			slog.Info(
				"base: credentials unavailable",
				"id",
				manifest.DiagnosticFor(obj.Named(), obj).String(),
				"err",
				err,
			)
		}
		var related []manifest.NamedResource
		if hasMissing && missing.Secret != (manifest.NamedResource{}) {
			related = []manifest.NamedResource{missing.Secret}
		}
		e.Reads.SetAcquisition(store.AcquisitionSkipped)
		e.Reads.AddWarning(manifest.Warning{
			Resource: obj.Named(),
			Related:  related,
			Subject:  manifest.DiagnosticFor(obj.Named(), obj),
			Category: manifest.WarnSourceCredentialsUnavailable,
			Message:  err.Error(),
		})
	}
	checkpoint := e.Checkpoint()
	result := p.acquisition.Complete(ctx, artifact, err, level)
	if checkpoint != nil {
		return checkpoint
	}
	if err != nil && result == nil {
		e.Reads.SetAcquisition(store.AcquisitionPending)
	}
	return result
}
