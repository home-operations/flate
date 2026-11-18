package base

import (
	"context"
	"errors"
	"log/slog"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

// Acquisition retains credential policy and continuations for acquired content.
type Acquisition struct {
	ProviderOnly bool
	// Fetch acquires content with this execution's captured inputs.
	Fetch func(context.Context) (source.PreparedFetch, error)
	// Select MUST classify discovery-admitted content before remote credential preparation.
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
	p := &pendingAcquisitionError{execution: e, object: obj, acquisition: acquisition}
	defer func() { p.finish(resultErr) }()
	e.Reads.SetAcquisition(store.AcquisitionPending)
	if err := e.Checkpoint(); err != nil {
		return err
	}
	if acquisition.Select != nil {
		local, err := acquisition.Select(ctx)
		if err != nil {
			return err
		}
		p.local = local
		if local {
			return p.advance(ctx, level)
		}
	}
	e.Tasks.YieldSlot(func() { p.prepared, p.cause = acquisition.Fetch(ctx) })
	if errors.Is(p.cause, context.Canceled) || errors.Is(p.cause, context.DeadlineExceeded) {
		return p.cause
	}
	if len(p.prepared.Inputs) != 0 {
		if err := e.Require(ctx, p.prepared.Inputs, level); err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.BlockedError](err); blocked {
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
	local           bool
}

func (p *pendingAcquisitionError) Error() string { return p.cause.Error() }
func (p *pendingAcquisitionError) Unwrap() error {
	if len(p.prepared.Inputs) != 0 || p.missing == nil || p.ambientConsumed {
		return p.cause
	}
	return &depwait.BlockedError{Deps: []manifest.NamedResource{p.missing.Secret}}
}

func (p *pendingAcquisitionError) Settle(ctx context.Context, level manifest.DrainLevel) (resultErr error) {
	e := p.execution
	defer func() { p.finish(resultErr) }()
	if err := e.Checkpoint(); err != nil {
		return err
	}
	if len(p.prepared.Inputs) != 0 {
		if err := e.classify(ctx, level); err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.BlockedError](err); blocked {
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
		p.close()
		assessment := p.assess(p.cause, p.missing.RetryAmbient != nil)
		if assessment.Effect() != values.Deferred {
			return p.complete(ctx, nil, p.cause, level)
		}
		e.Tasks.YieldSlot(func() { p.prepared, p.cause = p.missing.RetryMachine(ctx, &assessment) })
		if p.cause != nil {
			return p.complete(ctx, nil, p.cause, level)
		}
	}
	return p.advance(ctx, level)
}

func (p *pendingAcquisitionError) finish(err error) {
	e := p.execution
	if !errors.Is(err, p) || !e.Reads.Valid() {
		p.close()
	}
	if err == nil || errors.Is(err, manifest.ErrSourceSkipped) {
		return
	}
	if _, blocked := errors.AsType[*depwait.BlockedError](err); !blocked {
		e.Reads.SetAcquisition(store.AcquisitionPending)
	}
}

func (p *pendingAcquisitionError) close() {
	if p.prepared.Close != nil {
		p.prepared.Close()
		p.prepared.Close = nil
	}
}

func (p *pendingAcquisitionError) advance(ctx context.Context, level manifest.DrainLevel) error {
	e := p.execution
	if err := e.Checkpoint(); err != nil {
		return err
	}
	e.Reads.SetAcquisition(store.AcquisitionPrepared)
	var local *store.SourceArtifact
	if p.acquisition.Resolve != nil {
		var err error
		local, err = p.acquisition.Resolve(ctx, level)
		if err != nil {
			p.cause = err
			if _, blocked := errors.AsType[*depwait.BlockedError](err); blocked {
				return p
			}
			return err
		}
	}
	if err := e.Checkpoint(); err != nil {
		return err
	}
	// Discovery-admitted content MUST bypass remote credential settlement and acquisition.
	if p.local {
		return p.complete(ctx, local, nil, level)
	}
	if local != nil && p.prepared.LocalUnavailable != nil {
		return p.complete(ctx, nil, p.prepared.LocalUnavailable, level)
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
		assessment := p.assess(err, false)
		if assessment.Effect() != values.Omitted {
			return assessment.Err()
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

func (p *pendingAcquisitionError) assess(err error, ambient bool) values.Result {
	request := values.ResolveRequest{Demand: values.SourceDemand, Failure: err}
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok {
		request = missing.Input
		request.Reference.NamedResource = missing.Secret
		request.Reference.Kind = manifest.KindSecret
		request.Demand = values.SourceDemand
		request.Unavailable = err
		request.Ambient = ambient
		request.AnonymousDenied = missing.AnonymousDenied
		if missing.Cause != nil && !missing.AnonymousDenied {
			request.Failure = err
		}
	} else if failure, ok := errors.AsType[*manifest.Error](err); ok &&
		errors.Is(failure.Category(), manifest.ErrMissingSecret) && failure.Cause() == nil {
		request.Failure, request.Unavailable = nil, err
		request.Reference.Kind = manifest.KindSecret
	}
	request.Permission = p.execution.Missing
	request.ProviderOnly = p.acquisition.ProviderOnly
	return values.Resolve(&request)
}
