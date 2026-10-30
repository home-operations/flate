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
	Fetch func(context.Context) (*store.SourceArtifact, error)
	// Complete receives acquired content or an allowed unavailable-material error.
	Complete func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error
}

// Acquire fetches content or retains credential settlement before completing its consumer.
func (e *Execution) Acquire(ctx context.Context, obj manifest.BaseManifest, acquisition Acquisition, level manifest.DrainLevel) error {
	var artifact *store.SourceArtifact
	var err error
	e.Tasks.YieldSlot(func() { artifact, err = acquisition.Fetch(ctx) })
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok {
		pending := &pendingAcquisition{execution: e, object: obj, acquisition: acquisition, missing: missing}
		if level == manifest.DrainNone && missing.Secret != (manifest.NamedResource{}) {
			e.Store.UpdateStatus(e.Reads.ID(), store.StatusInfo{Status: store.StatusPending, Message: "waiting for source Secret"})
			return pending
		}
		return pending.Settle(ctx, level)
	}
	return (&pendingAcquisition{execution: e, object: obj, acquisition: acquisition}).complete(ctx, artifact, err, level)
}

type pendingAcquisition struct {
	execution   *Execution
	object      manifest.BaseManifest
	acquisition Acquisition
	missing     *source.MissingSecretError
}

func (p *pendingAcquisition) Error() string { return p.missing.Error() }
func (p *pendingAcquisition) Unwrap() error {
	return &depwait.ErrBlocked{Deps: []manifest.NamedResource{p.missing.Secret}}
}
func (p *pendingAcquisition) Settle(ctx context.Context, level manifest.DrainLevel) error {
	var artifact *store.SourceArtifact
	var err error
	p.execution.Tasks.YieldSlot(func() { artifact, err = p.missing.RetryMachine(ctx, &p.execution.Reads) })
	if err != nil && !errors.Is(err, p.missing) {
		err = manifest.NewError(nil, err, p.missing.Error())
	}
	return p.complete(ctx, artifact, err, level)
}

func (p *pendingAcquisition) complete(ctx context.Context, artifact *store.SourceArtifact, err error, level manifest.DrainLevel) error {
	e, obj := p.execution, p.object
	if err != nil {
		missing, hasMissing := errors.AsType[*source.MissingSecretError](err)
		anonymousDenied := hasMissing && p.acquisition.AllowAnonymousDenied && missing.AnonymousDenied && missing.Secret == (manifest.NamedResource{})
		allowed := p.acquisition.AllowMissingSecrets || anonymousDenied || hasMissing && missing.ProducerBacked(&e.Reads)
		if !errors.Is(err, manifest.ErrMissingSecret) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || !allowed {
			return err
		}
		if e.Reads.Valid() {
			slog.Info("base: credentials unavailable", "id", manifest.DiagnosticFor(obj.Named(), obj).String(), "err", err)
		}
		var related []manifest.NamedResource
		if hasMissing && missing.Secret != (manifest.NamedResource{}) {
			related = []manifest.NamedResource{missing.Secret}
		}
		e.Reads.AddWarning(manifest.Warning{Resource: obj.Named(), Related: related, Subject: manifest.DiagnosticFor(obj.Named(), obj), Category: manifest.WarnSourceCredentialsUnavailable,
			Message: err.Error()})
	}
	return p.acquisition.Complete(ctx, artifact, err, level)
}
