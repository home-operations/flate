package cache

import (
	"context"
	"errors"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

type attemptHistory struct {
	previous *entry
	lastGood ocispec.Descriptor
	count    uint32
	due      bool
}

func (e *entry) attemptHistory() attemptHistory {
	h := attemptHistory{previous: e}
	if e == nil {
		return h
	}
	h.lastGood = e.stored
	if e.outcome == nil {
		return h
	}
	// acquireOnce observes this outcome under the same key lock.
	h.due = !e.outcome.invalid
	h.lastGood = e.outcome.lastGood
	if h.due {
		h.count = e.outcome.record.Count
	}
	return h
}

func (c *Cache) attempt(
	ctx context.Context,
	keyValue *acquireKey,
	e *entry,
	callbacks acquisitionCallbacks,
) (*entry, error) {
	key := *keyValue
	if err := ctx.Err(); err != nil {
		return nil, &CacheError{err}
	}
	history := e.attemptHistory()
	if history.due {
		key.policy.Retry.Attempts = 1
		ctx = context.WithValue(ctx, attemptCeilingKey{}, true)
	}
	result, err := c.fill(ctx, &key, e, callbacks)
	if result != nil && result.resolved.IsZero() {
		return result, err
	}
	if err != nil {
		return history.failed(ctx, c, &key, err)
	}
	e.waveResult = result
	c.entries.Store(key.id, result)
	return result, nil
}

func classifyAttemptFailure(err error, attempts int) error {
	// A joined cleanup failure must retain cache failure attribution.
	if producer, ok := errors.AsType[*producerError](err); !ok || !errors.Is(producer, err) {
		return &CacheError{err}
	}
	if _, terminal := errors.AsType[*TerminalError](err); !terminal && retryable(err) && attempts <= 1 {
		return &TerminalError{Err: err, Attempted: true}
	}
	return err
}

func (h *attemptHistory) failed(ctx context.Context, c *Cache, key *acquireKey, err error) (*entry, error) {
	err = classifyAttemptFailure(err, key.policy.Retry.Attempts)
	// Caller cancellation must retain the previous generation and its freshness.
	if ctx.Err() != nil || h.due && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return nil, err
	}
	next := &entry{stored: h.lastGood}
	if h.due {
		next.stored, next.outcome = h.previous.stored, h.previous.outcome
	}
	stored, o, saveErr := h.persistOutcome(ctx, c, key, err)
	if saveErr != nil {
		err = saveErr
	} else if stored.Digest != "" {
		next.stored, next.outcome = stored, o
	}
	// Retry and deadline failures must retain each caller's policy.
	shareFailure := !retryable(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	if _, terminal := errors.AsType[*TerminalError](err); h.due || shareFailure && !terminal {
		h.previous.waveErr = err
	}
	c.entries.Store(key.id, next)
	return nil, err
}

func (h *attemptHistory) persistOutcome(
	ctx context.Context,
	c *Cache,
	key *acquireKey,
	err error,
) (ocispec.Descriptor, *outcome, error) {
	if c.store == nil || key.memoryOnly || ctx.Err() != nil {
		return ocispec.Descriptor{}, nil, nil //nolint:nilerr // Only producer failures are eligible for a persisted negative outcome.
	}
	if _, producer := errors.AsType[*producerError](err); !producer {
		return ocispec.Descriptor{}, nil, nil
	}
	o := newOutcome(err, time.Now().UTC(), nextOutcomeCount(h.count), &h.lastGood)
	if o == nil {
		return ocispec.Descriptor{}, nil, nil
	}
	stored, saveErr := c.publishOutcome(ctx, key.id, o, err)
	return stored, o, saveErr
}

func (c *Cache) acceptedCandidate(ctx context.Context, key *acquireKey,
	validate func(context.Context, *entry) (bool, error),
) (*entry, error) {
	if key.accepted == "" {
		return nil, nil //nolint:nilnil // No accepted key means that ordinary acquisition continues.
	}
	generation, descriptor, _, err := c.Blob(
		ctx,
		BlobKey{Content: key.accepted, Render: RenderAccepted},
		Policy{MaxAge: Forever},
		nil,
	)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, nil //nolint:nilnil // An absent accepted generation permits ordinary acquisition.
		}
		return nil, err
	}
	accepted := &entry{fs: generation, descriptor: descriptor}
	valid, err := c.validCandidate(ctx, validate, accepted)
	if err != nil || valid {
		return accepted, err
	}
	return nil, nil //nolint:nilnil // A rejected accepted generation permits ordinary acquisition.
}

func (c *Cache) waveCandidate(ctx context.Context, key *acquireKey, observed *entry,
	validate func(context.Context, *entry) (bool, error),
) (*entry, *entry, error) {
	if observed == nil || key.lookupOnly {
		return nil, nil, nil
	} //nolint:nilnil // Lookup-only requests do not reuse an acquisition wave.
	if observed.waveErr != nil {
		if observed.outcome != nil {
			return nil, nil, &TerminalError{Err: observed.waveErr, Attempted: false}
		}
		return nil, nil, observed.waveErr
	}
	if observed.waveResult == nil {
		return nil, nil, nil
	} //nolint:nilnil // No completed wave leaves the stored candidate eligible.
	valid, err := c.validCandidate(ctx, validate, observed.waveResult)
	if err != nil || valid {
		return observed.waveResult, nil, err
	}
	return nil, observed.waveResult, nil //nolint:nilnil // The rejected wave must be excluded from the stored-candidate check.
}

func (c *Cache) freshCandidate(ctx context.Context, key *acquireKey, e *entry,
	validate func(context.Context, *entry) (bool, error),
) (*entry, error) {
	if err := c.touch(&*key, e); err != nil {
		if !key.lookupOnly {
			c.entries.Store(key.id, &entry{stored: e.stored})
		}
		return nil, err
	}
	valid, err := c.validCandidate(ctx, validate, e)
	if err != nil || valid {
		return e, err
	}
	return nil, nil //nolint:nilnil // Validation rejection starts a new acquisition.
}
