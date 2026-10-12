package manifest

import (
	"cmp"
	"errors"
	"fmt"
	"strings"
)

// TrimSentinelPrefix removes ErrFlux and a registered sentinel category
// from user-facing messages. Sentinels register their categories at declaration.
func TrimSentinelPrefix(msg string) string {
	prefix := ErrFlux.Error() + ": "
	rest, ok := strings.CutPrefix(msg, prefix)
	if !ok {
		return msg
	}
	category, body, ok := strings.Cut(rest, ": ")
	if !ok {
		return rest
	}
	for _, sentinel := range sentinels {
		if category == strings.TrimPrefix(sentinel.Error(), prefix) {
			return body
		}
	}
	return rest
}

var sentinels []error

func sentinel(category string) error {
	err := fmt.Errorf("%w: "+category, ErrFlux)
	sentinels = append(sentinels, err)
	return err
}

// Sentinel errors. Callers branch on these with errors.Is. Every error
// produced by this module wraps ErrFlux, so a generic
// errors.Is(err, manifest.ErrFlux) classifies any flux-related failure.
var (
	ErrFlux                       = errors.New("flux error")
	ErrInput                      = sentinel("input error")
	ErrObjectNotFound             = sentinel("object not found")
	ErrInvalidValuesReference     = sentinel("invalid values reference")
	ErrInvalidSubstituteReference = sentinel("invalid substitute reference")
	// ErrMissingSecret marks absent or wiped source auth, cert or proxy material.
	// The source is skipped only under producer or --allow-missing-secrets policy;
	// configured TLS/proxy requirements are never discarded for a fetch.
	// Malformed real material remains a failure. Signature refs are ignored.
	ErrMissingSecret = sentinel("missing secret")
	// ErrSourceSkipped signals that a downstream consumer (KS sourceRef,
	// HR chartRef) cannot proceed because its source was skipped —
	// typically by --allow-missing-secrets. Consumers translate this
	// into their own skip rather than a hard failure.
	ErrSourceSkipped = sentinel("source skipped")
)

// inputf formats an input error.
func inputf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInput}, args...)...)
}

// DependencyFailedError signals that a parent resource cannot proceed
// because one of its dependencies has failed.
type DependencyFailedError struct {
	// Parent is the resource whose reconcile is being aborted.
	Parent NamedResource
	// Failed is the ordered list of dependency IDs that failed.
	Failed []NamedResource
	// Reasons maps each failed ID to its underlying message.
	Reasons map[NamedResource]string
}

func (e *DependencyFailedError) Error() string {
	if len(e.Failed) == 0 {
		return fmt.Sprintf("%s: dependencies failed", e.Parent.String())
	}
	parts := make([]string, 0, len(e.Failed))
	for _, f := range e.Failed {
		parts = append(parts, f.String()+": "+cmp.Or(e.Reasons[f], "unknown error"))
	}
	return "dependencies failed: " + strings.Join(parts, "; ")
}

func (*DependencyFailedError) Unwrap() error { return ErrInput }
