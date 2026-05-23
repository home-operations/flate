package manifest

import (
	"errors"
	"fmt"
	"strings"
)

// TrimSentinelPrefix removes the noisy `flux error: <subcategory>: `
// chain produced by sentinel-wrapped errors (ErrFlux → ErrInput /
// ErrObjectNotFound / …) so user-facing messages lead with the
// actual cause. Idempotent — strings that don't start with the
// prefix, or whose subcategory isn't one of the known sentinels,
// pass through unchanged.
//
// Lives next to the sentinels themselves so the whitelist stays
// in sync: adding a new ErrFoo here means updating one list, not
// hunting through orchestrator code.
func TrimSentinelPrefix(msg string) string {
	const prefix = "flux error: "
	if !strings.HasPrefix(msg, prefix) {
		return msg
	}
	rest := msg[len(prefix):]
	// One more level: `<subcategory>: <body>`. Strip subcategory only
	// when it matches a known sentinel wrap so colon-containing
	// non-sentinel messages aren't mangled.
	i := strings.Index(rest, ": ")
	if i <= 0 {
		return rest
	}
	switch rest[:i] {
	case "input error",
		"object not found",
		"invalid values reference",
		"invalid substitute reference",
		"command error":
		return rest[i+2:]
	}
	return rest
}

// Sentinel errors. Callers branch on these with errors.Is. Every error
// produced by this module wraps ErrFlux, so a generic
// errors.Is(err, manifest.ErrFlux) classifies any flux-related failure.
var (
	ErrFlux                       = errors.New("flux error")
	ErrInput                      = fmt.Errorf("%w: input error", ErrFlux)
	ErrObjectNotFound             = fmt.Errorf("%w: object not found", ErrFlux)
	ErrInvalidValuesReference     = fmt.Errorf("%w: invalid values reference", ErrFlux)
	ErrInvalidSubstituteReference = fmt.Errorf("%w: invalid substitute reference", ErrFlux)
	ErrCommand                    = fmt.Errorf("%w: command error", ErrFlux)
)

// inputf formats an input error.
func inputf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInput}, args...)...)
}

// ResourceFailedError signals that a reconciliation entered a terminal
// failed state.
type ResourceFailedError struct {
	Resource string
	Reason   string
}

func (e *ResourceFailedError) Error() string {
	reason := e.Reason
	if reason == "" {
		reason = "unknown error"
	}
	return fmt.Sprintf("resource %s failed: %s", e.Resource, reason)
}

func (*ResourceFailedError) Unwrap() error { return ErrFlux }

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
		reason := e.Reasons[f]
		if reason == "" {
			reason = "unknown error"
		}
		parts = append(parts, f.String()+": "+reason)
	}
	return "dependencies failed: " + strings.Join(parts, "; ")
}

func (*DependencyFailedError) Unwrap() error { return ErrInput }
