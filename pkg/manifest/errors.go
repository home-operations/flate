package manifest

import (
	"errors"
	"fmt"
	"strings"
)

// Error separates domain classification from the actionable cause.
type Error struct {
	causes  [2]error
	message string
}

// NewError creates a classified error with an optional concise message.
func NewError(category, cause error, message string) *Error {
	return &Error{causes: [2]error{category, cause}, message: message}
}

// Category returns the classification sentinel.
func (e *Error) Category() error { return e.causes[0] }

// Cause returns the retained underlying error.
func (e *Error) Cause() error { return e.causes[1] }

// Error returns the explicit summary or the retained cause.
func (e *Error) Error() string {
	if e.message != "" {
		return e.message
	}
	return e.causes[1].Error()
}

// Unwrap exposes classification and retained causes to structural traversal.
func (e *Error) Unwrap() []error {
	start, end := 0, len(e.causes)
	if e.causes[0] == nil {
		start++
	}
	if e.causes[1] == nil {
		end--
	}
	return e.causes[start:end]
}

// ErrorDetails formats retained causes, including details hidden by concise summaries.
func ErrorDetails(err error) string {
	var out strings.Builder
	var visit func(error, int)
	visit = func(err error, depth int) {
		if err == nil {
			return
		}
		if out.Len() > 0 {
			out.WriteByte('\n')
		}
		out.WriteString(strings.Repeat("  ", depth))
		out.WriteString(err.Error())
		switch e := err.(type) { //nolint:errorlint // Each concrete node contributes its own retained cause.
		case *Error:
			visit(e.causes[1], depth+1)
		case interface{ Unwrap() []error }:
			for _, cause := range e.Unwrap() {
				visit(cause, depth+1)
			}
		case interface{ Unwrap() error }:
			visit(e.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	return out.String()
}

// Sentinel errors. Callers branch on these with errors.Is. Every error
// produced by this module wraps ErrFlux, so a generic
// errors.Is(err, manifest.ErrFlux) classifies any flux-related failure.
var (
	ErrFlux                             = errors.New("flux error")
	ErrInput                      error = NewError(ErrFlux, nil, "input error")
	ErrObjectNotFound             error = NewError(ErrFlux, nil, "object not found")
	ErrInvalidValuesReference     error = NewError(ErrFlux, nil, "invalid values reference")
	ErrInvalidSubstituteReference error = NewError(ErrFlux, nil, "invalid substitute reference")
	// ErrMissingSecret marks absent or wiped source auth, cert or proxy material.
	// The source is skipped only under producer or --allow-missing-kinds Secret policy;
	// configured TLS/proxy requirements are never discarded for a fetch.
	// Malformed real material remains a failure. Signature refs are ignored.
	ErrMissingSecret error = NewError(ErrFlux, nil, "missing secret")
	// ErrSourceSkipped signals that a downstream consumer (KS sourceRef,
	// HR chartRef) cannot proceed because its source was skipped —
	// typically by --allow-missing-kinds Secret. Consumers translate this
	// into their own skip rather than a hard failure.
	ErrSourceSkipped error = NewError(ErrFlux, nil, "source skipped")
)

func inputf(format string, args ...any) error {
	return NewError(ErrInput, nil, fmt.Sprintf(format, args...))
}

// DependencyFailedError signals that a parent resource cannot proceed
// because one of its dependencies has failed.
type DependencyFailedError struct {
	// Parent is the resource whose reconcile is being aborted.
	Parent NamedResource
	// Failed is the ordered list of dependency IDs that failed.
	Failed []NamedResource
	// Reasons maps each raw dependency to its captured diagnostic tree.
	Reasons map[NamedResource]DiagnosticReason
	// Subject is the captured parent display identity.
	Subject DiagnosticIdentity
}

func (e *DependencyFailedError) Error() string {
	if len(e.Failed) == 0 {
		return "dependencies failed"
	}
	parts := make([]string, 0, len(e.Failed))
	for _, f := range e.Failed {
		reason := e.Reasons[f]
		if reason.Subject.ID == (NamedResource{}) {
			reason.Subject = DiagnosticFor(f, nil)
		}
		parts = append(parts, reason.String())
	}
	return "dependencies failed: " + strings.Join(parts, "; ")
}

func (*DependencyFailedError) Unwrap() error { return ErrInput }
