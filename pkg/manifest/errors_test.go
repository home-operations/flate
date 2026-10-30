package manifest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestError_ClassificationAndDetails(t *testing.T) {
	t.Parallel()
	if got := ErrorDetails(nil); got != "" {
		t.Fatalf("nil details = %q", got)
	}
	cause := errors.New("cause")
	for _, tc := range []struct {
		name    string
		err     *Error
		causes  []error
		details string
	}{
		{"cause only", NewError(nil, cause, "summary"), []error{cause}, "summary\n  cause"},
		{"category only", NewError(ErrInput, nil, "summary"), []error{ErrInput}, "summary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.err.Unwrap(); !slices.Equal(got, tc.causes) {
				t.Fatalf("causes = %v, want %v", got, tc.causes)
			}
			if got := ErrorDetails(tc.err); got != tc.details {
				t.Fatalf("details = %q, want %q", got, tc.details)
			}
		})
	}
	summary := NewError(ErrInput, cause, "summary")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"wrapped summary", fmt.Errorf("operation: %w", summary)},
		{"joined summary", errors.Join(errors.New("other"), summary)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ErrorDetails(tc.err); !strings.Contains(got, cause.Error()) {
				t.Fatalf("lost retained cause: %q", got)
			}
		})
	}
	for _, category := range []error{ErrInput, ErrObjectNotFound, ErrInvalidValuesReference, ErrInvalidSubstituteReference, ErrMissingSecret, ErrSourceSkipped} {
		t.Run(category.Error(), func(t *testing.T) {
			t.Parallel()
			underlying := &os.PathError{Op: "read", Path: "library-cause", Err: fs.ErrPermission}
			cause := fmt.Errorf("fetch operation: %w", errors.Join(underlying, context.Canceled))
			for _, message := range []string{"", "actionable cause"} {
				err := NewError(category, cause, message)
				want := message
				if want == "" {
					want = cause.Error()
				}
				if err.Error() != want {
					t.Fatalf("Error() = %q, want %q", err, want)
				}
				for _, target := range []error{ErrFlux, category, fs.ErrPermission, context.Canceled} {
					if !errors.Is(err, target) {
						t.Errorf("lost classification %v", target)
					}
				}
				if got, ok := errors.AsType[*os.PathError](err); !ok || got != underlying {
					t.Fatal("lost typed cause")
				}
				for _, detail := range []string{want, "fetch operation", "library-cause", context.Canceled.Error()} {
					if !strings.Contains(ErrorDetails(err), detail) {
						t.Errorf("lost diagnostic detail %q", detail)
					}
				}
			}
		})
	}
}

func TestError_CategoryTraversalAllocations(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		if !errors.Is(ErrInput, ErrFlux) {
			t.Fatal("lost root classification")
		}
		if _, ok := errors.AsType[*DependencyFailedError](ErrInput); ok {
			t.Fatal("category matched a dependency failure")
		}
	})
	if allocations != 0 {
		t.Fatalf("category traversal allocates %v objects, want 0", allocations)
	}
}

func TestError_CauseTraversalAllocations(t *testing.T) {
	cause := errors.New("failure")
	err := NewError(ErrInput, cause, "")
	allocations := testing.AllocsPerRun(100, func() {
		if !errors.Is(err, ErrFlux) || !errors.Is(err, cause) {
			t.Fatal("lost classification or cause")
		}
		if _, ok := errors.AsType[*DependencyFailedError](err); ok {
			t.Fatal("cause matched a dependency failure")
		}
	})
	if allocations != 0 {
		t.Fatalf("cause traversal allocates %v objects, want 0", allocations)
	}
}

func TestInputf_Allocations(t *testing.T) {
	var err error
	allocations := testing.AllocsPerRun(100, func() {
		err = inputf("missing %s", "kind")
	})
	if allocations != 2 {
		t.Fatalf("input error allocates %v objects, want 2", allocations)
	}
	if !errors.Is(err, ErrInput) || err.Error() != "missing kind" {
		t.Fatalf("input error = %v", err)
	}
}

func TestDependencyFailedError_Format(t *testing.T) {
	t.Parallel()
	parent := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "default", Name: "child"}
	depA := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "security", Name: "pocket-id-instance"}
	depB := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "database", Name: "cnpg-cluster"}

	err := &DependencyFailedError{
		Parent: parent,
		Failed: []NamedResource{depA, depB},
		Reasons: map[NamedResource]DiagnosticReason{
			depA: {Subject: DiagnosticFor(depA, nil), Message: `variable "SECRET_DOMAIN" is undefined and has no default`},
			depB: {Subject: DiagnosticFor(depB, nil), Message: "rendering timed out"},
		},
	}
	msg := err.Error()
	if !strings.Contains(msg, "dependencies failed:") {
		t.Errorf("missing prefix: %q", msg)
	}
	if !strings.Contains(msg, depA.String()) || !strings.Contains(msg, depB.String()) {
		t.Errorf("missing dependency IDs: %q", msg)
	}
	if !strings.Contains(msg, "SECRET_DOMAIN") || !strings.Contains(msg, "timed out") {
		t.Errorf("missing reasons: %q", msg)
	}
}

func TestDependencyFailedError_Unwraps(t *testing.T) {
	t.Parallel()
	err := &DependencyFailedError{
		Parent: NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "x"},
	}
	if !errors.Is(err, ErrInput) {
		t.Errorf("expected errors.Is(err, ErrInput) to be true")
	}
	if !errors.Is(err, ErrFlux) {
		t.Errorf("expected errors.Is(err, ErrFlux) to be true (ErrInput wraps ErrFlux)")
	}

	if _, ok := errors.AsType[*DependencyFailedError](err); !ok {
		t.Errorf("errors.As should match *DependencyFailedError")
	}
}

func TestDependencyFailedError_EmptyFailedList(t *testing.T) {
	t.Parallel()
	err := &DependencyFailedError{
		Parent: NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "ns", Name: "k"},
	}
	if msg := err.Error(); msg != "dependencies failed" {
		t.Errorf("empty-failed error should still mention dependencies failed: %q", msg)
	}
}
