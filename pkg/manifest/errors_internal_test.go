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

	"github.com/home-operations/flate/internal/assert"
)

func TestError_ClassificationAndDetails(t *testing.T) {
	t.Parallel()
	{
		got := ErrorDetails(nil)
		assert.FatalIf(t, got != "", "nil details = %q", got)
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
			testErrorClassificationAndDetailsCase(t, &tc)
		},
		)
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
			testErrorClassificationAndDetailsCase2(
				t,
				&testErrorClassificationAndDetailsCase2Inputs{cause: &cause, tc: &tc},
			)
		},
		)
	}
	for _, category := range []error{ErrInput, ErrObjectNotFound, ErrInvalidValuesReference, ErrInvalidSubstituteReference, ErrMissingSecret, ErrSourceSkipped} {
		t.Run(category.Error(), func(t *testing.T) {
			t.Parallel()
			testErrorClassificationAndDetailsCase3(t, &category)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestError_CategoryTraversalAllocations(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		if !errors.Is(ErrInput, ErrFlux) {
			t.Fatal("lost root classification")
		}
		if _, ok := errors.AsType[*DependencyFailedError](ErrInput); ok {
			t.Fatal("category matched a dependency failure")
		}
	})
	{
		assert.FatalIf(t, allocations != 0, "category traversal allocates %v objects, want 0", allocations)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
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
	{
		assert.FatalIf(t, allocations != 0, "cause traversal allocates %v objects, want 0", allocations)
	}
}

func TestDependencyFailedError_Format(t *testing.T) {
	t.Parallel()
	parent := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "default", Name: "child"}
	depA := NamedResource{
		Group:     FluxKustomizeDomain,
		Kind:      KindKustomization,
		Namespace: "security",
		Name:      "pocket-id-instance",
	}
	depB := NamedResource{
		Group:     FluxKustomizeDomain,
		Kind:      KindKustomization,
		Namespace: "database",
		Name:      "cnpg-cluster",
	}

	err := &DependencyFailedError{
		Parent: parent,
		Failed: []NamedResource{depA, depB},
		Reasons: map[NamedResource]DiagnosticReason{
			depA: {
				Subject: DiagnosticFor(depA, nil),
				Message: `variable "SECRET_DOMAIN" is undefined and has no default`,
			},
			depB: {Subject: DiagnosticFor(depB, nil), Message: "rendering timed out"},
		},
	}
	msg := err.Error()
	{
		assert.ErrorIf(t, !strings.Contains(msg, "dependencies failed:"), "missing prefix: %q", msg)
	}
	{
		assert.ErrorIf(
			t,
			!strings.Contains(msg, depA.String()) || !strings.Contains(msg, depB.String()),
			"missing dependency IDs: %q",
			msg,
		)
	}
	{
		assert.ErrorIf(
			t,
			!strings.Contains(msg, "SECRET_DOMAIN") || !strings.Contains(msg, "timed out"),
			"missing reasons: %q",
			msg,
		)
	}
}

func TestDependencyFailedError_Unwraps(t *testing.T) {
	t.Parallel()
	err := &DependencyFailedError{
		Parent: NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: "x"},
	}
	{
		assert.ErrorIf(t, !errors.Is(err, ErrInput), "expected errors.Is(err, ErrInput) to be true")
	}
	{
		assert.ErrorIf(
			t,
			!errors.Is(err, ErrFlux),
			"expected errors.Is(err, ErrFlux) to be true (ErrInput wraps ErrFlux)",
		)
	}

	{
		_, ok := errors.AsType[*DependencyFailedError](err)
		assert.ErrorIf(t, !ok, "errors.As should match *DependencyFailedError")
	}
}

func TestDependencyFailedError_EmptyFailedList(t *testing.T) {
	t.Parallel()
	err := &DependencyFailedError{
		Parent: NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "ns", Name: "k"},
	}
	{
		msg := err.Error()
		assert.ErrorIf(
			t,
			msg != "dependencies failed",
			"empty-failed error should still mention dependencies failed: %q",
			msg,
		)
	}
}

func testErrorClassificationAndDetailsCase(t *testing.T, testCaseInput *struct {
	name    string
	err     *Error
	causes  []error
	details string
},
) {
	t.Helper()

	if got := testCaseInput.err.Unwrap(); !slices.Equal(got, testCaseInput.causes) {
		t.Fatalf("causes = %v, want %v", got, testCaseInput.causes)
	}
	if got := ErrorDetails(testCaseInput.err); got != testCaseInput.details {
		t.Fatalf("details = %q, want %q", got, testCaseInput.details)
	}
}

type testErrorClassificationAndDetailsCase2Inputs struct {
	cause *error
	tc    *struct {
		name string
		err  error
	}
}

func testErrorClassificationAndDetailsCase2(t *testing.T, testCaseInput *testErrorClassificationAndDetailsCase2Inputs) {
	t.Helper()

	{
		got := ErrorDetails(testCaseInput.tc.err)
		assert.FatalIf(t, !strings.Contains(got, (*testCaseInput.cause).Error()), "lost retained cause: %q", got)
	}
}

func testErrorClassificationAndDetailsCase3(t *testing.T, testCaseInput *error,
) {
	t.Helper()

	underlying := &os.PathError{Op: "read", Path: "library-cause", Err: fs.ErrPermission}
	cause := fmt.Errorf("fetch operation: %w", errors.Join(underlying, context.Canceled))
	for _, message := range []string{"", "actionable cause"} {
		err := NewError((*testCaseInput), cause, message)
		want := message
		if want == "" {
			want = cause.Error()
		}
		{
			assert.FatalIf(t, err.Error() != want, "Error() = %q, want %q", err, want)
		}
		for _, target := range []error{ErrFlux, (*testCaseInput), fs.ErrPermission, context.Canceled} {
			{
				assert.ErrorIf(t, !errors.Is(err, target), "lost classification %v", target)
			}
		}
		{
			got, ok := errors.AsType[*os.PathError](err)
			assert.FatalIf(t, !ok || got != underlying, "%s", "lost typed cause")
		}
		for _, detail := range []string{want, "fetch operation", "library-cause", context.Canceled.Error()} {
			{
				assert.ErrorIf(
					t,
					!strings.Contains(ErrorDetails(err), detail),
					"lost diagnostic detail %q",
					detail,
				)
			}
		}
	}
}
