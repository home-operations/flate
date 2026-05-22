package manifest

import (
	"errors"
	"strings"
	"testing"
)

func TestDependencyFailedError_Format(t *testing.T) {
	parent := NamedResource{Kind: KindKustomization, Namespace: "default", Name: "child"}
	depA := NamedResource{Kind: KindKustomization, Namespace: "security", Name: "pocket-id-instance"}
	depB := NamedResource{Kind: KindKustomization, Namespace: "database", Name: "cnpg-cluster"}

	err := &DependencyFailedError{
		Parent: parent,
		Failed: []NamedResource{depA, depB},
		Reasons: map[NamedResource]string{
			depA: `variable "SECRET_DOMAIN" is undefined and has no default`,
			depB: "rendering timed out",
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
	err := &DependencyFailedError{
		Parent: NamedResource{Kind: KindKustomization, Name: "x"},
	}
	if !errors.Is(err, ErrInput) {
		t.Errorf("expected errors.Is(err, ErrInput) to be true")
	}
	if !errors.Is(err, ErrFlux) {
		t.Errorf("expected errors.Is(err, ErrFlux) to be true (ErrInput wraps ErrFlux)")
	}

	var typed *DependencyFailedError
	if !errors.As(err, &typed) {
		t.Errorf("errors.As should match *DependencyFailedError")
	}
}

func TestDependencyFailedError_EmptyFailedList(t *testing.T) {
	err := &DependencyFailedError{
		Parent: NamedResource{Kind: KindKustomization, Namespace: "ns", Name: "k"},
	}
	if msg := err.Error(); !strings.Contains(msg, "dependencies failed") {
		t.Errorf("empty-failed error should still mention dependencies failed: %q", msg)
	}
}
