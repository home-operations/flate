package manifest

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// DiagnosticIdentity retains execution identity and its frozen display context.
type DiagnosticIdentity struct {
	ID       NamedResource
	Resource NamedResource
	Path     string
}

type diagnosticSubject interface {
	diagnostic(NamedResource) DiagnosticIdentity
}

// DiagnosticFor resolves display context from a captured immutable declaration.
// It MUST remain inlineable so concrete callers retain stack allocation.
func DiagnosticFor(id NamedResource, obj BaseManifest) DiagnosticIdentity {
	if source, ok := obj.(diagnosticSubject); ok {
		return source.diagnostic(id)
	}
	return DiagnosticIdentity{ID: id, Resource: id}
}

// String formats a resource identity with its optional entry path.
func (d DiagnosticIdentity) String() string {
	if d.Resource == (NamedResource{}) {
		return fmt.Sprintf("entry path %q", d.Path)
	}
	if d.Path != "" {
		return fmt.Sprintf("%s (entry path %q)", d.Resource, d.Path)
	}
	return d.Resource.String()
}

// Label formats a source label with its optional entry path.
func (d DiagnosticIdentity) Label() string {
	if d.Resource == (NamedResource{}) || d.Path != "" {
		return d.String()
	}
	return d.Resource.Kind + " " + d.Resource.NamespacedName()
}

// DiagnosticReason captures a leaf failure or ordered dependency causes.
type DiagnosticReason struct {
	Subject DiagnosticIdentity
	Message string
	Causes  []DiagnosticReason
}

// String formats captured context without consulting live state.
func (r DiagnosticReason) String() string {
	message := cmp.Or(r.Message, "unknown error")
	if len(r.Causes) != 0 {
		parts := make([]string, len(r.Causes))
		for i, cause := range r.Causes {
			parts[i] = cause.String()
		}
		message = "dependencies failed: " + strings.Join(parts, "; ")
	}
	return r.Subject.String() + ": " + message
}

// Clone detaches all mutable cause slices.
func (r DiagnosticReason) Clone() DiagnosticReason {
	r.Causes = slices.Clone(r.Causes)
	for i := range r.Causes {
		r.Causes[i] = r.Causes[i].Clone()
	}
	return r
}

// Compare orders semantic reasons by raw identity, message and nested causes.
func (r DiagnosticReason) Compare(other DiagnosticReason) int {
	return cmp.Or(r.Subject.ID.Compare(other.Subject.ID), cmp.Compare(r.Message, other.Message), slices.CompareFunc(r.Causes, other.Causes, DiagnosticReason.Compare))
}

func (k *Kustomization) diagnostic(id NamedResource) DiagnosticIdentity {
	return capturedDiagnostic(id, k.diagnosticOwner, k.diagnosticPath, k.hasDiagnostic)
}

func (h *HelmChartSource) diagnostic(id NamedResource) DiagnosticIdentity {
	return capturedDiagnostic(id, h.diagnosticOwner, h.diagnosticPath, h.hasDiagnostic)
}

func (o *OCIRepository) diagnostic(id NamedResource) DiagnosticIdentity {
	return capturedDiagnostic(id, o.diagnosticOwner, o.diagnosticPath, o.hasDiagnostic)
}

func capturedDiagnostic(id, resource NamedResource, path string, captured bool) DiagnosticIdentity {
	if captured {
		return DiagnosticIdentity{ID: id, Resource: resource, Path: path}
	}
	return DiagnosticIdentity{ID: id, Resource: id}
}
