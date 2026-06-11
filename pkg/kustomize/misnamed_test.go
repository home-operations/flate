package kustomize

import (
	"context"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// TestRenderFlux_MisnamedKustomization confirms a misspelled kustomization file
// (a kustomize config the auto-generate scan would otherwise try to decode as a
// resource and fail on "missing metadata.name") yields an actionable error
// naming the file and the rename, instead of the cryptic decode error.
func TestRenderFlux_MisnamedKustomization(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomzation.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n- cm.yaml\n")
	testutil.WriteFile(t, root, "app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata: {k: v}\n")

	_, err := RenderFlux(context.Background(), NewTreeCache(), root, false, "app", fluxKS())
	if err == nil {
		t.Fatal("expected an error for the misnamed kustomization file")
	}
	s := err.Error()
	if !strings.Contains(s, "misnamed kustomization") || !strings.Contains(s, "kustomzation.yaml") {
		t.Errorf("want an actionable misnamed-kustomization hint naming the file, got: %v", err)
	}
	if !strings.Contains(s, "kustomization.yaml") {
		t.Errorf("error should point at the correct filename: %v", err)
	}
}
