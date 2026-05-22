package loader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_Load(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  k: v
`)
	testutil.WriteFile(t, dir, "README.md", "ignored")

	s := store.New()
	n, err := New(s).Load(t.Context(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 objects, got %d", n)
	}
	if got := len(s.ListObjects(manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
}

func TestLoader_SkipsTemplatesDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "chart", "templates"), 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `{{ if .Values.x }}foo: bar{{ end }}`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	n, err := New(store.New()).Load(t.Context(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 (templates skipped), got %d", n)
	}
}

// TestLoader_RespectsCanceledContext asserts the walk bails out on
// context cancellation. Useful when a stuck NFS mount or symlink
// loop would otherwise block Bootstrap indefinitely.
func TestLoader_RespectsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "a.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(store.New()).Load(ctx, dir)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
