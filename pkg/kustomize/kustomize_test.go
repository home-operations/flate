package kustomize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestRenderFlux_RespectsCanceledContext asserts the renderer bails
// fast when ctx is already done. The fluxcd/pkg/kustomize Generator
// and SecureBuild don't take ctx themselves, so flate checks at
// coarse boundaries — this guards that those checks fire.
func TestRenderFlux_RespectsCanceledContext(t *testing.T) {
	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = RenderFlux(ctx, cache, t.TempDir(), ".", map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "k", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestFilterKinds(t *testing.T) {
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "Secret"},
		{"kind": "Service"},
	}
	out := FilterKinds(docs, []string{"ConfigMap"})
	if len(out) != 1 || out[0]["kind"] != "ConfigMap" {
		t.Errorf("FilterKinds: %+v", out)
	}
	out = ExcludeKinds(docs, []string{"Secret", "Service"})
	if len(out) != 1 || out[0]["kind"] != "ConfigMap" {
		t.Errorf("ExcludeKinds: %+v", out)
	}
}

func TestSubstitute(t *testing.T) {
	in := []byte(`hello ${NAME}, version=${VERSION:=v1}, ${OPT:-x}`)
	out, err := Substitute(in, map[string]string{"NAME": "world"})
	if err != nil {
		t.Fatalf("Substitute: %v", err)
	}
	want := "hello world, version=v1, x"
	if string(out) != want {
		t.Errorf("Substitute: got %q want %q", out, want)
	}

	_, err = Substitute([]byte("${MISSING}"), nil)
	if err == nil {
		t.Errorf("expected error for missing var")
	}
	if !strings.Contains(err.Error(), `variable "MISSING" is undefined`) {
		t.Errorf("missing-var error should name the var: %v", err)
	}
}

// TestSubstitute_DollarEscape — Flux's envsubst treats $${VAR} as a
// literal $ followed by ${VAR}, leaving the inner ${VAR} unsubstituted
// so a downstream shell / runtime envsubst can expand it. The previous
// regex-based engine missed this and tried to substitute anyway,
// which broke common patterns in home-ops repos (e.g. ${RUNNER_TOKEN}
// in a container's command).
func TestSubstitute_DollarEscape(t *testing.T) {
	cases := map[string]string{ //nolint:gosec // "TOKEN" / "DOMAIN" in literal templates are placeholder identifiers, not credentials
		`--token "$${RUNNER_TOKEN}"`:         `--token "${RUNNER_TOKEN}"`,
		`url: https://$${DOMAIN}/path`:       `url: https://${DOMAIN}/path`,
		`mixed $${ESCAPED} and ${UNESCAPED}`: `mixed ${ESCAPED} and HERE`,
	}
	for in, want := range cases {
		out, err := Substitute([]byte(in), map[string]string{"UNESCAPED": "HERE"})
		if err != nil {
			t.Errorf("Substitute(%q): %v", in, err)
			continue
		}
		if string(out) != want {
			t.Errorf("Substitute(%q) = %q, want %q", in, out, want)
		}
	}
}

// TestSubstitute_BashArrayBailsCleanly — Flux's envsubst parser bails
// when ${...} contains characters that aren't valid in POSIX parameter
// expansion (e.g. bash array brackets). flate now surfaces the same
// error envsubst raises rather than greedily matching the inner
// identifier and reporting it as a missing variable.
func TestSubstitute_BashArrayBailsCleanly(t *testing.T) {
	in := []byte(`for x in "${ARR[@]}"; do echo "$x"; done`)
	_, err := Substitute(in, map[string]string{"ARR": "ignored"})
	if err == nil {
		t.Fatalf("expected envsubst parse error for bash array")
	}
	if !strings.Contains(err.Error(), "missing closing brace") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestSubstitute_BashSubstringRemoval — Flux's envsubst recognizes
// POSIX-style ${VAR%%pattern} (strip-longest-suffix). The previous
// regex over-matched; envsubst handles this correctly when the var
// is defined.
func TestSubstitute_BashSubstringRemoval(t *testing.T) {
	in := []byte(`host=${ADDR%%:*}`)
	out, err := Substitute(in, map[string]string{"ADDR": "example.com:8080"})
	if err != nil {
		t.Fatalf("Substitute: %v", err)
	}
	if string(out) != "host=example.com" {
		t.Errorf("Substitute = %q, want %q", out, "host=example.com")
	}
}

// TestRenderFlux_HonorsCommonMetadata asserts that
// spec.commonMetadata.labels and spec.commonMetadata.annotations are
// merged into every rendered resource — kustomize-controller does this
// via ssautil.SetCommonMetadata after build, fluxcd/pkg/kustomize.Generator
// itself does not.
func TestRenderFlux_HonorsCommonMetadata(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "cm.yaml"), []byte(
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  namespace: ns\n  labels:\n    app: x\n  annotations:\n    note: y\ndata:\n  k: v\n",
	), 0o600); err != nil {
		t.Fatalf("write cm: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "kustomization.yaml"), []byte(
		"resources:\n- cm.yaml\n",
	), 0o600); err != nil {
		t.Fatalf("write kustomization: %v", err)
	}

	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	out, err := RenderFlux(context.Background(), cache, src, ".", map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "k", "namespace": "ns"},
		"spec": map[string]any{
			"path": "./",
			"commonMetadata": map[string]any{
				"labels":      map[string]any{"team": "flate", "app": "override"},
				"annotations": map[string]any{"managed-by": "flate"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}

	var got map[string]any
	if err := yaml.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	md, _ := got["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	annotations, _ := md["annotations"].(map[string]any)
	if labels["team"] != "flate" {
		t.Errorf("commonMetadata.labels not merged: %v", labels)
	}
	if labels["app"] != "override" {
		t.Errorf("commonMetadata.labels did not override existing: %v", labels)
	}
	if annotations["managed-by"] != "flate" {
		t.Errorf("commonMetadata.annotations not merged: %v", annotations)
	}
	if annotations["note"] != "y" {
		t.Errorf("existing annotations dropped: %v", annotations)
	}
}
