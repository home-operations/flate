package helm_test

import (
	"context"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

// TestSourceResolver_NoAddCallsNeeded verifies helm.Client renders a HR
// whose chart lives in a GitRepository on disk, without any AddRepo /
// AddLocalSource push-calls — only the SourceResolver wired in at
// construction. This locks the iter-12 contract: the helm.Client reads
// source CRs through the canonical Store, eliminating the duplicate
// push-registries.
func TestSourceResolver_NoAddCallsNeeded(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: test
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)

	// Build a Store containing the GitRepository + its on-disk artifact.
	st := store.New()
	gr := &manifest.GitRepository{
		Name: "chart-repo", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + dir},
	}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})

	cli, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	// Resolver wired — NO AddRepo / AddLocalSource calls below.
	cli.SetSourceResolver(helm.NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}

	out, err := cli.Template(context.Background(), hr, map[string]any{"greeting": "hello"}, helm.Options{})
	if err != nil {
		t.Fatalf("Template via resolver: %v", err)
	}
	if !strings.Contains(out, "name: demo-cm") {
		t.Errorf("expected rendered ConfigMap; got: %s", out)
	}
	if !strings.Contains(out, "greeting: hello") {
		t.Errorf("expected values applied; got: %s", out)
	}
}
