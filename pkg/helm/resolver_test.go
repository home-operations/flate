package helm_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// TestSourceResolver_NoAddCallsNeeded verifies helm.Client renders a HR
// whose chart lives in a GitRepository on disk, without any AddRepo /
// AddLocalSource push-calls — only the SourceResolver wired in at
// construction. This locks the iter-12 contract: the helm.Client reads
// source CRs through the canonical Store, eliminating the duplicate
// push-registries.
func TestSourceResolver_NoAddCallsNeeded(t *testing.T) {
	t.Parallel()
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
		URL: "file://" + dir,
	}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})

	owner := cachetest.New(t, "", cache.New)
	cli, err := helm.NewClient(owner)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	resolver := helm.NewStoreSourceResolver(st)
	foreign := gr.Named()
	foreign.Group = "example.test"
	foreignArtifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "foreign"}
	st.SetArtifact(foreign, foreignArtifact)
	if resolver.LocalSourceArtifact(foreign) != foreignArtifact || resolver.LocalSourceArtifact(gr.Named()).URL != gr.URL {
		t.Fatal("source resolver lost canonical artifact identity")
	}
	cli.SetSourceResolver(resolver)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoGroup:     manifest.SourceDomain,
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
