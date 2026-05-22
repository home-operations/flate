package helm

import (
	"context"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_LocalChart(t *testing.T) {
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

	cli, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.AddLocalGit(LocalGitRepository{
		Repo: &manifest.GitRepository{
			Name: "chart-repo", Namespace: "flux-system",
			URL: "file://" + dir,
		},
		Artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir},
	})

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}

	out, err := cli.Template(context.Background(), hr, map[string]any{"greeting": "hello"}, Options{})
	if err != nil {
		t.Fatalf("Template: %v", err)
	}
	if !strings.Contains(out, "name: demo-cm") {
		t.Errorf("rendered output missing expected name: %s", out)
	}
	if !strings.Contains(out, "greeting: hello") {
		t.Errorf("values not applied: %s", out)
	}
}

func TestOptions_SkipResourceKinds(t *testing.T) {
	o := Options{SkipCRDs: true, SkipSecrets: true, SkipKinds: []string{"ConfigMap"}}
	got := o.SkipResourceKinds()
	want := map[string]bool{"ConfigMap": true, "CustomResourceDefinition": true, "Secret": true}
	for _, k := range got {
		if !want[k] {
			t.Errorf("unexpected kind in skip list: %s", k)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 kinds, got %d: %v", len(got), got)
	}
}
