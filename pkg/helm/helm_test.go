package helm

import (
	"context"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

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
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + dir},
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

// helmChartFixture stages a tiny chart with a hook, a test hook, and a
// CM template that the templating tests share.
func helmChartFixture(t *testing.T) *Client {
	t.Helper()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  k: v\n")
	testutil.WriteFile(t, dir, "mychart/templates/pre-install-hook.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-pre\n  annotations:\n    \"helm.sh/hook\": pre-install\ndata:\n  k: v\n")
	testutil.WriteFile(t, dir, "mychart/templates/test-hook.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-test\n  annotations:\n    \"helm.sh/hook\": test\ndata:\n  k: v\n")

	cli, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.AddLocalGit(LocalGitRepository{
		Repo: &manifest.GitRepository{
			Name: "chart-repo", Namespace: "flux-system",
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + dir},
		},
		Artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir},
	})
	return cli
}

func newHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo",
			RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}
}

func TestTemplate_TestHooksSkippedByDefault(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()

	out, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("Template: %v", err)
	}
	// pre-install hook should render; test hook should not when
	// spec.test.enable is unset (default behavior matches helm-controller).
	if !strings.Contains(out, "demo-pre") {
		t.Errorf("expected pre-install hook in output: %s", out)
	}
	if strings.Contains(out, "demo-test") {
		t.Errorf("test hook should be skipped by default: %s", out)
	}
}

func TestTemplate_TestEnableIncludesTestHook(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	hr.Test = &helmv2.Test{Enable: true}

	out, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("Template: %v", err)
	}
	if !strings.Contains(out, "demo-test") {
		t.Errorf("test hook should render when spec.test.enable=true: %s", out)
	}
}

func TestTemplate_HRInstallDisableHooks(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	hr.Install = &helmv2.Install{DisableHooks: true}

	out, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("Template: %v", err)
	}
	if strings.Contains(out, "demo-pre") {
		t.Errorf("HR-scoped spec.install.disableHooks should suppress pre-install hook: %s", out)
	}
	// Positive control — non-hook resources must still render so we
	// know the absence of "demo-pre" is hook-suppression, not a broken
	// render path.
	if !strings.Contains(out, "demo-cm") {
		t.Errorf("expected non-hook ConfigMap to still render: %s", out)
	}
}

func TestTemplateDocs_AppliesHRCommonMetadata(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	hr.CommonMetadata = &helmv2.CommonMetadata{
		Labels:      map[string]string{"team": "flate", "managed-by": "override"},
		Annotations: map[string]string{"owner": "platform"},
	}

	docs, err := cli.TemplateDocs(context.Background(), hr, nil, Options{NoHooks: true})
	if err != nil {
		t.Fatalf("TemplateDocs: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected at least one rendered doc")
	}
	cm := docs[0]
	md, _ := cm["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	annotations, _ := md["annotations"].(map[string]any)
	if labels["team"] != "flate" || labels["managed-by"] != "override" {
		t.Errorf("commonMetadata.labels not merged: %v", labels)
	}
	if annotations["owner"] != "platform" {
		t.Errorf("commonMetadata.annotations not merged: %v", annotations)
	}
}

func TestApplyHRCommonMetadata_LabelsOnly(t *testing.T) {
	docs := []map[string]any{
		{"metadata": map[string]any{"name": "x"}},
	}
	applyHRCommonMetadata(docs, &helmv2.CommonMetadata{
		Labels: map[string]string{"team": "flate"},
	})
	md := docs[0]["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	if labels["team"] != "flate" {
		t.Errorf("labels not merged: %v", labels)
	}
	if _, ok := md["annotations"]; ok {
		t.Errorf("annotations key should not be created when input is empty: %v", md)
	}
}

func TestApplyHRCommonMetadata_AnnotationsOnly(t *testing.T) {
	docs := []map[string]any{
		{"metadata": map[string]any{"name": "x"}},
	}
	applyHRCommonMetadata(docs, &helmv2.CommonMetadata{
		Annotations: map[string]string{"owner": "platform"},
	})
	md := docs[0]["metadata"].(map[string]any)
	annotations, _ := md["annotations"].(map[string]any)
	if annotations["owner"] != "platform" {
		t.Errorf("annotations not merged: %v", annotations)
	}
	if _, ok := md["labels"]; ok {
		t.Errorf("labels key should not be created when input is empty: %v", md)
	}
}

func TestApplyHRCommonMetadata_NilOrEmptyIsNoop(t *testing.T) {
	docs := []map[string]any{
		{"metadata": map[string]any{"name": "x"}},
	}
	original := docs[0]["metadata"].(map[string]any)
	applyHRCommonMetadata(docs, nil)
	applyHRCommonMetadata(docs, &helmv2.CommonMetadata{})
	if len(original) != 1 || original["name"] != "x" {
		t.Errorf("metadata mutated by nil/empty CommonMetadata: %v", original)
	}
}

func TestApplyHRCommonMetadata_CreatesMetadataWhenMissing(t *testing.T) {
	docs := []map[string]any{{}} // no metadata
	applyHRCommonMetadata(docs, &helmv2.CommonMetadata{
		Labels: map[string]string{"team": "flate"},
	})
	md, ok := docs[0]["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata not created: %v", docs[0])
	}
	labels, _ := md["labels"].(map[string]any)
	if labels["team"] != "flate" {
		t.Errorf("labels not merged into newly-created metadata: %v", labels)
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
