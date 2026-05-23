package helm

import (
	"context"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

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

// TestTemplateDocs_StampsOriginLabels locks the helm-controller
// OriginLabels post-renderer behavior: every rendered resource is
// stamped with helm.toolkit.fluxcd.io/{name,namespace} so a real Flux
// would identify it as owned by this HelmRelease.
func TestTemplateDocs_StampsOriginLabels(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	docs, err := cli.TemplateDocs(context.Background(), hr, nil, Options{NoHooks: true})
	if err != nil {
		t.Fatalf("TemplateDocs: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected at least one rendered doc")
	}
	for i, doc := range docs {
		md, _ := doc["metadata"].(map[string]any)
		labels, _ := md["labels"].(map[string]any)
		if labels["helm.toolkit.fluxcd.io/name"] != hr.Name {
			t.Errorf("doc[%d] missing/wrong helm.toolkit.fluxcd.io/name: %v", i, labels)
		}
		if labels["helm.toolkit.fluxcd.io/namespace"] != hr.Namespace {
			t.Errorf("doc[%d] missing/wrong helm.toolkit.fluxcd.io/namespace: %v", i, labels)
		}
	}
}

// TestTemplateDocs_OriginLabelsWinOverCommonMetadata locks the upstream
// precedence rule: OriginLabels run AFTER CommonRenderer, so an
// origin-key collision in CommonMetadata is silently overridden by the
// origin value. Matches helm-controller/internal/postrender/build.go:46.
func TestTemplateDocs_OriginLabelsWinOverCommonMetadata(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	hr.CommonMetadata = &helmv2.CommonMetadata{
		Labels: map[string]string{
			"helm.toolkit.fluxcd.io/name": "user-override-should-lose",
			"team":                        "platform",
		},
	}
	docs, err := cli.TemplateDocs(context.Background(), hr, nil, Options{NoHooks: true})
	if err != nil {
		t.Fatalf("TemplateDocs: %v", err)
	}
	md, _ := docs[0]["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	if labels["helm.toolkit.fluxcd.io/name"] != hr.Name {
		t.Errorf("origin label should win; got %v", labels["helm.toolkit.fluxcd.io/name"])
	}
	if labels["team"] != "platform" {
		t.Errorf("non-colliding commonMetadata label dropped; got %v", labels["team"])
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

func TestMergeChartValuesFiles(t *testing.T) {
	chartWith := func(files map[string]string) *chart.Chart {
		c := &chart.Chart{}
		for name, body := range files {
			c.Files = append(c.Files, &common.File{Name: name, Data: []byte(body)})
		}
		return c
	}

	t.Run("LayersInOrderOverridesEarlier", func(t *testing.T) {
		c := chartWith(map[string]string{
			"first.yaml":  "a: first-only\nb: from-first\n",
			"second.yaml": "b: from-second\nc: second-only\n",
		})
		got, err := mergeChartValuesFiles(c, []string{"first.yaml", "second.yaml"}, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got["a"] != "first-only" || got["b"] != "from-second" || got["c"] != "second-only" {
			t.Errorf("layering failed: %v", got)
		}
	})

	t.Run("MissingFileIgnored", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		got, err := mergeChartValuesFiles(c, []string{"a.yaml", "missing.yaml"}, true)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got["k"] != "v" {
			t.Errorf("present file should still merge: %v", got)
		}
	})

	t.Run("MissingFileErrorsWhenStrict", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		_, err := mergeChartValuesFiles(c, []string{"missing.yaml"}, false)
		if err == nil {
			t.Fatal("expected error for missing file when ignoreMissing=false")
		}
		if !strings.Contains(err.Error(), "missing.yaml") {
			t.Errorf("error should name the missing file: %v", err)
		}
	})

	t.Run("InvalidYAMLErrors", func(t *testing.T) {
		c := chartWith(map[string]string{"bad.yaml": "key: : not-yaml\n"})
		_, err := mergeChartValuesFiles(c, []string{"bad.yaml"}, false)
		if err == nil {
			t.Fatal("expected error for invalid YAML")
		}
		if !strings.Contains(err.Error(), "bad.yaml") {
			t.Errorf("error should name the bad file: %v", err)
		}
	})

	t.Run("EmptyNamesReturnsEmpty", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		got, err := mergeChartValuesFiles(c, nil, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty result for nil names: %v", got)
		}
	})
}

// TestFilterShowOnly covers the --show-only flag's filter logic: keep
// only sections whose "# Source: <path>" header matches one of the
// requested template paths. The function is wired into Options.ShowOnly
// for CLI consumers; pin the matrix here so future refactors don't
// silently break the flag.
func TestFilterShowOnly(t *testing.T) {
	rendered := `# Source: mychart/templates/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: keep-me
---
# Source: mychart/templates/secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: drop-me
---
# Source: mychart/templates/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: keep-me-too
`

	t.Run("KeepsListedPaths", func(t *testing.T) {
		got := filterShowOnly(rendered, []string{
			"mychart/templates/configmap.yaml",
			"mychart/templates/service.yaml",
		})
		if !strings.Contains(got, "name: keep-me") {
			t.Errorf("missing kept configmap: %s", got)
		}
		if !strings.Contains(got, "name: keep-me-too") {
			t.Errorf("missing kept service: %s", got)
		}
		if strings.Contains(got, "name: drop-me") {
			t.Errorf("unfiltered secret leaked through: %s", got)
		}
	})

	t.Run("EmptyPathListProducesEmptyOutput", func(t *testing.T) {
		got := filterShowOnly(rendered, nil)
		if got != "" {
			t.Errorf("nil show-only list should drop everything; got: %s", got)
		}
	})

	t.Run("MissingHeaderSkipped", func(t *testing.T) {
		noHeader := `apiVersion: v1
kind: ConfigMap
metadata:
  name: orphan
`
		got := filterShowOnly(noHeader, []string{"any/path.yaml"})
		if got != "" {
			t.Errorf("doc with no Source header must be dropped; got: %s", got)
		}
	})

	t.Run("UnmatchedPathProducesEmptyOutput", func(t *testing.T) {
		got := filterShowOnly(rendered, []string{"mychart/templates/nonexistent.yaml"})
		if got != "" {
			t.Errorf("unmatched show-only path should drop everything; got: %s", got)
		}
	})
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
