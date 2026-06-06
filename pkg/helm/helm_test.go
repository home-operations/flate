package helm

import (
	"context"
	"reflect"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	release "helm.sh/helm/v4/pkg/release/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
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

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))

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

// TestTemplate_DoesNotMutateInputValues guards the copy-on-collision
// optimization's load-bearing assumption. ExpandValueReferences now SHARES
// cached valuesFrom sub-trees into hr.Values by reference (instead of a full
// per-HR deep copy), so the render pipeline MUST treat the passed values as
// immutable — an in-place mutation would corrupt the shared cache canonical
// across HRs. helm v4 deep-copies the input on entry (CoalesceValues ->
// copystructure.Copy); this test renders with a nested map + slice values
// tree and asserts it is byte-identical afterward, catching a future helm
// bump that mutates the caller's map/sub-maps/slices.
func TestTemplate_DoesNotMutateInputValues(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/values.yaml", "image:\n  tag: default\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  tag: {{ .Values.image.tag }}\n")

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo", RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}
	vals := map[string]any{
		"image": map[string]any{"tag": "v1", "repository": "nginx"},
		"env":   []any{map[string]any{"name": "A", "value": "1"}},
	}
	snapshot := manifest.DeepCopyMap(vals)

	if _, err := cli.Template(context.Background(), hr, vals, Options{}); err != nil {
		t.Fatalf("Template: %v", err)
	}
	if !reflect.DeepEqual(vals, snapshot) {
		t.Errorf("render mutated the input values map (helm no longer copies on entry?)\n before=%#v\n after =%#v", snapshot, vals)
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

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	return cli
}

// localChartResolver wires a *store.Store containing a single
// GitRepository with its on-disk artifact, then returns a
// StoreSourceResolver backed by it. The helper keeps the existing
// test fixtures using the canonical resolver path instead of the
// legacy Add*-driven push API.
func localChartResolver(t *testing.T, name, namespace, dir string) SourceResolver {
	t.Helper()
	st := store.New()
	gr := &manifest.GitRepository{Name: name, Namespace: namespace}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	return NewStoreSourceResolver(st)
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

func TestReleaseManifest_HookSeparators(t *testing.T) {
	rel := &release.Release{
		Manifest: "kind: ConfigMap",
		Hooks: []*release.Hook{
			{Path: "hooks/a.yaml", Manifest: "kind: Job", Events: []release.HookEvent{release.HookPreInstall}},
			{Path: "hooks/b.yaml", Manifest: "kind: Secret\n", Events: []release.HookEvent{release.HookPreInstall}},
		},
	}

	got := releaseManifest(rel, Options{}, false, false)
	want := "kind: ConfigMap\n" +
		"---\n# Source: hooks/a.yaml\nkind: Job\n" +
		"---\n# Source: hooks/b.yaml\nkind: Secret\n"
	if got != want {
		t.Errorf("releaseManifest hook separators mismatch\nwant:\n%q\ngot:\n%q", want, got)
	}
}

// TestApplyHRCommonMetadata_OnRenderedChart locks that commonMetadata
// labels/annotations merge onto real rendered chart output — the
// post-render pass the helmrelease controller runs after TemplateDocs.
func TestApplyHRCommonMetadata_OnRenderedChart(t *testing.T) {
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
	ApplyHRCommonMetadata(docs, hr.CommonMetadata)
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

// TestApplyHROriginLabels_OnRenderedChart locks the helm-controller
// OriginLabels post-renderer behavior: every rendered resource is
// stamped with helm.toolkit.fluxcd.io/{name,namespace} so a real Flux
// would identify it as owned by this HelmRelease.
func TestApplyHROriginLabels_OnRenderedChart(t *testing.T) {
	cli := helmChartFixture(t)
	hr := newHR()
	docs, err := cli.TemplateDocs(context.Background(), hr, nil, Options{NoHooks: true})
	if err != nil {
		t.Fatalf("TemplateDocs: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected at least one rendered doc")
	}
	ApplyHROriginLabels(docs, hr)
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

// TestApplyHROriginLabels_WinsOverCommonMetadata locks the upstream
// precedence rule: OriginLabels run AFTER CommonRenderer, so an
// origin-key collision in CommonMetadata is silently overridden by the
// origin value. Matches helm-controller/internal/postrender/build.go:46.
func TestApplyHROriginLabels_WinsOverCommonMetadata(t *testing.T) {
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
	ApplyHRCommonMetadata(docs, hr.CommonMetadata)
	ApplyHROriginLabels(docs, hr)
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
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{
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
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{
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
	ApplyHRCommonMetadata(docs, nil)
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{})
	if len(original) != 1 || original["name"] != "x" {
		t.Errorf("metadata mutated by nil/empty CommonMetadata: %v", original)
	}
}

func TestApplyHRCommonMetadata_CreatesMetadataWhenMissing(t *testing.T) {
	docs := []map[string]any{{}} // no metadata
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{
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

// TestApplyHRCommonMetadata_SkipsHooks locks the upstream contract:
// commonMetadata + origin labels apply only to "real" workload
// resources. Helm hooks (helm.sh/hook annotation) are skipped because
// helm-controller's PostRenderStrategy is NoHooks — the chain is fed
// only non-hook templates. Without this, flate stamped labels on
// hooks that cluster-side Flux never adds.
func TestApplyHRCommonMetadata_SkipsHooks(t *testing.T) {
	docs := []map[string]any{
		{
			"kind": "Deployment",
			"metadata": map[string]any{
				"name": "real-workload",
			},
		},
		{
			"kind": "ConfigMap",
			"metadata": map[string]any{
				"name": "hook-cm",
				"annotations": map[string]any{
					"helm.sh/hook": "pre-install",
				},
			},
		},
	}
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{
		Labels: map[string]string{"team": "platform"},
	})

	workloadLabels := docs[0]["metadata"].(map[string]any)["labels"]
	if workloadLabels == nil {
		t.Fatal("workload doc lost its labels stamp")
	}
	if workloadLabels.(map[string]any)["team"] != "platform" {
		t.Errorf("workload doc labels: %v", workloadLabels)
	}

	hookMd := docs[1]["metadata"].(map[string]any)
	if _, ok := hookMd["labels"]; ok {
		t.Errorf("hook doc should not receive commonMetadata stamp; got labels %v", hookMd["labels"])
	}
}

// TestApplyHRCommonMetadata_SkipsCRDs locks the upstream behavior:
// helm-controller's separate applyCRDs path uses setOriginVisitor for
// origin labels but never CommonMetadata. flate must not stamp
// commonMetadata onto CRDs — that produces labels real Flux doesn't
// apply in cluster.
func TestApplyHRCommonMetadata_SkipsCRDs(t *testing.T) {
	docs := []map[string]any{
		{
			"kind":     "Deployment",
			"metadata": map[string]any{"name": "real"},
		},
		{
			"kind":     "CustomResourceDefinition",
			"metadata": map[string]any{"name": "things.example.com"},
		},
	}
	ApplyHRCommonMetadata(docs, &helmv2.CommonMetadata{
		Labels: map[string]string{"team": "platform"},
	})

	if docs[0]["metadata"].(map[string]any)["labels"] == nil {
		t.Error("workload doc lost its commonMetadata stamp")
	}
	crdMd := docs[1]["metadata"].(map[string]any)
	if _, ok := crdMd["labels"]; ok {
		t.Errorf("CRD should not receive commonMetadata stamp; got %v", crdMd["labels"])
	}
}

// TestApplyHROriginLabels_SkipsHooks mirrors the commonMetadata
// hook-exclusion above for the origin-labels pass.
func TestApplyHROriginLabels_SkipsHooks(t *testing.T) {
	docs := []map[string]any{
		{
			"kind":     "Deployment",
			"metadata": map[string]any{"name": "real"},
		},
		{
			"kind": "Job",
			"metadata": map[string]any{
				"name": "hook-job",
				"annotations": map[string]any{
					"helm.sh/hook": "pre-install",
				},
			},
		},
	}
	ApplyHROriginLabels(docs, &manifest.HelmRelease{Name: "demo", Namespace: "apps"})

	if _, ok := docs[0]["metadata"].(map[string]any)["labels"]; !ok {
		t.Error("workload doc lost its origin labels")
	}
	hookMd := docs[1]["metadata"].(map[string]any)
	if _, ok := hookMd["labels"]; ok {
		t.Errorf("hook doc should not receive origin labels; got %v", hookMd["labels"])
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
		got, err := mergeChartValuesFilesUncached(c, []string{"first.yaml", "second.yaml"}, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got["a"] != "first-only" || got["b"] != "from-second" || got["c"] != "second-only" {
			t.Errorf("layering failed: %v", got)
		}
	})

	t.Run("MissingFileIgnored", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		got, err := mergeChartValuesFilesUncached(c, []string{"a.yaml", "missing.yaml"}, true)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got["k"] != "v" {
			t.Errorf("present file should still merge: %v", got)
		}
	})

	t.Run("MissingFileErrorsWhenStrict", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		_, err := mergeChartValuesFilesUncached(c, []string{"missing.yaml"}, false)
		if err == nil {
			t.Fatal("expected error for missing file when ignoreMissing=false")
		}
		if !strings.Contains(err.Error(), "missing.yaml") {
			t.Errorf("error should name the missing file: %v", err)
		}
	})

	t.Run("InvalidYAMLErrors", func(t *testing.T) {
		c := chartWith(map[string]string{"bad.yaml": "key: : not-yaml\n"})
		_, err := mergeChartValuesFilesUncached(c, []string{"bad.yaml"}, false)
		if err == nil {
			t.Fatal("expected error for invalid YAML")
		}
		if !strings.Contains(err.Error(), "bad.yaml") {
			t.Errorf("error should name the bad file: %v", err)
		}
	})

	t.Run("EmptyNamesReturnsEmpty", func(t *testing.T) {
		c := chartWith(map[string]string{"a.yaml": "k: v\n"})
		got, err := mergeChartValuesFilesUncached(c, nil, false)
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
