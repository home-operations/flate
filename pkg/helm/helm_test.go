package helm

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	release "helm.sh/helm/v4/pkg/release/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_LocalChart(t *testing.T) {
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

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
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
			RepoGroup:     manifest.SourceDomain,
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
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/values.yaml", "image:\n  tag: default\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  tag: {{ .Values.image.tag }}\n")

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo", RepoNamespace: "flux-system", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository,
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

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	return cli
}

// localChartResolver wires a *store.Store containing a single
// GitRepository with its on-disk artifact, then returns a
// StoreSourceResolver backed by it.
func localChartResolver(t *testing.T, name, namespace, dir string) SourceResolver {
	t.Helper()
	st := store.New()
	gr := &manifest.GitRepository{Name: name, Namespace: namespace}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	return NewStoreSourceResolver(st)
}

func newHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo",
			RepoNamespace: "flux-system", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository,
		},
	}
}

func TestTemplate_TestHooksSkippedByDefault(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestApplyHRMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		docs          string
		common        *helmv2.CommonMetadata
		origin        bool
		renderedChart bool
		want          string
	}{
		{
			name:          "RenderedChart",
			renderedChart: true,
			common: &helmv2.CommonMetadata{
				Labels: map[string]string{
					"team": "flate", "managed-by": "override",
					"helm.toolkit.fluxcd.io/name":      "user-override-should-lose",
					"helm.toolkit.fluxcd.io/namespace": "user-override-should-lose",
				},
				Annotations: map[string]string{"owner": "platform"},
			},
			origin: true,
			want: `apiVersion: v1
kind: ConfigMap
metadata:
  name: demo-cm
  labels:
    team: flate
    managed-by: override
    helm.toolkit.fluxcd.io/name: demo
    helm.toolkit.fluxcd.io/namespace: default
  annotations: {owner: platform}
data: {k: v}
`,
		},
		{
			name:   "LabelsOnly",
			docs:   "metadata: {name: x}\n",
			common: &helmv2.CommonMetadata{Labels: map[string]string{"team": "flate"}},
			want:   "metadata: {name: x, labels: {team: flate}}\n",
		},
		{
			name:   "AnnotationsOnly",
			docs:   "metadata: {name: x}\n",
			common: &helmv2.CommonMetadata{Annotations: map[string]string{"owner": "platform"}},
			want:   "metadata: {name: x, annotations: {owner: platform}}\n",
		},
		{
			name: "NilMetadata",
			docs: "metadata: {name: x}\n",
			want: "metadata: {name: x}\n",
		},
		{
			name:   "EmptyMetadata",
			docs:   "metadata: {name: x}\n",
			common: &helmv2.CommonMetadata{},
			want:   "metadata: {name: x}\n",
		},
		{
			name:   "CreatesMetadataWhenMissing",
			docs:   "kind: Deployment\n",
			common: &helmv2.CommonMetadata{Labels: map[string]string{"team": "flate"}},
			want:   "kind: Deployment\nmetadata: {labels: {team: flate}}\n",
		},
		{
			name: "CommonMetadataSkipsHooksAndCRDs",
			docs: `kind: Deployment
metadata: {name: real}
---
kind: ConfigMap
metadata: {name: hook-cm, annotations: {helm.sh/hook: pre-install}}
---
kind: CustomResourceDefinition
metadata: {name: things.example.com}
`,
			common: &helmv2.CommonMetadata{
				Labels:      map[string]string{"team": "platform"},
				Annotations: map[string]string{"owner": "platform"},
			},
			want: `kind: Deployment
metadata: {name: real, labels: {team: platform}, annotations: {owner: platform}}
---
kind: ConfigMap
metadata: {name: hook-cm, annotations: {helm.sh/hook: pre-install}}
---
kind: CustomResourceDefinition
metadata: {name: things.example.com}
`,
		},
		{
			name: "OriginLabelsSkipHooks",
			docs: `kind: Deployment
metadata: {name: real}
---
kind: Job
metadata: {name: hook-job, annotations: {helm.sh/hook: pre-install}}
`,
			origin: true,
			want: `kind: Deployment
metadata:
  name: real
  labels: {helm.toolkit.fluxcd.io/name: demo, helm.toolkit.fluxcd.io/namespace: default}
---
kind: Job
metadata: {name: hook-job, annotations: {helm.sh/hook: pre-install}}
`,
		},
		{
			name: "CommonThenOrigin",
			docs: `kind: Deployment
metadata: {name: real, labels: {app: retained}}
---
kind: Job
metadata: {name: hook-job, annotations: {helm.sh/hook: pre-install}}
---
kind: CustomResourceDefinition
metadata: {name: things.example.com}
`,
			common: &helmv2.CommonMetadata{
				Labels:      map[string]string{"team": "platform"},
				Annotations: map[string]string{"owner": "platform"},
			},
			origin: true,
			want: `kind: Deployment
metadata:
  name: real
  labels: {app: retained, team: platform, helm.toolkit.fluxcd.io/name: demo, helm.toolkit.fluxcd.io/namespace: default}
  annotations: {owner: platform}
---
kind: Job
metadata: {name: hook-job, annotations: {helm.sh/hook: pre-install}}
---
kind: CustomResourceDefinition
metadata:
  name: things.example.com
  labels: {helm.toolkit.fluxcd.io/name: demo, helm.toolkit.fluxcd.io/namespace: default}
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := newHR()
			hr.CommonMetadata = tc.common
			raw := tc.docs
			if tc.renderedChart {
				var err error
				raw, err = helmChartFixture(t).Template(t.Context(), hr, nil, Options{NoHooks: true})
				assert.NoError(t, err)
			}
			docs, err := manifest.SplitDocs([]byte(raw))
			assert.NoError(t, err)
			want, err := manifest.SplitDocs([]byte(tc.want))
			assert.NoError(t, err)
			ApplyHRCommonMetadata(docs, hr.CommonMetadata)
			if tc.origin {
				ApplyHROriginLabels(docs, hr)
			}
			assert.Diff(t, docs, want)
		})
	}
}

func TestMergeChartValuesFiles(t *testing.T) {
	t.Parallel()
	chartWith := func(files map[string]string) *chart.Chart {
		c := &chart.Chart{}
		for name, body := range files {
			c.Files = append(c.Files, &common.File{Name: name, Data: []byte(body)})
		}
		return c
	}

	t.Run("LayersInOrderOverridesEarlier", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
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
	t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
		got := filterShowOnly(rendered, nil)
		if got != "" {
			t.Errorf("nil show-only list should drop everything; got: %s", got)
		}
	})

	t.Run("MissingHeaderSkipped", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		got := filterShowOnly(rendered, []string{"mychart/templates/nonexistent.yaml"})
		if got != "" {
			t.Errorf("unmatched show-only path should drop everything; got: %s", got)
		}
	})
}

func TestOptions_SkipResourceKinds(t *testing.T) {
	t.Parallel()
	o := Options{SkipCRDs: true, SkipKinds: []string{manifest.KindConfigMap, manifest.KindSecret}}
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
