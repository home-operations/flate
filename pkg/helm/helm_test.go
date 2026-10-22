package helm

import (
	"context"
	"os"
	"strings"
	"sync"
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
	cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}

	out, err := cli.Template(context.Background(), hr, map[string]any{"greeting": "hello"}, RenderOptions{Options: Options{}, Sources: cliSources})
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

func TestTemplate_DoesNotMutateInputValues(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/values.yaml", "image:\n  tag: default\n")
	testutil.WriteFile(t, dir, "mychart/first.yaml", "image:\n  tag: first\n  repository: file-repo\n  pullPolicy: Always\n")
	testutil.WriteFile(t, dir, "mychart/second.yaml", "image:\n  tag: second\n  repository: nginx-file\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  tag: {{ .Values.image.tag }}\n  repository: {{ .Values.image.repository }}\n  pullPolicy: {{ .Values.image.pullPolicy }}\n")

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
	vals := map[string]any{
		"image": map[string]any{"tag": "v1", "repository": "nginx"},
		"env":   []any{map[string]any{"name": "A", "value": "1"}},
	}
	hr.ChartValuesFiles = []string{"mychart/values.yaml", "mychart/first.yaml", "mychart/second.yaml"}
	snapshot := manifest.DeepCopyMap(vals)
	hrBefore := hr.Clone()
	canonical, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	chartSnapshot := manifest.DeepCopyMap(canonical.chart.Values)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	outputs := make([]string, 2)
	ready.Add(2)
	for i := range 2 {
		done.Go(func() {
			ready.Done()
			<-start
			var err error
			outputs[i], err = cli.Template(t.Context(), hr, vals, RenderOptions{Options: Options{}, Sources: cliSources})
			if err != nil {
				t.Error(err)
			}
		})
	}
	ready.Wait()
	close(start)
	done.Wait()
	assert.Diff(t, canonical.chart.Values, chartSnapshot)
	assert.Diff(t, vals, snapshot)
	assert.Diff(t, hr, hrBefore)
	assert.Equal(t, outputs[1], outputs[0])
	for _, want := range []string{"tag: v1", "repository: nginx", "pullPolicy: Always"} {
		if !strings.Contains(outputs[0], want) {
			t.Errorf("missing %q in render: %s", want, outputs[0])
		}
	}
	cachedBefore := make(map[string]map[string]any, len(cli.chartValuesCache))
	for key, value := range cli.chartValuesCache {
		cachedBefore[key] = manifest.DeepCopyMap(value.merged)
	}
	repeated, err := cli.Template(t.Context(), hr, vals, RenderOptions{Options: Options{}, Sources: cliSources})
	assert.NoError(t, err)
	assert.Equal(t, repeated, outputs[0])
	assert.Equal(t, len(cli.chartValuesCache), len(cachedBefore))
	for key, before := range cachedBefore {
		assert.Diff(t, cli.chartValuesCache[key].merged, before)
	}
}

func helmChartFixture(t *testing.T) (*Client, Sources) {
	t.Helper()
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	cliSources := localChartResolver(t, "chart-repo", "flux-system", helmChartDirectory(t))
	return cli, cliSources
}

func helmChartDirectory(t *testing.T) string {
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

	return dir
}

// localChartResolver wires a *store.Store containing a single
// GitRepository with its on-disk artifact, then returns a
// StoreSourceResolver backed by it.
func localChartResolver(t *testing.T, name, namespace, dir string) Sources {
	t.Helper()
	st := store.New()
	gr := &manifest.GitRepository{Name: name, Namespace: namespace}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	return NewSources(st)
}

func newHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
}

func TestTemplate_TestHooksSkippedByDefault(t *testing.T) {
	t.Parallel()
	cli, cliSources := helmChartFixture(t)
	hr := newHR()

	out, err := cli.Template(context.Background(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
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

func templateResources(t *testing.T, raw string) []string {
	t.Helper()
	docs, err := manifest.SplitDocs([]byte(raw))
	assert.NoError(t, err)
	resources := make([]string, 0, len(docs))
	for _, doc := range docs {
		name, _ := manifest.DocMetadata(doc)
		resources = append(resources, manifest.DocKind(doc)+"/"+name)
	}
	return resources
}

func TestTemplate_TestEnableIncludesTestHook(t *testing.T) {
	t.Parallel()
	cli, cliSources := helmChartFixture(t)
	for _, tc := range []struct {
		name string
		skip bool
		want []string
	}{
		{"enabled", false, []string{"ConfigMap/demo-cm", "ConfigMap/demo-pre", "ConfigMap/demo-test"}},
		{"explicit-skip", true, []string{"ConfigMap/demo-cm", "ConfigMap/demo-pre"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := newHR()
			hr.Test = &helmv2.Test{Enable: true}
			out, err := cli.Template(t.Context(), hr, nil, RenderOptions{SkipTests: tc.skip, Sources: cliSources})
			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, out), tc.want)
		})
	}
}

func TestTemplate_ActionHookPolicy(t *testing.T) {
	t.Parallel()
	cli, cliSources := helmChartFixture(t)
	on, off := true, false
	for _, tc := range []struct {
		name              string
		isUpgrade         bool
		install, upgrade  *bool
		noHooks, wantHook bool
	}{
		{"install-both-allow", false, &off, &off, false, true},
		{"install-inactive-disables", false, &off, &on, false, true},
		{"install-active-disables", false, &on, &off, false, false},
		{"install-both-disable", false, &on, &on, false, false},
		{"upgrade-both-allow", true, &off, &off, false, true},
		{"upgrade-inactive-disables", true, &on, &off, false, true},
		{"upgrade-active-disables", true, &off, &on, false, false},
		{"upgrade-both-disable", true, &on, &on, false, false},
		{"install-override", false, &off, &off, true, false},
		{"upgrade-override", true, &off, &off, true, false},
		{"install-nil-active", false, nil, &on, false, true},
		{"upgrade-nil-active", true, &on, nil, false, true},
		{"install-absent", false, nil, nil, false, true},
		{"upgrade-absent", true, nil, nil, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := newHR()
			if tc.install != nil {
				hr.Install = &helmv2.Install{DisableHooks: *tc.install}
			}
			if tc.upgrade != nil {
				hr.Upgrade = &helmv2.Upgrade{DisableHooks: *tc.upgrade}
			}
			out, err := cli.Template(t.Context(), hr, nil, RenderOptions{IsUpgrade: tc.isUpgrade, NoHooks: tc.noHooks, Sources: cliSources})
			assert.NoError(t, err)
			want := []string{"ConfigMap/demo-cm"}
			if tc.wantHook {
				want = append(want, "ConfigMap/demo-pre")
			}
			assert.Diff(t, templateResources(t, out), want)
		})
	}
	t.Run("ShowOnlyHook", func(t *testing.T) {
		t.Parallel()
		hr := newHR()
		hr.Install = &helmv2.Install{}
		hr.Upgrade = &helmv2.Upgrade{DisableHooks: true}
		opts := Options{ShowOnly: []string{"mychart/templates/pre-install-hook.yaml"}}
		out, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
		assert.NoError(t, err)
		const want = "---\n# Source: mychart/templates/pre-install-hook.yaml\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-pre\n  annotations:\n    \"helm.sh/hook\": pre-install\ndata:\n  k: v\n"
		assert.Equal(t, out, want)
		assert.Diff(t, templateResources(t, out), []string{"ConfigMap/demo-pre"})
		opts.NoHooks = true
		out, err = cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources})
		assert.NoError(t, err)
		assert.Equal(t, out, "")
	})
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
				cli, cliSources := helmChartFixture(t)
				raw, err = cli.Template(t.Context(), hr, nil, RenderOptions{NoHooks: true, Sources: cliSources})
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
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
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
		got, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: c}, []string{"first.yaml", "second.yaml"}, false)
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
		got, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: c}, []string{"a.yaml", "missing.yaml"}, true)
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
		_, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: c}, []string{"missing.yaml"}, false)
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
		_, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: c}, []string{"bad.yaml"}, false)
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
		got, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: c}, nil, false)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty result for nil names: %v", got)
		}
	})
}

func TestFilterShowOnly(t *testing.T) {
	t.Parallel()
	const first = "# Source: mychart/templates/configmap.yaml\nkind: ConfigMap\nmetadata: {name: keep-me}\n"
	const a = "---\n" + first
	const b = "---\n# Source: mychart/templates/secret.yaml\nkind: Secret\nmetadata: {name: drop-me}\n"
	const c = "---\n# Source: mychart/templates/service.yaml\nkind: Service\nmetadata: {name: keep-me-too}\n"
	for _, tc := range []struct {
		name, content string
		paths         []string
		want          string
	}{
		{"all-framed", a + b + c, []string{"mychart/templates/configmap.yaml", "mychart/templates/secret.yaml", "mychart/templates/service.yaml"}, a + b + c},
		{"KeepsListedPaths", first + b + c, []string{"mychart/templates/configmap.yaml", "mychart/templates/service.yaml"}, first + c},
		{"first-only", a + b + c, []string{"mychart/templates/configmap.yaml"}, a},
		{"header-only-first", first + b + c, []string{"mychart/templates/configmap.yaml"}, first},
		{"later-only", a + b + c, []string{"mychart/templates/secret.yaml"}, b},
		{"final-newline", a + b + c, []string{"mychart/templates/service.yaml"}, c},
		{"no-final-newline", a + b + strings.TrimSuffix(c, "\n"), []string{"mychart/templates/service.yaml"}, strings.TrimSuffix(c, "\n")},
		{"retained-blank-line", a + "\n" + b, []string{"mychart/templates/configmap.yaml"}, a + "\n"},
		{"EmptyPathListProducesEmptyOutput", a + b + c, nil, ""},
		{"MissingHeaderSkipped", "kind: ConfigMap\n", []string{"any/path.yaml"}, ""},
		{"missing-header-before-selection", "kind: ConfigMap\n" + b, []string{"mychart/templates/secret.yaml"}, b},
		{"UnmatchedPathProducesEmptyOutput", a + b + c, []string{"mychart/templates/nonexistent.yaml"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, filterShowOnly(tc.content, tc.paths), tc.want)
		})
	}
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
