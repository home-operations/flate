package loader

import (
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	internalkustomize "github.com/home-operations/flate/internal/kustomize"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_NativeEntryNeedsOwnObligation(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"workload/value.yaml", "entry/child.yaml", "entry/kustomization.yaml", "entry/patch.yaml", "entry/input.env", "entry/component/kustomization.yaml", "entry/missing/data.yaml"} {
		t.Run("construction/"+file, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					LocalPath: root,
					FS: fstest.MapFS{"entry/kustomization.yaml": {Data: []byte(`resources: [child.yaml, missing]
patches: [{path: patch.yaml}]
components: [component]
configMapGenerator: [{name: settings, envs: [input.env]}]
`)}, "entry/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./workload}
`)}, "entry/patch.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {interval: 1h}
`)}, "entry/input.env": {Data: []byte("VALUE=one\n")}, "entry/component/kustomization.yaml": {Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nnamespace: apps\n")}, "workload/kustomization.yaml": {Data: []byte("resources: [value.yaml]\n")}, "workload/value.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: value}\n")}},
				},
			)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{file})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Blocked, "entry"), file != "workload/value.yaml")
		})
	}
	for _, tc := range []struct {
		name, file   string
		failed, want bool
	}{{name: "child_payload", file: "workload/value.yaml"}, {name: "entry_declaration", file: "entry/child.yaml", want: true}, {name: "entry_failure", file: "workload/value.yaml", failed: true, want: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./workload"}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", owner.Named())
			st.AddObjects([]manifest.BaseManifest{owner, child, entry})
			if tc.failed {
				st.UpdateStatus(
					entry.Named(),
					store.StatusInfo{Status: store.StatusFailed, Message: "invalid entry package"},
				)
			}
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Changes = change.NewSet([]string{tc.file})
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Index = &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {File: "entry/owner.yaml", Declaration: owner},
					child.Named(): {File: "entry/child.yaml", Declaration: child, Parent: owner.Named()},
				},
			}
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, entry.Named()), tc.want)
		})
	}
}

func TestAdmission_UnreferencedEntryFile(t *testing.T) {
	t.Parallel()
	for _, historical := range []bool{false, true} {
		t.Run(fmt.Sprintf("history=%t", historical), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					LocalPath: root,
					FS: fstest.MapFS{
						"entry/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
						"entry/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./workload}
`)},
						"entry/unreferenced.yaml": {
							Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n"),
						},
					},
				},
			)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"entry/unreferenced.yaml"})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
			var history map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}
			if historical {
				history = map[manifest.NamedResource]struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{entry.Named(): {Complete: true, Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					yield(
						entry.Named(),
						manifest.DiscoveryRecord{
							ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"entry"}},
						},
					)
				}}}
			}
			proposal, err := l.Prepare(
				t.Context(),
				PrepareRequest{Generation: []store.Child{{Obj: entry}}, History: history},
			)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, entry.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Blocked, "entry"), false)
		})
	}
}

func TestAdmission_NamedDemandRequiresNativeOwner(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [apps.yaml]\n")},
		"entry/apps.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: apps}
spec: {path: ./workload, postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}}
`)},
		"workload/kustomization.yaml": {Data: []byte("resources: [release.yaml]\n")},
		"workload/release.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec: {interval: 1m, chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}, valuesFrom: [{kind: ConfigMap, name: settings}]}
`)},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	selected := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Namespace: "apps",
		Name:      "selected",
	}
	parent := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "apps",
	}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{selected}}
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.Load(t.Context(), filepath.Join(root, "workload"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	defer proposal.Retire()
	// Pass 18 index authority admits the authored parent without native preparation.
	assert.Equal(t, len(proposal.Blocked), 0)
	assert.Equal(t, slices.ContainsFunc(proposal.Objects, func(child store.Child) bool {
		return child.Obj.Named() == parent && child.SourceSupport == manifest.BootstrapSourceID
	}), true)
	assert.Equal(t, slices.Contains(proposal.Needed, parent), true)
	assert.Equal(t, slices.Contains(proposal.Ordinary, parent), true)
	assert.Equal(t, slices.Contains(proposal.Selected, parent), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, parent), false)
	assert.Equal(t, st.GetObject(selected) == nil, true)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(child store.Child) bool { return child.Obj.Named() == selected }),
		false,
	)
}

func TestAdmission_BuiltParentRequiresNativeOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, transformation, prefix string }{
		{name: "renamed", transformation: "namePrefix: built-\n", prefix: "built-"},
		{name: "same_name", transformation: "patches:\n- target: {kind: Kustomization, name: apps}\n  patch: |-\n    - op: add\n      path: /spec/sourceRef\n      value: {kind: GitRepository, name: effective, namespace: apps}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: [apps.yaml]\n" + tc.transformation)},
				"entry/apps.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: apps}
spec: {path: ./workload, postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}}
`)},
				"workload/kustomization.yaml": {Data: []byte("resources: [release.yaml]\n")},
				"workload/release.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec: {interval: 1m, chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}, valuesFrom: [{kind: ConfigMap, name: settings}]}
`)},
			}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			selected := manifest.NamedResource{
				Group:     manifest.HelmReleaseDomain,
				Kind:      manifest.KindHelmRelease,
				Namespace: "apps",
				Name:      "selected",
			}
			parent := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "apps",
				Name:      tc.prefix + "apps",
			}
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{selected}}
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			_, err = l.Load(t.Context(), filepath.Join(root, "workload"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			defer proposal.Retire()
			_, authored := l.occurrence(parent)
			assert.Equal(t, authored, false)
			assert.Equal(t, l.Index.Records[parent].Builder, "")
			assert.Diff(t, proposal.Blocked, []string{"entry"})
			assert.Equal(t, slices.ContainsFunc(proposal.Objects, func(child store.Child) bool {
				return child.Obj.Named() == parent
			}), false)
			assert.Equal(t, slices.Contains(proposal.Needed, parent), true)
			assert.Equal(t, slices.Contains(proposal.Ordinary, parent), true)
			assert.Equal(t, slices.Contains(proposal.Selected, parent), false)
			assert.Equal(t, slices.Contains(proposal.Dirty, parent), false)
			assert.Equal(t, st.GetObject(selected) == nil, true)
			assert.Equal(
				t,
				slices.ContainsFunc(
					proposal.Objects,
					func(child store.Child) bool { return child.Obj.Named() == selected },
				),
				false,
			)

			native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
			fs := filesys.MakeFsInMemory()
			for name, file := range input.FS.(fstest.MapFS) {
				target := filepath.Join(root, name)
				assert.NoError(t, fs.MkdirAll(filepath.Dir(target)))
				assert.NoError(t, fs.WriteFile(target, file.Data))
			}
			internalkustomize.BuildMutex.Lock()
			result, buildErr := internalkustomize.Build(fs, filepath.Join(root, "entry"))
			internalkustomize.BuildMutex.Unlock()
			assert.NoError(t, buildErr)
			data, err := result.AsYaml()
			assert.NoError(t, err)
			children := builtObjectsForTest(t, string(data))
			l.RegisterBuilt(supportedBuiltInputForTest(l, &BuiltInput{
				Root: filepath.Join(root, "entry"), Owner: native.Named(), Source: manifest.BootstrapSourceID,
			}), children)
			next, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			defer next.Retire()
			assert.Equal(t, slices.ContainsFunc(next.Objects, func(child store.Child) bool {
				return child.Obj.Named() == parent && child.Obj == children[0].Obj
			}), true)
			assert.Equal(t, slices.ContainsFunc(next.Objects, func(child store.Child) bool {
				return child.Obj.Named() == selected
			}), false)
			if tc.name == "same_name" {
				assert.Equal(t, children[0].Obj.(*manifest.Kustomization).SourceID().Name, "effective")
			}
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, FS: input.FS, Content: "replacement"},
			)
			_, current := l.BuiltChildren(filepath.Join(root, "entry"), native.Named(), nil)
			assert.Equal(t, current, false)
			_, authored = l.occurrence(parent)
			assert.Equal(t, authored, false)
		})
	}
}
