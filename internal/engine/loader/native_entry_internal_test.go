package loader

import (
	"fmt"
	"iter"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	internalkustomize "github.com/home-operations/flate/internal/engine/kustomize/krusty"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
					Provider:  store.ProviderInput,
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

			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Changes = detectChanges(t, []string{file}, []string{file})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
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
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./workload"}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", owner.Named())
			for _, object := range []manifest.BaseManifest{owner, child, entry} {
				st.AddObject(object)
			}
			if tc.failed {
				st.UpdateStatus(
					entry.Named(),
					store.StatusInfo{Status: store.StatusFailed, Message: "invalid entry package"},
				)
			}
			l := New(st)
			l.SourceRoot = root

			l.Options.Changes = detectChanges(t, []string{tc.file}, []string{tc.file})
			configureSelection(t, l, selection.Request{Changed: true})
			l.Index = &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {File: "entry/owner.yaml", Declaration: owner},
					child.Named(): {File: "entry/child.yaml", Declaration: child, Parent: owner.Named()},
				},
			}
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
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
					Provider:  store.ProviderInput,
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

			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Changes = detectChanges(
				t,
				[]string{"entry/unreferenced.yaml"},
				[]string{"entry/unreferenced.yaml"},
			)
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
			proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{
				Changes:    l.Options.Changes,
				Generation: []store.Child{{Obj: entry}},
				History:    history,
			})
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
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
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
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{selected}})
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.Load(t.Context(), filepath.Join(root, "workload"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
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
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
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
			configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{selected}})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			_, err = l.Load(t.Context(), filepath.Join(root, "workload"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
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
			result, buildErr := internalkustomize.Build(fs, filepath.Join(root, "entry"), false)
			internalkustomize.BuildMutex.Unlock()
			assert.NoError(t, buildErr)
			data, err := result.AsYaml()
			assert.NoError(t, err)
			children := builtObjectsForTest(t, string(data))
			assert.NoError(t, l.RegisterBuilt(t.Context(), supportedBuiltInputForTest(l, &BuiltInput{
				Root: filepath.Join(root, "entry"), Owner: native.Named(), Source: manifest.BootstrapSourceID,
			}), children))
			next, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
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
				&store.SourceArtifact{
					Provider:  store.ProviderInput,
					LocalPath: root,
					FS:        input.FS,
					Content:   "replacement",
				},
			)
			_, current := l.BuiltChildren(filepath.Join(root, "entry"), native.Named(), nil)
			assert.Equal(t, current, false)
			_, authored = l.occurrence(parent)
			assert.Equal(t, authored, false)
		})
	}
}

func TestAdmission_ProjectedBootstrapOwnership(t *testing.T) {
	t.Parallel()
	const cluster = "clusters/primary"
	const peer = "clusters/peer"
	const bootstrap = cluster + "/bootstrap/owner.yaml"
	const changed = "workload/value.yaml"
	source := (&manifest.GitRepository{Name: "fleet", Namespace: "apps"}).Named()
	owner := testutil.KustomizationID("apps", "primary")
	native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, cluster, manifest.NamedResource{}).Named()
	for _, entry := range []string{cluster, "."} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			for _, patched := range []bool{false, true} {
				root := t.TempDir()
				child := "  sourceRef: {kind: GitRepository, name: fleet}\n"
				inputs := fstest.MapFS{
					cluster + "/kustomization.yaml": {Data: []byte("resources: [bootstrap, ../../bundle]\n")},
					cluster + "/bootstrap/kustomization.yaml": {
						Data: []byte("resources: [owner.yaml, namespace.yaml]\n"),
					},
					bootstrap: {
						Data: []byte(testutil.KustomizationYAML("primary", "apps", "  path: ./"+cluster+"\n"+child)),
					},
					cluster + "/bootstrap/namespace.yaml": {
						Data: []byte("apiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n"),
					},
					"bundle/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
					"bundle/child.yaml": {
						Data: []byte(testutil.KustomizationYAML("application", "apps", "  path: ./workload\n"+child)),
					},
					"workload/kustomization.yaml": {Data: []byte("resources: [value.yaml]\n")},
					changed: {
						Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: value}\n"),
					},
					peer + "/kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
					peer + "/owner.yaml": {
						Data: []byte(testutil.KustomizationYAML("peer", "apps", "  path: ./"+peer+"\n"+child)),
					},
				}
				if patched {
					input := inputs[cluster+"/bootstrap/kustomization.yaml"]
					input.Data = append(input.Data, []byte(`patches:
- target: {kind: Namespace, name: apps}
  patch: |-
    - op: add
      path: /metadata/labels
      value: {example: patched}
`)...)
				}
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
					Provider: store.ProviderInput, LocalPath: root, FS: inputs,
				})
				st.SetArtifact(source, &store.SourceArtifact{
					Provider: store.ProviderAlias, LocalRoot: root, LocalPath: t.TempDir(), FS: inputs,
				})
				l := New(st)
				l.SourceRoot = root
				configureSelection(t, l, selection.Request{Changed: true})
				l.Options.Changes = detectChanges(t, []string{changed}, []string{changed})

				_, err := l.Load(t.Context(), filepath.Join(root, entry), manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
				record, indexed := l.Index.Lookup(owner)
				assert.Equal(t, indexed, true)
				assert.Equal(t, record.Authored, owner)
				assert.Equal(t, l.locations[owner].Identity, owner)
				assert.Equal(t, record.Builder, "")
				assert.Equal(t, st.GetDeclaration(owner), manifest.BaseManifest(nil))
				want := manifest.NamedResource{}
				if patched {
					want = native
					assert.Equal(t, slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
						return p.Owner == native && p.Source == manifest.BootstrapSourceID && p.File == bootstrap
					}), true)
				}
				assert.Equal(t, record.Parent, want)
				var reads store.ReadSet
				reads.Init(st, manifest.NamedResource{}, nil)
				reads.RequireMaterializer(owner)
				proposal, err := prepareSelection(
					t.Context(),
					t,
					l,
					selection.PrepareRequest{Invocation: true, Reads: &reads},
				)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Needed, native), patched)
				peerEntry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, peer, manifest.NamedResource{}).
					Named()
				assert.Equal(t, slices.Contains(proposal.Needed, peerEntry), false)
				proposal.Retire()
				reads.Retire()

				assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
				frozen, indexed := l.Index.Lookup(owner)
				assert.Equal(t, indexed, true)
				assert.Equal(t, frozen.Parent, want)
				l.Retire()
			}
		})
	}
}

func TestAdmission_PendingSelfClaimSupply(t *testing.T) {
	t.Parallel()
	const left = "roots/left"
	const right = "roots/left/nested"
	const peer = "roots/peer"
	for _, tc := range []struct {
		name        string
		packages    map[string]string
		parent      string
		productions []string
		selfClaim   bool
	}{
		{name: "inactive self claim"},
		{
			name: "self claim alone for its effective identity",
			packages: map[string]string{
				left: "resources: [../../shared]\nnamespace: elsewhere\n",
			},
			selfClaim: true,
		},
		{
			name: "one supplier", packages: map[string]string{left: "resources: [../../shared]\n"},
			parent: left, productions: []string{left}, selfClaim: true,
		},
		{
			name: "competing peer roots",
			packages: map[string]string{
				left: "resources: [../../shared]\n",
				peer: "resources: [../../shared]\n",
			},
			productions: []string{left, peer}, selfClaim: true,
		},
		{
			name: "competing nested roots",
			packages: map[string]string{
				left:  "resources: [../../shared]\n",
				right: "resources: [../../../shared]\n",
			},
			productions: []string{left, right}, selfClaim: true,
		},
		{
			name: "real nested import",
			packages: map[string]string{
				left:  "resources: [nested]\n",
				right: "resources: [../../../shared]\n",
			},
			parent: left, productions: []string{left}, selfClaim: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := pendingSelfClaimLoader(t, tc.packages)
			owner := testutil.KustomizationID("apps", "self")
			source := (&manifest.GitRepository{Name: "fleet", Namespace: "apps"}).Named()
			record, indexed := l.Index.Lookup(owner)
			assert.Equal(t, indexed, true)
			assert.Equal(t, record.Authored, owner)
			assert.Equal(t, l.locations[owner].Identity, owner)
			assert.Equal(t, l.locations[owner].requiresBuild, tc.selfClaim)
			assert.Equal(t, record.Builder, "")
			assert.Equal(t, slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
				return p.Owner == owner && p.Source == source && p.Authored == owner
			}), tc.selfClaim)
			var suppliers []string
			for i := range record.Productions {
				p := &record.Productions[i]
				if p.Owner == (manifest.NamedResource{}) || p.Owner == owner {
					continue
				}
				assert.Equal(t, p.Source, manifest.BootstrapSourceID)
				assert.Equal(t, p.Authored, owner)
				assert.Equal(t, p.File, "shared/owner.yaml")
				suppliers = append(suppliers, p.Owner.Name)
			}
			slices.Sort(suppliers)
			assert.Diff(t, suppliers, tc.productions)
			var parent manifest.NamedResource
			if tc.parent != "" {
				parent = manifest.NewEntryKustomization(
					manifest.BootstrapSourceID, tc.parent, manifest.NamedResource{},
				).Named()
			}
			assert.Equal(t, record.Parent, parent)
		})
	}
}

func TestAdmission_FrozenSelfClaimSupply(t *testing.T) {
	t.Parallel()
	const baseRoot = "roots/base"
	const headRoot = "roots/head"
	base := pendingSelfClaimLoader(t, map[string]string{baseRoot: "resources: [../../shared]\n"})
	head := pendingSelfClaimLoader(t, map[string]string{headRoot: "resources: [../../shared]\n"})
	assert.NoError(t, base.FinalizeIndex(t.Context(), base.SourceRoot, true))
	assert.NoError(t, head.FinalizeIndex(t.Context(), head.SourceRoot, true))

	owner := testutil.KustomizationID("apps", "self")
	before, baseIndexed := base.Index.Lookup(owner)
	after, headIndexed := head.Index.Lookup(owner)
	assert.Equal(t, baseIndexed, true)
	assert.Equal(t, headIndexed, true)
	assert.Equal(t, before.Authored, after.Authored)
	assert.Equal(t, before.Parent, manifest.NewEntryKustomization(
		manifest.BootstrapSourceID, baseRoot, manifest.NamedResource{},
	).Named())
	assert.Equal(t, after.Parent, manifest.NewEntryKustomization(
		manifest.BootstrapSourceID, headRoot, manifest.NamedResource{},
	).Named())
}

func pendingSelfClaimLoader(t *testing.T, packages map[string]string) *Loader {
	t.Helper()
	root := t.TempDir()
	owner := testutil.KustomizationID("apps", "self")
	source := (&manifest.GitRepository{Name: "fleet", Namespace: "apps"}).Named()
	inputs := fstest.MapFS{
		"claim/kustomization.yaml": {Data: []byte("resources: [../shared]\n")},
		"shared/kustomization.yaml": {Data: []byte(`resources: [owner.yaml, namespace.yaml]
patches:
- target: {kind: Namespace, name: apps}
  patch: |-
    - op: add
      path: /metadata/labels
      value: {example: patched}
`)},
		"shared/namespace.yaml": {
			Data: []byte("apiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n"),
		},
		"shared/owner.yaml": {
			Data: []byte(testutil.KustomizationYAML(
				"self", "apps", "  path: ./claim\n  sourceRef: {kind: GitRepository, name: fleet}\n",
			) + `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: fleet, namespace: apps}
spec: {url: https://github.com/example/fleet.git}
`),
		},
	}
	for dir, kustomization := range packages {
		inputs[dir+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte(kustomization)}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
		Provider: store.ProviderInput, LocalPath: root, FS: inputs,
	})
	st.SetArtifact(source, &store.SourceArtifact{
		Provider: store.ProviderAlias, LocalRoot: root, LocalPath: root, FS: inputs,
	})
	l := New(st)
	t.Cleanup(l.Retire)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.Load(t.Context(), filepath.Join(root, "claim"), owner, nil)
	assert.NoError(t, err)
	for _, dir := range slices.Sorted(maps.Keys(packages)) {
		_, err = l.Load(t.Context(), filepath.Join(root, dir), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	return l
}
