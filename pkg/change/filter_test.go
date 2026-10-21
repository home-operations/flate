package change

import (
	"slices"
	"sync"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// addUngated and addRecursive are test-only scaffolding: they seed the keep
// set directly, bypassing the primary-emitter gate AddEmitted enforces in
// production. Kept in the test file so the production binary carries neither;
// both walk through addRecursiveLocked, the lock-free body production shares.

// addUngated unconditionally extends the keep set with id (and its transitive
// sourceRef/chartRef/valuesFrom deps), marking every newly-inserted entry
// primary. No-op when the filter is disabled.
func (f *Filter) addUngated(id manifest.NamedResource) {
	if !f.Enabled() {
		return
	}
	f.addRecursive(id)
}

// addRecursive adds id (and transitive deps) to keep AND primary under f.mu,
// returning the newly-added ids so the caller can refire arrivals outside the lock.
func (f *Filter) addRecursive(id manifest.NamedResource) []manifest.NamedResource {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addRecursiveLocked(id)
}

func refObj(id manifest.NamedResource) manifest.BaseManifest {
	apiVersion := "v1"
	if id.Group != "" {
		apiVersion = id.Group + "/v1"
	}
	return &manifest.RawObject{APIVersion: apiVersion, Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
}

func TestFilter_DisabledKeepsEverything(t *testing.T) {
	t.Parallel()
	var f Filter
	id := testutil.HelmReleaseID("ns", "x")
	if !f.ShouldReconcile(id) {
		t.Fatal("disabled filter must keep everything")
	}
}

func TestFilter_ResolveDirectMatch(t *testing.T) {
	t.Parallel()
	hr := testutil.HelmReleaseID("ns", "x")
	f := NewFilter(NewSet([]string{"apps/x/helmrelease.yaml"}), filterFixture{files: map[manifest.NamedResource]string{hr: "apps/x/helmrelease.yaml"}, root: "", objects: controllertest.EmptyLister()}.index(), controllertest.EmptyLister())
	if !f.ShouldReconcile(hr) {
		t.Fatalf("expected direct-match keep; keep=%v", f.KeepNames())
	}
}

func TestFilter_SharedComponentPropagatesToAllConsumers(t *testing.T) {
	t.Parallel()
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		Path:       "apps/media/plex/app",
		Components: []string{"../../../../components/volsync"},
	}
	atuin := &manifest.Kustomization{
		Name: "atuin", Namespace: "default",
		Path:       "apps/default/atuin/app",
		Components: []string{"../../../../components/volsync"},
	}
	hrPlex := testutil.HelmReleaseID("media", "plex")
	hrAtuin := testutil.HelmReleaseID("default", "atuin")
	ksPlex, ksAtuin := plex.Named(), atuin.Named()

	f := NewFilter(NewSet([]string{"components/volsync/pvc.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		ksPlex:  "apps/media/plex/app/ks.yaml",
		hrPlex:  "apps/media/plex/app/helmrelease.yaml",
		ksAtuin: "apps/default/atuin/app/ks.yaml",
		hrAtuin: "apps/default/atuin/app/helmrelease.yaml",
	}, root: "", objects: controllertest.MapLister{ksPlex: plex, ksAtuin: atuin}}.index(), controllertest.MapLister{ksPlex: plex, ksAtuin: atuin})

	for _, id := range []manifest.NamedResource{ksPlex, ksAtuin, hrPlex, hrAtuin} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

func TestFilter_AncestorKSAlsoKept(t *testing.T) {
	t.Parallel()
	// A meta-KS at apps/ and a specific KS at apps/media/plex/app —
	// the leaf plex is the deepest owner of the changed file, but the
	// meta-KS's render (parent-injected spec.patches and
	// postBuild.substituteFrom) mutates plex's spec. Both must be
	// kept under changed-only mode so the leaf renders against the
	// post-mutation spec. See #58.
	meta := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "apps",
	}
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		Path: "apps/media/plex/app",
	}
	hrPlex := testutil.HelmReleaseID("media", "plex")
	metaID, plexID := meta.Named(), plex.Named()

	f := NewFilter(NewSet([]string{"apps/media/plex/app/helmrelease.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		metaID: "flux/cluster/ks.yaml",
		plexID: "apps/media/plex/app/ks.yaml",
		hrPlex: "apps/media/plex/app/helmrelease.yaml",
	}, root: "", objects: controllertest.MapLister{metaID: meta, plexID: plex}}.index(), controllertest.MapLister{metaID: meta, plexID: plex})

	for _, id := range []manifest.NamedResource{plexID, hrPlex, metaID} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

// TestFilter_StructuralParentOfOwnerKSAlsoKept covers the home-ops
// cross-tree pattern (joryirving-style): a leaf Flux KS in apps/base/
// is registered by a parent KS rendering apps/main/. The leaf's
// source file lives under the parent's spec.path, so the parent has
// to reconcile too — otherwise the namespace-scoped sources it emits
// (e.g. components/namespace producing one OCIRepository per tenant
// ns) never land in the store and the leaf can't resolve its chart
// ref. The changed file itself stays in apps/base/, which the parent
// does NOT cover via spec.path, so ancestorsOf(file) wouldn't catch
// the parent — only ownersOf(leaf-KS source file) does.
func TestFilter_StructuralParentOfOwnerKSAlsoKept(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "apps/main",
	}
	leaf := &manifest.Kustomization{
		Name: "actual", Namespace: "self-hosted",
		Path: "apps/base/self-hosted/actual",
	}
	hrActual := testutil.HelmReleaseID("self-hosted", "actual")
	parentID, leafID := parent.Named(), leaf.Named()

	f := NewFilter(NewSet([]string{"apps/base/self-hosted/actual/helmrelease.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		parentID: "clusters/main/apps.yaml",
		leafID:   "apps/main/self-hosted/actual.yaml",
		hrActual: "apps/base/self-hosted/actual/helmrelease.yaml",
	}, root: "", objects: controllertest.MapLister{parentID: parent, leafID: leaf}}.index(), controllertest.MapLister{parentID: parent, leafID: leaf})

	for _, id := range []manifest.NamedResource{hrActual, leafID, parentID} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

func TestFilter_AncestorKSDoesNotPullInUnrelatedSiblings(t *testing.T) {
	t.Parallel()
	// Two leaf KSes under the same meta-KS. Only plex changes. plex
	// + meta are kept; atuin (an unrelated sibling under the meta-KS)
	// must NOT be pulled in — keeping the meta-KS reconcile-eligible
	// does not widen sibling-resource coverage.
	meta := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "apps",
	}
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		Path: "apps/media/plex/app",
	}
	atuin := &manifest.Kustomization{
		Name: "atuin", Namespace: "default",
		Path: "apps/default/atuin/app",
	}
	metaID, plexID, atuinID := meta.Named(), plex.Named(), atuin.Named()
	hrAtuin := testutil.HelmReleaseID("default", "atuin")

	f := NewFilter(NewSet([]string{"apps/media/plex/app/helmrelease.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		metaID:  "flux/cluster/ks.yaml",
		plexID:  "apps/media/plex/app/ks.yaml",
		atuinID: "apps/default/atuin/app/ks.yaml",
		hrAtuin: "apps/default/atuin/app/helmrelease.yaml",
	}, root: "", objects: controllertest.MapLister{metaID: meta, plexID: plex, atuinID: atuin}}.index(), controllertest.MapLister{metaID: meta, plexID: plex, atuinID: atuin})

	if !f.ShouldReconcile(metaID) {
		t.Errorf("meta KS must be kept (ancestor of changed file): %v", f.KeepNames())
	}
	if !f.ShouldReconcile(plexID) {
		t.Errorf("plex KS must be kept (owns changed file): %v", f.KeepNames())
	}
	if f.ShouldReconcile(atuinID) {
		t.Errorf("unrelated sibling atuin must not be pulled in: %v", f.KeepNames())
	}
	if f.ShouldReconcile(hrAtuin) {
		t.Errorf("unrelated sibling atuin HR must not be pulled in: %v", f.KeepNames())
	}
}

func TestFilter_ExternalSourcedKSClaimsExcludedFromOwnership(t *testing.T) {
	t.Parallel()
	// An external-sourced KS (e.g. OCIRepository) with `path: ./` claims
	// the repo root — since matchingPrefixes matches the root prefix,
	// that makes it a false ancestor of EVERY changed file, and its own
	// dependsOn then blocks the whole graph as a cycle (immich-app
	// yucca-o11y; loader-side counterpart is issue #752). Its spec.path
	// is relative to its own artifact, not the local tree, so resolve()
	// must drop its claims when the orchestrator supplies it via
	// externalKS.
	external := &manifest.Kustomization{
		Name: "yucca-o11y", Namespace: "flux-system",
		Path: "./",
	}
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		Path: "apps/media/plex/app",
	}
	externalID, plexID := external.Named(), plex.Named()
	sourceFiles := map[manifest.NamedResource]string{
		externalID: "flux/cluster/yucca-o11y.yaml",
		plexID:     "apps/media/plex/app/ks.yaml",
	}
	lister := controllertest.MapLister{externalID: external, plexID: plex}
	changed := []string{"apps/media/plex/app/helmrelease.yaml"}

	// Without the exclusion the root claim wins ancestor status — this
	// pins the cascade the fix removes (and the root-prefix match that
	// enables it).
	f := NewFilter(NewSet(changed), filterFixture{files: sourceFiles, root: "", objects: lister, refs: nil, excluded: nil, readers: nil, changed: NewSet(changed)}.index(), lister)
	if !f.ShouldReconcile(externalID) {
		t.Fatalf("precondition: root-claiming KS should be an ancestor without exclusions; keep=%v", f.KeepNames())
	}

	f = NewFilter(NewSet(changed), filterFixture{files: sourceFiles, root: "", objects: lister, refs: nil, excluded: map[manifest.NamedResource]struct{}{externalID: {}}, readers: nil, changed: NewSet(changed)}.index(), lister)
	if !f.ShouldReconcile(plexID) {
		t.Fatalf("owner of the changed file must be kept; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(externalID) {
		t.Errorf("external-sourced root-claiming KS must not enter keep as ancestor; keep=%v", f.KeepNames())
	}
}

func TestFilter_PinnedOwnershipKeepsProducer(t *testing.T) {
	t.Parallel()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "producers"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "producers/child"}
	consumer := &manifest.Kustomization{
		Name: "consumer", Namespace: "apps", Path: "consumers",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}}},
	}
	cm := &manifest.ConfigMap{Name: "settings"}
	files := map[manifest.NamedResource]string{
		producer.Named(): "flux/producer.yaml",
		child.Named():    "flux/child.yaml",
		consumer.Named(): "flux/consumer.yaml",
		cm.Named():       "producers/cm.yaml",
	}
	objs := controllertest.MapLister{
		producer.Named(): producer,
		child.Named():    child,
		consumer.Named(): consumer,
		cm.Named():       cm,
	}
	for _, tt := range []struct {
		name         string
		changed      string
		wantProducer bool
		wantConsumer bool
		wantChild    bool
	}{
		{name: "dirty-path", changed: "producers/cm.yaml"},
		{name: "dirty-ancestor", changed: "producers/child/dirty.yaml", wantChild: true},
		{name: "dirty-resources-reference", changed: "shared/cm.yaml"},
		{name: "consumer-dependency", changed: "flux/consumer.yaml", wantProducer: true, wantConsumer: true},
		{name: "authored-pin", changed: "flux/producer.yaml", wantProducer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := NewFilter(NewSet([]string{tt.changed}), filterFixture{files: files, objects: objs, changed: NewSet([]string{tt.changed}),
				pinned: map[manifest.NamedResource]struct{}{producer.Named(): {}},
				readers: func(file string) []manifest.NamedResource {
					if file == "shared/cm.yaml" {
						return []manifest.NamedResource{producer.Named()}
					}
					return nil
				},
			}.index(), objs)
			for _, id := range []manifest.NamedResource{
				cm.Named(),
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
			} {
				if got := f.producersFor(id); !slices.Equal(got, []manifest.NamedResource{producer.Named()}) {
					t.Errorf("production owners(%s) = %v, want [%s]", id, got, producer.Named())
				}
			}
			foreign := cm.Named()
			foreign.Group, foreign.Namespace = "foreign.example", "apps"
			assert.Equal(t, len(f.producersFor(foreign)), 0)
			foreign = producer.Named()
			foreign.Group = "foreign.example"
			assert.Equal(t, f.ShouldReconcile(foreign), false)
			for _, want := range []struct {
				id   manifest.NamedResource
				keep bool
			}{
				{producer.Named(), tt.wantProducer},
				{consumer.Named(), tt.wantConsumer},
				{child.Named(), tt.wantChild},
			} {
				if got := f.ShouldReconcile(want.id); got != want.keep {
					t.Errorf("ShouldReconcile(%s) = %v, want %v; keep=%v", want.id, got, want.keep, f.KeepNames())
				}
			}
			if tt.wantConsumer {
				sentinel := testutil.KustomizationID("", "sentinel")
				f.AddEmitted(producer.Named(), refObj(sentinel))
				if f.ShouldReconcile(sentinel) {
					t.Fatal("dependency-only pinned producer selected an unrelated child")
				}
			}
		})
	}
}

func TestFilter_ArtifactProducerIgnoresCheckoutRootClaim(t *testing.T) {
	t.Parallel()
	root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "./"}
	pinned := &manifest.Kustomization{Name: "pinned", Namespace: "apps", Path: "producers"}
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	file := "../artifact/producers/cm.yaml"
	f := NewFilter(NewSet([]string{"flux/unrelated.yaml"}), filterFixture{files: map[manifest.NamedResource]string{cm: file}, objects: controllertest.MapLister{root.Named(): root, pinned.Named(): pinned}, changed: NewSet([]string{"flux/unrelated.yaml"}),
		root: t.TempDir(),
		readers: func(path string) []manifest.NamedResource {
			if path == file {
				return []manifest.NamedResource{pinned.Named()}
			}
			return nil
		},
	}.index(), controllertest.MapLister{root.Named(): root, pinned.Named(): pinned})
	if got := f.producersFor(cm); !slices.Equal(got, []manifest.NamedResource{pinned.Named()}) {
		t.Fatalf("production owners(%s) = %v, want [%s]", cm, got, pinned.Named())
	}
}

func TestFilter_FileOwnerFallbackCatchesResourcesEscape(t *testing.T) {
	t.Parallel()
	apps := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps/homelab"}
	appsID := apps.Named()
	const baseFile = "apps/base/postgres/cluster.yaml"

	fileOwners := func(file string) []manifest.NamedResource {
		if file == baseFile {
			return []manifest.NamedResource{appsID}
		}
		return nil
	}

	f := NewFilter(NewSet([]string{baseFile}), filterFixture{files: map[manifest.NamedResource]string{appsID: "apps/homelab/kustomization.yaml"}, root: "", objects: controllertest.MapLister{appsID: apps}, refs: nil, excluded: nil, readers: fileOwners, changed: NewSet([]string{baseFile})}.index(), controllertest.MapLister{appsID: apps})
	if !f.ShouldReconcile(appsID) {
		t.Fatalf("owner supplied by the fileOwners fallback must be kept; keep=%v", f.KeepNames())
	}

	apps.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "cluster"}
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	other := &manifest.Kustomization{Name: "unrelated", Namespace: "flux-system", Path: "apps/other"}
	objects := controllertest.MapLister{apps.Named(): apps, source.Named(): source, other.Named(): other}
	for _, file := range []string{baseFile, "inputs/patch", "inputs/unrelated"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			fileOwners := func(file string) []manifest.NamedResource {
				if file == baseFile || file == "inputs/patch" {
					return []manifest.NamedResource{apps.Named()}
				}
				return nil
			}
			changed := NewSet([]string{file})
			f := NewFilter(changed, filterFixture{files: map[manifest.NamedResource]string{apps.Named(): "apps/homelab/kustomization.yaml"}, objects: objects, readers: fileOwners, changed: changed}.index(), objects)
			want := file != "inputs/unrelated"
			assert.Equal(t, f.ShouldReconcile(apps.Named()), want)
			assert.Equal(t, f.ShouldReconcile(source.Named()), want)
			assert.Equal(t, f.ShouldReconcile(other.Named()), false)
		})
	}
}

// TestFilter_FileOwnerMergesWithPrefixClaim: an escaped resource can sit
// under ANOTHER Kustomization's own claimed spec.path. A prefix hit
// there must not skip the fileOwners lookup — both the prefix claimant
// and the KS that reads the file via resources: need to be kept.
func TestFilter_FileOwnerMergesWithPrefixClaim(t *testing.T) {
	t.Parallel()
	postgres := &manifest.Kustomization{Name: "postgres", Namespace: "flux-system", Path: "apps/base/postgres"}
	apps := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps/homelab"}
	postgresID, appsID := postgres.Named(), apps.Named()
	const baseFile = "apps/base/postgres/cluster.yaml"

	fileOwners := func(file string) []manifest.NamedResource {
		if file == baseFile {
			return []manifest.NamedResource{appsID}
		}
		return nil
	}

	f := NewFilter(NewSet([]string{baseFile}), filterFixture{files: map[manifest.NamedResource]string{
		postgresID: "apps/base/postgres/kustomization.yaml",
		appsID:     "apps/homelab/kustomization.yaml",
	}, root: "", objects: controllertest.MapLister{postgresID: postgres, appsID: apps}, refs: nil, excluded: nil, readers: fileOwners, changed: NewSet([]string{baseFile})}.index(), controllertest.MapLister{postgresID: postgres, appsID: apps})
	if !f.ShouldReconcile(postgresID) {
		t.Errorf("prefix claimant must still be kept; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(appsID) {
		t.Errorf("fileOwners owner must be kept alongside the prefix claimant; keep=%v", f.KeepNames())
	}
}

// TestFilter_NoFileOwnerFallbackLeavesUnclaimedFileUnowned pins the
// pre-fix behavior: without a FileOwnerLookup, an unclaimed file's
// change resolves to an empty keep set.
func TestFilter_NoFileOwnerFallbackLeavesUnclaimedFileUnowned(t *testing.T) {
	t.Parallel()
	apps := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps/homelab"}
	appsID := apps.Named()

	f := NewFilter(NewSet([]string{"apps/base/postgres/cluster.yaml"}), filterFixture{files: map[manifest.NamedResource]string{appsID: "apps/homelab/kustomization.yaml"}, root: "", objects: controllertest.MapLister{appsID: apps}}.index(), controllertest.MapLister{appsID: apps})
	if len(f.KeepNames()) != 0 {
		t.Errorf("expected empty keep set without a fileOwners fallback; keep=%v", f.KeepNames())
	}
}

func TestFilter_KeepNamespaces(t *testing.T) {
	t.Parallel()
	// Drive keep via NewFilter rather than mutating the struct.
	ksA := &manifest.Kustomization{Name: "x", Namespace: "ns-a"}
	ksB := &manifest.Kustomization{Name: "y", Namespace: "ns-b"}
	ksCluster := &manifest.Kustomization{Name: "cluster-scope", Namespace: ""}
	a, b, c := ksA.Named(), ksB.Named(), ksCluster.Named()
	f := NewFilter(NewSet([]string{"a.yaml", "b.yaml", "c.yaml"}), filterFixture{files: map[manifest.NamedResource]string{a: "a.yaml", b: "b.yaml", c: "c.yaml"}, root: "", objects: controllertest.MapLister{a: ksA, b: ksB, c: ksCluster}}.index(), controllertest.MapLister{a: ksA, b: ksB, c: ksCluster})
	ns := f.KeepNamespaces()
	got := make([]string, 0, len(ns))
	for k := range ns {
		got = append(got, k)
	}
	slices.Sort(got)
	want := []string{"ns-a", "ns-b"}
	if !slices.Equal(got, want) {
		t.Errorf("KeepNamespaces=%v want %v", got, want)
	}
}

func TestFilter_TransitiveDepsKustomization(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system"},
		DependsOn: []kustomizev1.DependencyReference{{Namespace: "flux-system", Name: "repositories"}},
	}
	ksID := ks.Named()
	gitID := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system"}
	depID := testutil.KustomizationID("flux-system", "repositories")

	f := NewFilter(NewSet([]string{"ks.yaml"}), filterFixture{files: map[manifest.NamedResource]string{ksID: "ks.yaml"}, root: "", objects: controllertest.MapLister{ksID: ks}}.index(), controllertest.MapLister{ksID: ks})

	assert.Equal(t, f.ShouldReconcile(gitID), true)
	assert.Equal(t, f.ShouldReconcile(depID), false)
}

func TestFilter_ContentDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		obj  manifest.BaseManifest
		deps []manifest.NamedResource
	}{
		{&manifest.HelmRelease{Name: "plex", Namespace: "media", Chart: manifest.HelmChart{RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository, RepoName: "app-template", RepoNamespace: "flux-system"}, ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "plex-values"}}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}, {Kind: manifest.KindConfigMap, Namespace: "media", Name: "plex-values"}}},
		{&manifest.GitRepository{Name: "source", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}, []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}}},
		{&manifest.OCIRepository{Name: "source", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}, CertSecretRef: &manifest.LocalObjectReference{Name: "cert"}}, []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}, {Kind: manifest.KindSecret, Namespace: "apps", Name: "cert"}}},
		{&manifest.ResourceSet{Name: "set", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Name: "inputs"}}}, []manifest.NamedResource{{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "inputs"}}},
		{&manifest.HelmChartSource{Name: "chart", Namespace: "apps", SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "source"}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "source"}}},
	} {
		t.Run(tc.obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			objects := controllertest.MapLister{tc.obj.Named(): tc.obj}
			f := NewFilter(NewSet([]string{"changed.yaml"}), filterFixture{files: map[manifest.NamedResource]string{tc.obj.Named(): "changed.yaml"}, objects: objects}.index(), objects)
			for _, id := range tc.deps {
				assert.Equal(t, f.ShouldReconcile(id), true)
				if id.Group == "" {
					id.Group = "foreign.example"
				} else {
					id.Group = ""
				}
				assert.Equal(t, f.ShouldReconcile(id), false)
			}
			assert.Equal(t, f.ShouldReconcile(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "unrelated"}), false)
		})
	}
}

func TestFilter_DependsOnNotFollowed(t *testing.T) {
	t.Parallel()
	// dependsOn is reconcile-ordering only. A change to `a` must not
	// drag `b` into the keep set just because `a` depends on `b`.
	a := &manifest.Kustomization{
		Name: "a", Namespace: "flux-system", Path: "./a",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "src", Namespace: "flux-system"},
		DependsOn: []kustomizev1.DependencyReference{{
			Namespace: "flux-system", Name: "b",
		}},
	}
	b := &manifest.Kustomization{Name: "b", Namespace: "flux-system", Path: "./b"}
	aID, bID := a.Named(), b.Named()

	f := NewFilter(NewSet([]string{"a.yaml"}), filterFixture{files: map[manifest.NamedResource]string{aID: "a.yaml"}, root: "", objects: controllertest.MapLister{aID: a, bID: b}}.index(), controllertest.MapLister{aID: a, bID: b})

	if !f.ShouldReconcile(aID) {
		t.Fatalf("a should be kept; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(bID) {
		t.Errorf("dependsOn dragged b into keep set; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddExtendsKeepSetAtRuntime pins issue #204: a parent
// KS in the keep set may render and emit a child KS that wasn't
// discoverable at filter-build time (kustomize component +
// replacement patterns generate per-app Kustomizations on the fly).
// The KS controller calls Filter.AddEmitted(parent, child) before
// AddObject so the listener's PreGate filter check sees the
// extended keep set. This test uses addUngated directly (skipping
// the primacy gate) to seed the runtime keep without simulating
// a parent render; the gated path is covered by the AddEmitted
// tests below.
//
// Without this, the kept parent reconciles but every render-emitted
// child gets marked Ready "unchanged" by PreGate, never reconciles,
// and the child's own render output is silently absent from the
// diff (the user's repo: 37 of 40 OCIRepository digest changes
// missing because their `components/ks/app` pattern emits the leaf
// KS via replacements).
func TestFilter_AddExtendsKeepSetAtRuntime(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "database")
	child := testutil.KustomizationID("database", "leaf-app")

	f := NewFilter(NewSet([]string{"apps/database/parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "apps/database/parent.yaml"}, root: "", objects: controllertest.EmptyLister()}.index(), controllertest.EmptyLister())
	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be in keep set from file change; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(child) {
		t.Fatalf("precondition: child must NOT be in keep set before Add; keep=%v", f.KeepNames())
	}

	f.addUngated(child)
	if !f.ShouldReconcile(child) {
		t.Errorf("Add(child) should extend keep set; ShouldReconcile(child)=false; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddOnDisabledFilterIsNoOp protects against accidentally
// activating the keep-set semantics when the filter is disabled
// (no --path-orig). A disabled filter must continue to return true
// for everything regardless of Add() calls.
func TestFilter_AddOnDisabledFilterIsNoOp(t *testing.T) {
	t.Parallel()
	var f Filter
	id := testutil.KustomizationID("ns", "x")
	f.addUngated(id)
	other := testutil.HelmReleaseID("ns", "y")
	if !f.ShouldReconcile(other) {
		t.Errorf("disabled filter must still keep everything after Add()")
	}
}

func TestFilter_ShouldReconcileEmptyNamespaceFallback(t *testing.T) {
	t.Parallel()
	hrLoaded := testutil.HelmReleaseID("", "x")
	hrLookup := testutil.HelmReleaseID("media", "x")
	f := NewFilter(NewSet([]string{"f"}), filterFixture{files: map[manifest.NamedResource]string{hrLoaded: "f"}, root: "", objects: controllertest.EmptyLister()}.index(), controllertest.EmptyLister())
	// keep contains hrLoaded (namespace=""); a lookup with namespace=media
	// must still hit via the (Kind, Name) fallback index.
	if !f.ShouldReconcile(hrLookup) {
		t.Fatalf("(Kind, Name) fallback didn't match; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedRejectsAncestorOnlyEmitter pins the
// keep-cascade fix: when a parent KS is in the keep set only as an
// ancestor (kept so its patches/substituteFrom render before the
// leaf descendants per #58), its file-loaded emitted children must
// NOT auto-join keep. Without this gate, a one-file change in a
// deeply-nested leaf cascades the entire tree into keep — every
// ancestor renders, emits every file-loaded child, AddEmitted
// previously called Add unconditionally, every emitted child then
// emits its own children, and the cluster is back to a full reconcile.
//
// The change: AddEmitted no-ops when the emitter is in keep only
// as an ancestor. Render-only children (no sourceFile entry) of an
// ancestor parent are also gated out — if the ancestor's own render
// is unchanged from baseline, anything it emits at render time is
// unchanged too, and there's no diff to capture.
func TestFilter_AddEmittedRejectsAncestorOnlyEmitter(t *testing.T) {
	t.Parallel()
	// reloader's OCIRepository file is the only file change. The
	// leaf KS (reloader/app) owns that file → marked primary. The
	// "cluster-apps" ancestor whose spec.path covers everything is
	// kept (so its patches apply) but is NOT primary.
	leafKS := testutil.KustomizationID("kube-system", "reloader-app")
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	siblingApp := testutil.KustomizationID("media", "plex-app")

	leafKSObj := &manifest.Kustomization{Name: "reloader-app", Namespace: "kube-system",
		Path: "kubernetes/apps/kube-system/reloader/app"}
	clusterAppsObj := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps"}
	siblingObj := &manifest.Kustomization{Name: "plex-app", Namespace: "media",
		Path: "kubernetes/apps/media/plex/app"}

	f := NewFilter(NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		leafKS:      "kubernetes/apps/kube-system/reloader/app/ks.yaml",
		clusterApps: "kubernetes/flux/cluster-apps.yaml",
		siblingApp:  "kubernetes/apps/media/plex/ks.yaml",
	}, root: "", objects: controllertest.MapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj}}.index(), controllertest.MapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj})

	if !f.ShouldReconcile(leafKS) {
		t.Fatalf("leaf KS (owner of changed file) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(clusterApps) {
		t.Fatalf("cluster-apps ancestor must be in keep (so its patches render); keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(siblingApp) {
		t.Fatalf("precondition: sibling under same ancestor must NOT be in keep at resolve time; keep=%v", f.KeepNames())
	}

	// Simulate cluster-apps rendering and emitting the file-loaded
	// sibling. AddEmitted must NOT keep-add it.
	f.AddEmitted(clusterApps, refObj(siblingApp))
	if f.ShouldReconcile(siblingApp) {
		t.Errorf("AddEmitted(ancestor, sibling) over-extended keep set; sibling should remain SKIPPED; keep=%v", f.KeepNames())
	}

	// Same path but with a render-only child (no sourceFiles entry):
	// still rejected because the emitter (ancestor) is not primary
	// and an unchanged emitter wouldn't produce a different child.
	renderOnly := testutil.KustomizationID("rendered", "from-replacement")
	f.AddEmitted(clusterApps, refObj(renderOnly))
	if f.ShouldReconcile(renderOnly) {
		t.Errorf("AddEmitted(ancestor, render-only) should reject when ancestor is not primary; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedNoCascadeAcrossDeepAncestorChain pins the
// 3+ level ancestor case from the home-ops layout: a single file
// change deep under cluster-apps → kube-system → reloader →
// reloader-app should land ONLY reloader-app and the ancestor
// chain in keep, NOT every other ancestor's siblings (media-apps,
// network-apps, etc.). Each ancestor renders so its patches
// propagate (#58) but its AddEmitted calls for unrelated children
// must skip because the ancestor itself is not primary.
//
// The fix's correctness rests on this chain depth — the single-
// ancestor test alone wouldn't catch a future change that
// accidentally promoted an ancestor to primary mid-cascade.
func TestFilter_AddEmittedNoCascadeAcrossDeepAncestorChain(t *testing.T) {
	t.Parallel()
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	kubeSystem := testutil.KustomizationID("flux-system", "kube-system")
	reloader := testutil.KustomizationID("kube-system", "reloader")
	reloaderApp := testutil.KustomizationID("kube-system", "reloader-app")

	// Unrelated siblings at each ancestor level — none should
	// land in keep regardless of which level emits them.
	mediaSibling := testutil.KustomizationID("flux-system", "media-apps")
	spegelSibling := testutil.KustomizationID("kube-system", "spegel")
	reloaderSibling := testutil.KustomizationID("kube-system", "descheduler-app")

	objs := controllertest.MapLister{
		clusterApps: &manifest.Kustomization{Name: clusterApps.Name, Namespace: clusterApps.Namespace,
			Path: "kubernetes/apps"},
		kubeSystem: &manifest.Kustomization{Name: kubeSystem.Name, Namespace: kubeSystem.Namespace,
			Path: "kubernetes/apps/kube-system"},
		reloader: &manifest.Kustomization{Name: reloader.Name, Namespace: reloader.Namespace,
			Path: "kubernetes/apps/kube-system/reloader"},
		reloaderApp: &manifest.Kustomization{Name: reloaderApp.Name, Namespace: reloaderApp.Namespace,
			Path: "kubernetes/apps/kube-system/reloader/app"},
		mediaSibling:    &manifest.Kustomization{Name: mediaSibling.Name, Namespace: mediaSibling.Namespace, Path: "kubernetes/apps/media"},
		spegelSibling:   &manifest.Kustomization{Name: spegelSibling.Name, Namespace: spegelSibling.Namespace, Path: "kubernetes/apps/kube-system/spegel"},
		reloaderSibling: &manifest.Kustomization{Name: reloaderSibling.Name, Namespace: reloaderSibling.Namespace, Path: "kubernetes/apps/kube-system/descheduler/app"},
	}

	f := NewFilter(NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		clusterApps:     "kubernetes/flux/cluster-apps.yaml",
		kubeSystem:      "kubernetes/flux/kube-system.yaml",
		reloader:        "kubernetes/apps/kube-system/reloader/ks.yaml",
		reloaderApp:     "kubernetes/apps/kube-system/reloader/app/ks.yaml",
		mediaSibling:    "kubernetes/flux/media-apps.yaml",
		spegelSibling:   "kubernetes/apps/kube-system/spegel/ks.yaml",
		reloaderSibling: "kubernetes/apps/kube-system/descheduler/app/ks.yaml",
	}, root: "", objects: objs}.index(), objs)

	// Precondition: the leaf and its ancestor chain are in keep.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.ShouldReconcile(id) {
			t.Fatalf("expected %s in keep at resolve time; keep=%v", id, f.KeepNames())
		}
	}
	// And the unrelated siblings are NOT.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.ShouldReconcile(id) {
			t.Fatalf("precondition: %s must NOT be in keep before emissions; keep=%v", id, f.KeepNames())
		}
	}

	// Simulate cascade: each ancestor renders and emits all of
	// its children — including the unrelated siblings. Walking
	// the chain top-down so we cover the worst case where a
	// primary could theoretically leak through an upper ancestor.
	f.AddEmitted(clusterApps, refObj(kubeSystem))
	f.AddEmitted(clusterApps, refObj(mediaSibling))
	f.AddEmitted(kubeSystem, refObj(reloader))
	f.AddEmitted(kubeSystem, refObj(spegelSibling))
	f.AddEmitted(reloader, refObj(reloaderApp))
	f.AddEmitted(reloader, refObj(reloaderSibling))

	// Each ancestor (clusterApps, kubeSystem, reloader) is in
	// keep only as ancestor-only — none of their AddEmitted calls
	// should promote a file-loaded sibling into keep.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.ShouldReconcile(id) {
			t.Errorf("ancestor-only cascade leaked: %s joined keep via AddEmitted; keep=%v", id, f.KeepNames())
		}
	}
	// Sanity: the ancestor chain itself is still kept.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.ShouldReconcile(id) {
			t.Errorf("ancestor chain entry %s dropped after AddEmitted churn; keep=%v", id, f.KeepNames())
		}
	}
}

// TestFilter_AddEmittedFromPrimaryParent verifies the #204 path
// still works: a primary parent (its own file changed, or it owns
// the changed file) emitting any child — render-only or file-loaded
// — keep-adds the child. This is the original AddEmitted purpose
// and the bug-cascade prevention should not regress it.
func TestFilter_AddEmittedFromPrimaryParent(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "database")
	child := testutil.KustomizationID("database", "leaf-app")

	f := NewFilter(NewSet([]string{"apps/database/parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "apps/database/parent.yaml"}, root: "", objects: controllertest.EmptyLister()}.index(), controllertest.EmptyLister())
	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}

	f.AddEmitted(parent, refObj(child))
	if !f.ShouldReconcile(child) {
		t.Errorf("AddEmitted(primary parent, child) should keep-add child; keep=%v", f.KeepNames())
	}
}

func TestAddEmitted_StampedHelmReleasePullsChartSource(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"new_child", "replaced_child"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("flux-system", "cluster-apps")
			hr := &manifest.HelmRelease{
				Name: "sonarr", Namespace: "media",
				Chart: manifest.HelmChart{
					RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository, RepoName: "app-template", RepoNamespace: "flux-system",
				},
			}
			chartSrc := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}
			oldSource := chartSrc
			oldSource.Name = "old-chart"
			objects := controllertest.EmptyLister()
			if name == "replaced_child" {
				old := *hr
				old.Chart.RepoName = oldSource.Name
				objects[old.Named()] = &old
			}
			f := NewFilter(NewSet([]string{"apps/media/kustomization.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "apps/media/kustomization.yaml"}, objects: objects}.index(), objects)
			assert.Equal(t, f.ShouldReconcile(parent), true)
			for _, id := range []manifest.NamedResource{hr.Named(), chartSrc, oldSource} {
				assert.Equal(t, f.ShouldReconcile(id), false)
			}

			added := f.AddEmitted(parent, hr)

			assert.Equal(t, len(added), 2)
			for _, id := range []manifest.NamedResource{hr.Named(), chartSrc} {
				if !f.ShouldReconcile(id) || !slices.Contains(added, id) {
					t.Errorf("current identity %s not selected and returned: added=%v, keep=%v", id, added, f.KeepNames())
				}
			}
			if f.ShouldReconcile(oldSource) || slices.Contains(added, oldSource) {
				t.Errorf("obsolete source selected or returned: added=%v, keep=%v", added, f.KeepNames())
			}
		})
	}
}

func TestAddEmitted_KeptChildUsesIncomingReferences(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"ancestor_only", "primary"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("apps", "parent")
			child := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{
				RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository, RepoName: "new-chart", RepoNamespace: "apps",
			}}
			f := NewFilter(NewSet([]string{"parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "parent.yaml"}}.index(), controllertest.EmptyLister())
			if name == "primary" {
				f.addUngated(child.Named())
			} else {
				f.mu.Lock()
				f.markKept(child.Named())
				f.mu.Unlock()
			}
			assert.Equal(t, f.ShouldReconcile(child.Named()), true)
			assert.Equal(t, f.ShouldReconcile(child.ChartSource()), false)

			added := f.AddEmitted(parent, child)

			assert.Diff(t, added, []manifest.NamedResource{child.ChartSource()})
			assert.Equal(t, f.ShouldReconcile(child.ChartSource()), true)
			sentinel := testutil.KustomizationID("apps", "sentinel")
			assert.Diff(t, f.AddEmitted(child.Named(), refObj(sentinel)), []manifest.NamedResource{sentinel})
			assert.Equal(t, f.ShouldReconcile(sentinel), true)
		})
	}
}

func TestAddEmitted_AlreadySelectedSourceRetained(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{
		RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository, RepoName: "new-chart", RepoNamespace: "apps",
	}}
	old := *child
	old.Chart.RepoName = "old-chart"
	oldSource := old.ChartSource()
	objects := controllertest.MapLister{old.Named(): &old}
	f := NewFilter(NewSet([]string{"parent.yaml", "old-chart.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		parent: "parent.yaml", oldSource: "old-chart.yaml",
	}, objects: objects}.index(), objects)
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, f.ShouldReconcile(child.Named()), false)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), false)

	added := f.AddEmitted(parent, child)

	assert.Equal(t, len(added), 2)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), true)
		assert.Equal(t, slices.Contains(added, id), true)
	}
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, slices.Contains(added, oldSource), false)
}

func TestAddEmitted_ConcurrentReplacementReturnsEachIDOnce(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{
		RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindOCIRepository, RepoName: "new-chart", RepoNamespace: "apps",
	}}
	old := *child
	old.Chart.RepoName = "old-chart"
	objects := controllertest.MapLister{old.Named(): &old}
	f := NewFilter(NewSet([]string{"parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "parent.yaml"}, objects: objects}.index(), objects)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource(), old.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), false)
	}
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	var results [2][]manifest.NamedResource
	ready.Add(len(results))
	for i := range results {
		joined.Go(func() {
			ready.Done()
			<-start
			results[i] = f.AddEmitted(parent, child)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()

	returned := make(map[manifest.NamedResource]int)
	for _, ids := range results {
		for _, id := range ids {
			returned[id]++
		}
	}
	assert.Diff(t, returned, map[manifest.NamedResource]int{child.Named(): 1, child.ChartSource(): 1})
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), true)
	assert.Equal(t, f.ShouldReconcile(old.ChartSource()), false)
}

// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric-fallback contract: a fully-namespaced keep entry must
// NOT match a fully-namespaced lookup that happens to share
// (Kind, Name). Without this, a kept
// Kustomization.kustomize.toolkit.fluxcd.io/cluster-infra/external-secrets would silently scope-in
// an unrelated Kustomization.kustomize.toolkit.fluxcd.io/database/external-secrets (and likewise
// for the runtime AddEmitted / Add paths), broadening changed-only
// mode in ways the user can't see. The empty-namespace bridge in
// TestFilter_ShouldReconcileEmptyNamespaceFallback is preserved
// because it indexes only entries whose Namespace is empty.
func TestFilter_KeepByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	t.Parallel()
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, before, "changed.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: same}\n")
	testutil.WriteFile(t, after, "changed.yaml", "apiVersion: foreign.example/v1\nkind: Kustomization\nmetadata: {name: same}\n")
	changes, err := Detect(t.Context(), testInput(before), testInput(after))
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"changed.yaml"})
	for _, tc := range []struct {
		name, group, namespace, otherGroup, otherNamespace string
	}{
		{"namespace", manifest.FluxKustomizeDomain, "cluster-infra", manifest.FluxKustomizeDomain, "database"},
		{"real changed", manifest.FluxKustomizeDomain, "apps", "foreign.example", "apps"},
		{"foreign changed", "foreign.example", "apps", manifest.FluxKustomizeDomain, "apps"},
		{"real bare changed", manifest.FluxKustomizeDomain, "", "foreign.example", "apps"},
		{"foreign bare changed", "foreign.example", "", manifest.FluxKustomizeDomain, "apps"},
		{"core bare changed", "", "", manifest.FluxKustomizeDomain, "apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kept := manifest.NamedResource{Group: tc.group, Kind: manifest.KindKustomization, Namespace: tc.namespace, Name: "same"}
			collider := manifest.NamedResource{Group: tc.otherGroup, Kind: kept.Kind, Namespace: tc.otherNamespace, Name: kept.Name}
			files := map[manifest.NamedResource]string{kept: "changed.yaml", collider: "unchanged.yaml"}
			f := NewFilter(changes, filterFixture{files: files}.index(), controllertest.EmptyLister())
			assert.Equal(t, f.ShouldReconcile(kept), true)
			assert.Equal(t, f.ShouldReconcile(collider), false)
			projected := kept
			projected.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(projected), kept.Namespace == "")
			child := kept
			child.Name, child.Namespace = "emitted", ""
			assert.Equal(t, len(f.AddEmitted(collider, refObj(child))), 0)
			assert.Equal(t, f.ShouldReconcile(child), false)
			f.AddEmitted(kept, refObj(child))
			assert.Equal(t, f.ShouldReconcile(child), true)
			child.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(child), true)
			child.Group = tc.otherGroup
			child.Namespace = tc.otherNamespace
			if tc.group != tc.otherGroup {
				assert.Equal(t, f.ShouldReconcile(child), false)
			}
		})
	}
}

func TestFilter_TransitiveDepsHelmReleaseViaHelmChartCRD(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmChart, RepoName: "demo-chart", RepoNamespace: "apps",
		},
	}
	hc := &manifest.HelmChartSource{
		Name: "demo-chart", Namespace: "apps",
		SourceRef: sourcev1.LocalHelmChartSourceReference{
			Kind: manifest.KindOCIRepository, Name: "backing-oci",
		},
	}
	hrID := hr.Named()
	hcID := hc.Named()
	ociID := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "backing-oci"}

	t.Run("initialization", func(t *testing.T) {
		t.Parallel()
		f := NewFilter(NewSet([]string{"hr.yaml"}), filterFixture{files: map[manifest.NamedResource]string{hrID: "hr.yaml"}, root: "", objects: controllertest.MapLister{hrID: hr, hcID: hc}}.index(), controllertest.MapLister{hrID: hr, hcID: hc})
		for _, id := range []manifest.NamedResource{hrID, hcID, ociID} {
			if !f.ShouldReconcile(id) {
				t.Errorf("transitive dependency %s not selected: keep=%v", id, f.KeepNames())
			}
		}
	})
	t.Run("runtime", func(t *testing.T) {
		t.Parallel()
		parent := testutil.KustomizationID("apps", "parent")
		objects := controllertest.MapLister{hcID: hc}
		f := NewFilter(NewSet([]string{"parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{parent: "parent.yaml"}, objects: objects}.index(), objects)
		for _, id := range []manifest.NamedResource{hrID, hcID, ociID} {
			assert.Equal(t, f.ShouldReconcile(id), false)
		}

		added := f.AddEmitted(parent, hr)

		assert.Equal(t, len(added), 3)
		for _, id := range []manifest.NamedResource{hrID, hcID, ociID} {
			if !f.ShouldReconcile(id) || !slices.Contains(added, id) {
				t.Errorf("transitive dependency %s not selected and returned: added=%v, keep=%v", id, added, f.KeepNames())
			}
		}
	})
}

// TestFilter_ReverseEdgeCentralizedOCIRepository pins the reverse-edge
// fix: when a centralized OCIRepository (its own Kustomization tree,
// separate from the consuming HelmReleases) has its spec.ref.tag
// bumped, the HelmRelease that chartRefs it must re-render even though
// the HR's own files didn't change. Pre-fix the filter walked only
// consumer→source edges, so the changed OCIRepository never reached its
// consumer and `diff hr` printed nothing.
//
// Mirrors github.com/Boemeltrein/TalosCluster: OCIRepository under
// repositories/oci owned by a `repositories` KS, HelmRelease under
// clusters/.../envoy-gateway/app owned by a different KS. The HR is NOT
// in the store at resolve() time (render-driven discovery); the reverse
// edge is driven by the discovery-supplied consumerRefs instead.
func TestFilter_ReverseEdgeCentralizedOCIRepository(t *testing.T) {
	t.Parallel()
	const (
		ociFile = "repositories/oci/envoy-gateway.yaml"
		hrFile  = "clusters/main/kubernetes/networking/envoy-gateway/app/helm-release.yaml"
		sibFile = "clusters/main/kubernetes/networking/envoy-gateway/app/other-hr.yaml"
	)
	oci := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "envoy-gateway"}
	hr := testutil.HelmReleaseID("envoy-gateway", "envoy-gateway")
	// Sibling HR under the SAME owner KS that consumes a DIFFERENT source
	// — it must stay out of keep (the owner KS is ancestor-only, so it
	// can't cascade unrelated siblings in).
	sibling := testutil.HelmReleaseID("envoy-gateway", "other")
	otherOCI := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "something-else"}
	reposKS := &manifest.Kustomization{Name: "repositories", Namespace: "flux-system",
		Path: "repositories"}
	appKS := &manifest.Kustomization{Name: "envoy-gateway", Namespace: "flux-system",
		Path: "clusters/main/kubernetes/networking/envoy-gateway/app"}

	f := NewFilter(NewSet([]string{ociFile}), filterFixture{files: map[manifest.NamedResource]string{
		oci:             ociFile,
		hr:              hrFile,
		sibling:         sibFile,
		reposKS.Named(): "repositories/flux-entry.yaml",
		appKS.Named():   "clusters/main/kubernetes/networking/envoy-gateway/ks.yaml",
	}, root: "", objects: controllertest.MapLister{reposKS.Named(): reposKS, appKS.Named(): appKS}, refs: map[manifest.NamedResource][]manifest.NamedResource{
		hr:      {oci},
		sibling: {otherOCI},
	}, excluded: nil, readers: nil, changed: NewSet([]string{ociFile})}.index(), controllertest.MapLister{reposKS.Named(): reposKS, appKS.Named(): appKS})

	if !f.ShouldReconcile(oci) {
		t.Fatalf("changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(hr) {
		t.Fatalf("reverse edge: HR consuming the changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(appKS.Named()) {
		t.Errorf("HR owner KS must render (ancestor) to emit the HR; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR consuming an unchanged source must NOT be pulled in; keep=%v", f.KeepNames())
	}
	// Namespace scoping (scopedNamespaces → KeepNamespaces) gates diff
	// output: without the HR's namespace the command would scope it out
	// even if ShouldReconcile passed.
	if ns := f.KeepNamespaces(); ns != nil {
		if _, ok := ns["envoy-gateway"]; !ok {
			t.Errorf("HR namespace must be in keep-namespaces for diff scoping; got %v", ns)
		}
	} else {
		t.Errorf("KeepNamespaces returned nil; expected envoy-gateway in scope")
	}
}

func TestFilter_ReverseEdgeHelmChartKeepsEmitter(t *testing.T) {
	t.Parallel()
	const ociFile = "repositories/oci/source.yaml"
	oci := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "source"}
	chart := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "flux-system", Name: "chart"}
	sibling := testutil.HelmReleaseID("apps", "other")
	app := &manifest.Kustomization{Name: "app", Namespace: "flux-system", Path: "apps/app"}
	meta := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps"}
	objects := controllertest.MapLister{app.Named(): app, meta.Named(): meta}
	f := NewFilter(NewSet([]string{ociFile}), filterFixture{
		files: map[manifest.NamedResource]string{
			oci:          ociFile,
			chart:        "apps/app/chart.yaml",
			sibling:      "apps/app/other.yaml",
			app.Named():  "apps/app.yaml",
			meta.Named(): "flux-system/apps.yaml",
		},
		objects: objects,
		refs:    map[manifest.NamedResource][]manifest.NamedResource{chart: {oci}},
	}.index(), objects)

	for _, id := range []manifest.NamedResource{oci, chart, app.Named(), meta.Named()} {
		if !f.ShouldReconcile(id) {
			t.Errorf("source, chart and emitting ancestors must be kept: missing %s; keep=%v", id, f.KeepNames())
		}
	}
	assert.Equal(t, f.ShouldReconcile(sibling), false)
	f.AddEmitted(app.Named(), refObj(sibling))
	assert.Equal(t, f.ShouldReconcile(sibling), false)
}

// TestFilter_ReverseEdgeNoCascadeFromChangedConsumer pins the guard on
// the reverse edge: it fires ONLY for a source whose OWN file changed,
// never for a source merely referenced by a changed consumer. Two
// HelmReleases share one OCIRepository; editing hrA's file must keep
// hrA but must NOT drag hrB in via the shared source — otherwise a
// single app edit would reverse-cascade every sibling sharing a common
// app-template source.
func TestFilter_ReverseEdgeNoCascadeFromChangedConsumer(t *testing.T) {
	t.Parallel()
	shared := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}
	aID := testutil.HelmReleaseID("apps", "a")
	bID := testutil.HelmReleaseID("apps", "b")

	f := NewFilter(NewSet([]string{"apps/a/hr.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		aID:    "apps/a/hr.yaml",
		bID:    "apps/b/hr.yaml",
		shared: "repositories/oci/app-template.yaml",
	}, root: "", objects: controllertest.EmptyLister(), refs: map[manifest.NamedResource][]manifest.NamedResource{
		aID: {shared},
		bID: {shared},
	}, excluded: nil, readers: nil, changed: NewSet([]string{"apps/a/hr.yaml"})}.index(), controllertest.EmptyLister())

	if !f.ShouldReconcile(aID) {
		t.Fatalf("changed HR a must be in keep; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(bID) {
		t.Errorf("unchanged sibling HR b reverse-cascaded via shared source; keep=%v", f.KeepNames())
	}
}

// TestFilter_ForwardEdgeChangedHRKeepsChartRefSource is the forward
// counterpart to the reverse edge: when a HelmRelease's OWN file changes, its
// chartRef/sourceRef source must enter keep so the source controller fetches
// the chart. HelmReleases are render-driven (absent from the store at
// resolve()), so the stored reference lookup returns nil; the keep walk
// relies on the discovery-supplied consumerRefs for the forward edge too.
// Regression: a one-file HR edit left its OCIRepository unfetched and render
// failed "artifact not available" (surfaced after #566 removed the anonymous
// helm-registry fallback that had masked it). Mirrors k8s-gitops media/plex.
func TestFilter_ForwardEdgeChangedHRKeepsChartRefSource(t *testing.T) {
	t.Parallel()
	const (
		hrFile  = "kubernetes/apps/media/plex/app/helmrelease.yaml"
		sibFile = "kubernetes/apps/media/other/app/helmrelease.yaml"
		ociFile = "kubernetes/flux/repositories/app-template.yaml"
	)
	// Render-driven HRs are seeded with an empty namespace (discovery records
	// them pre-inheritance); the shared chartRef OCIRepository lives elsewhere.
	hr := testutil.HelmReleaseID("", "plex")
	sibling := testutil.HelmReleaseID("", "other")
	oci := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}

	f := NewFilter(NewSet([]string{hrFile}), filterFixture{files: // only plex's file changed
	map[manifest.NamedResource]string{
		hr:      hrFile,
		sibling: sibFile,
		oci:     ociFile,
	}, root: "", objects: controllertest.EmptyLister(), refs: map[manifest.NamedResource][]manifest.NamedResource{
		hr:      {oci},
		sibling: {oci}, // shares the same chart source
	}, excluded: nil, readers: nil, changed: NewSet([]string{hrFile})}.index(), controllertest.EmptyLister())

	if !f.ShouldReconcile(hr) {
		t.Fatalf("changed HelmRelease must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(oci) {
		t.Fatalf("forward edge: the changed HR's chartRef OCIRepository must be in keep so its chart is fetched; keep=%v", f.KeepNames())
	}
	// Keeping the shared source via plex's forward edge must NOT reverse-
	// cascade to a sibling HR that merely shares it (its file is unchanged).
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR sharing the chart source must NOT be pulled in; keep=%v", f.KeepNames())
	}
}

// TestFilter_SubstituteFromConfigMapKeepsProducerKustomization pins
// issue #418: a kept Kustomization with a non-Optional
// postBuild.substituteFrom ConfigMap must pull the producer KS that
// renders that CM into keep, even when the producer's own files
// didn't change. Without this, changed-only mode fails reconcile
// with "ConfigMap/flux-system/cluster-settings: dependency not found"
// because the producer never runs and the CM never materializes.
//
// The producer joins keep as ancestor-only — NOT primary — so its
// render output for unrelated siblings can't cascade into changed-
// only scope (the #204 keep-cascade rationale).
func TestFilter_SubstituteFromConfigMapKeepsProducerKustomization(t *testing.T) {
	t.Parallel()
	clusterAppsObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		}},
	}
	clusterVarsObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
		// The CM lives in a shared Component referenced by
		// cluster-vars — ownersOf returns cluster-vars for any
		// file under the resolved component path.
		Components: []string{"../../components/cluster-settings"},
	}
	ntfyObj := &manifest.Kustomization{
		Name: "ntfy", Namespace: "communication",
		Path: "kubernetes/apps/communication/ntfy/app",
	}
	hrObj := &manifest.HelmRelease{Name: "ntfy", Namespace: "communication"}
	// rawSettings is the discovery-indexed pre-render form:
	// Namespace="" because kustomize's namespace directive hasn't
	// run yet at file-walk time.
	rawSettings := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	rawSettingsID := rawSettings.Named()

	clusterApps := clusterAppsObj.Named()
	clusterVars := clusterVarsObj.Named()
	ntfy := ntfyObj.Named()
	hr := hrObj.Named()
	// renderedSettings is the form the consumer KS waits on once
	// kustomize's namespace directive has stamped flux-system onto
	// the rendered CM.
	renderedSettings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}

	f := NewFilter(NewSet([]string{"kubernetes/apps/communication/ntfy/app/helmrelease.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		clusterApps:   "kubernetes/flux/cluster-apps.yaml",
		clusterVars:   "kubernetes/flux/cluster-vars.yaml",
		ntfy:          "kubernetes/apps/communication/ntfy/ks.yaml",
		hr:            "kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
		rawSettingsID: "kubernetes/components/cluster-settings/cluster-settings.yaml",
	}, root: "", objects: controllertest.MapLister{
		clusterApps:   clusterAppsObj,
		clusterVars:   clusterVarsObj,
		ntfy:          ntfyObj,
		hr:            hrObj,
		rawSettingsID: rawSettings,
	}}.index(), controllertest.MapLister{
		clusterApps:   clusterAppsObj,
		clusterVars:   clusterVarsObj,
		ntfy:          ntfyObj,
		hr:            hrObj,
		rawSettingsID: rawSettings,
	})

	// The leaf HR's file changed, so ntfy + the cluster-apps
	// ancestor are pulled in; the rendered ConfigMap is a
	// substituteFrom dep of cluster-apps; and cluster-vars is the
	// producer of that ConfigMap.
	for _, id := range []manifest.NamedResource{clusterApps, ntfy, hr, renderedSettings, clusterVars} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
	// The producer is ancestor-only: AddEmitted from cluster-vars
	// to a sentinel child must reject (producer is NOT primary).
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	f.AddEmitted(clusterVars, refObj(sentinel))
	if f.ShouldReconcile(sentinel) {
		t.Errorf("producer cluster-vars must be ancestor-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly
// covers the runtime AddEmitted path: a primary parent KS emits a
// child KS at render time, and that child has a substituteFrom CM
// produced by an unchanged third KS. The producer must land in keep
// (so the CM materializes) but stay dependency-only — its render
// output for unrelated siblings is unchanged, so promoting it would
// re-introduce the #204 cascade. See #418.
func TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "parent-apps")
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		Path: "kubernetes/apps/child",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "shared-settings"},
		}},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		Path: "kubernetes/apps/settings",
	}
	producer := producerObj.Named()
	// CM with namespace="" mirrors the discovery pre-render form.
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "", Name: "shared-settings"}
	cmObj := &manifest.ConfigMap{Name: cmID.Name, Namespace: cmID.Namespace}

	f := NewFilter(NewSet([]string{"kubernetes/parent/parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		parent:   "kubernetes/parent/parent.yaml",
		producer: "kubernetes/apps/settings/ks.yaml",
		cmID:     "kubernetes/apps/settings/cm.yaml",
	}, root: "", objects: controllertest.MapLister{producer: producerObj, cmID: cmObj}}.index(), controllertest.MapLister{producer: producerObj, cmID: cmObj})

	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(producer) {
		t.Fatalf("precondition: producer must NOT be in keep before AddEmitted runs; keep=%v", f.KeepNames())
	}

	added := f.AddEmitted(parent, childObj)

	data := cmID
	data.Namespace = child.Namespace
	for _, id := range []manifest.NamedResource{child, data, producer} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
		if !slices.Contains(added, id) {
			t.Errorf("AddEmitted must return %s; added=%v", id, added)
		}
	}
	// Producer is dependency-only: AddEmitted from producer to a
	// sentinel child must reject because producer is not primary.
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	f.AddEmitted(producer, refObj(sentinel))
	if f.ShouldReconcile(sentinel) {
		t.Errorf("producer must be dependency-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v", f.KeepNames())
	}
}

// Dependency-only producers must be returned so callers can refire their arrivals.
func TestFilter_AddEmittedReturnsSubstituteFromProducer(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "parent-apps")
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		Path: "kubernetes/apps/child",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "shared-settings", Optional: true},
		}},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		Path: "kubernetes/apps/settings",
	}
	producer := producerObj.Named()
	dataID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "", Name: "shared-settings"}
	dataObj := &manifest.Secret{Name: dataID.Name, Namespace: dataID.Namespace}

	f := NewFilter(NewSet([]string{"kubernetes/parent/parent.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		parent:   "kubernetes/parent/parent.yaml",
		producer: "kubernetes/apps/settings/ks.yaml",
		dataID:   "kubernetes/apps/settings/secret.yaml",
	}, root: "", objects: controllertest.MapLister{producer: producerObj, dataID: dataObj}}.index(), controllertest.MapLister{producer: producerObj, dataID: dataObj})

	added := f.AddEmitted(parent, childObj)

	data := dataID
	data.Namespace = child.Namespace
	for _, id := range []manifest.NamedResource{child, data, producer} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
		if !slices.Contains(added, id) {
			t.Errorf("AddEmitted must return %s; added=%v", id, added)
		}
	}
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	if added := f.AddEmitted(producer, refObj(sentinel)); len(added) != 0 || f.ShouldReconcile(sentinel) {
		t.Errorf("dependency-only producer must reject unrelated child; added=%v, keep=%v", added, f.KeepNames())
	}
}

// Optional producer inputs must retain the complete chain as dependency-only.
func TestFilter_ChainedSubstituteFromProducers(t *testing.T) {
	t.Parallel()
	aObj := &manifest.Kustomization{
		Name: "consumer", Namespace: "flux-system",
		Path: "kubernetes/consumer",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cm-a", Optional: true},
		}},
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "flux-system",
		Path: "kubernetes/b",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "cm-b", Optional: true},
		}},
	}
	cObj := &manifest.Kustomization{
		Name: "producer-c", Namespace: "flux-system",
		Path: "kubernetes/c",
	}
	a, b, c := aObj.Named(), bObj.Named(), cObj.Named()

	cmA := &manifest.ConfigMap{Name: "cm-a", Namespace: ""}
	cmAID := cmA.Named()
	cmB := &manifest.Secret{Name: "cm-b", Namespace: ""}
	cmBID := cmB.Named()

	f := NewFilter(NewSet([]string{"kubernetes/consumer/file.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		a:     "kubernetes/consumer/ks.yaml",
		b:     "kubernetes/b/ks.yaml",
		c:     "kubernetes/c/ks.yaml",
		cmAID: "kubernetes/b/cm-a.yaml",
		cmBID: "kubernetes/c/cm-b.yaml",
	}, root: "", objects: controllertest.MapLister{a: aObj, b: bObj, c: cObj, cmAID: cmA, cmBID: cmB}}.index(), controllertest.MapLister{a: aObj, b: bObj, c: cObj, cmAID: cmA, cmBID: cmB})

	if !f.ShouldReconcile(a) {
		t.Fatalf("consumer A must be in keep (file change); keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(b) {
		t.Errorf("producer B (renders cm-a) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(c) {
		t.Errorf("chained producer C (renders cm-b consumed by B) must be in keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_SelfProducerSkippedInResolve pins the bjw-s
// self-substitute pattern: a KS may produce its own
// postBuild.substituteFrom CM. The BFS must NOT enqueue the KS as
// its own ancestor (would infinite-loop or self-edge). See #418.
func TestFilter_SelfProducerSkippedInResolve(t *testing.T) {
	t.Parallel()
	selfObj := &manifest.Kustomization{
		Name: "self", Namespace: "flux-system",
		Path: "kubernetes/self",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "self-settings"},
		}},
	}
	self := selfObj.Named()
	// CM lives at spec.path of self — so ownersOf returns self.
	cmObj := &manifest.ConfigMap{Name: "self-settings", Namespace: ""}
	cmID := cmObj.Named()

	f := NewFilter(NewSet([]string{"kubernetes/self/changed.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		self: "kubernetes/self/ks.yaml",
		cmID: "kubernetes/self/cm.yaml",
	}, root: "", objects: controllertest.MapLister{self: selfObj, cmID: cmObj}}.index(), controllertest.MapLister{self: selfObj, cmID: cmObj})

	if !f.ShouldReconcile(self) {
		t.Errorf("self KS must be in keep; keep=%v", f.KeepNames())
	}
	// Sanity: no infinite loop happened (we made it here). Also
	// confirm self only appears once in the producer index — not as
	// its own ancestor via enqueueAncestor.
	prods := f.producersFor(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "self-settings"})
	if !slices.Contains(prods, self) {
		t.Errorf("producers index should still contain self for cm-by-name lookup; got=%v", prods)
	}
}

// TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric producer-byName rule, mirroring
// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces. A producer KS
// renders a CM in namespace ns1; an unrelated producer in ns2
// renders a same-named CM in ns2. A Productions query on
// CM/ns1/shared MUST return only the ns1 producer — never the ns2
// one. See #418.
func TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	t.Parallel()
	// Both producer KSes spec a CM named "shared". The CMs land at
	// distinct on-disk paths, so ownersOf attributes each producer
	// only to its own CM file.
	aObj := &manifest.Kustomization{
		Name: "producer-a", Namespace: "ns1",
		Path: "p1",
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "ns2",
		Path: "p2",
	}
	a, b := aObj.Named(), bObj.Named()

	// The discovery-indexed CMs are in their respective on-disk
	// paths but have explicit namespace already (so they land in
	// producersByID only — NOT producersByName).
	cmNS1 := &manifest.ConfigMap{Name: "shared", Namespace: "ns1"}
	cmNS1ID := cmNS1.Named()
	cmNS2 := &manifest.ConfigMap{Name: "shared", Namespace: "ns2"}
	cmNS2ID := cmNS2.Named()

	f := NewFilter(NewSet([]string{"unrelated.yaml"}), filterFixture{files: map[manifest.NamedResource]string{
		a:       "p1/ks.yaml",
		b:       "p2/ks.yaml",
		cmNS1ID: "p1/cm.yaml",
		cmNS2ID: "p2/cm.yaml",
	}, root: "", objects: controllertest.MapLister{a: aObj, b: bObj, cmNS1ID: cmNS1, cmNS2ID: cmNS2}}.index(), controllertest.MapLister{a: aObj, b: bObj, cmNS1ID: cmNS1, cmNS2ID: cmNS2})

	gotNS1 := f.producersFor(cmNS1ID)
	if !slices.Equal(gotNS1, []manifest.NamedResource{a}) {
		t.Errorf("production owners(CM/ns1/shared) leaked across namespaces: got %v, want [%v]", gotNS1, a)
	}
	gotNS2 := f.producersFor(cmNS2ID)
	if !slices.Equal(gotNS2, []manifest.NamedResource{b}) {
		t.Errorf("production owners(CM/ns2/shared) leaked across namespaces: got %v, want [%v]", gotNS2, b)
	}
}

func TestFilter_SubstitutionGeneratorProducers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind string
		optional   bool
		emptyName  bool
	}{
		{name: "ConfigMap", kind: manifest.KindConfigMap},
		{name: "Secret", kind: manifest.KindSecret},
		{name: "optional ConfigMap", kind: manifest.KindConfigMap, optional: true},
		{name: "optional Secret", kind: manifest.KindSecret, optional: true},
		{name: "empty optional ConfigMap", kind: manifest.KindConfigMap, optional: true, emptyName: true},
		{name: "empty optional Secret", kind: manifest.KindSecret, optional: true, emptyName: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			name := "settings"
			if tc.emptyName {
				name = ""
			}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "app",
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: tc.kind, Name: name, Optional: tc.optional}}}}
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "data"}
			data := manifest.NamedResource{Kind: tc.kind, Namespace: "apps", Name: name}
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					consumer.Named(): {File: "flux/consumer.yaml"},
					producer.Named(): {File: "flux/producer.yaml"},
					data:             {File: "data/kustomization.yaml", Productions: testutil.ProductionEvidence([]manifest.NamedResource{producer.Named()})},
				},
				Claims: map[string][]manifest.NamedResource{"app/": {consumer.Named()}, "data/": {producer.Named()}},
			}
			f := NewFilter(NewSet([]string{"app/cm.yaml"}), index, controllertest.MapLister{consumer.Named(): consumer, producer.Named(): producer})
			for _, id := range []manifest.NamedResource{data, producer.Named()} {
				if got := f.ShouldReconcile(id); got == tc.emptyName {
					t.Errorf("%s retained = %t, empty reference name = %t", id, got, tc.emptyName)
				}
			}
			sentinel := testutil.KustomizationID("apps", "unrelated")
			if added := f.AddEmitted(producer.Named(), refObj(sentinel)); len(added) != 0 || f.ShouldReconcile(sentinel) {
				t.Errorf("dependency-only producer must reject unrelated child; added=%v, keep=%v", added, f.KeepNames())
			}
		})
	}
}

func TestFilter_LogValue(t *testing.T) {
	t.Parallel()
	f := &Filter{keep: map[manifest.NamedResource]struct{}{
		testutil.KustomizationID("", "z"): {},
		testutil.KustomizationID("", "a"): {},
	}}
	attrs := f.LogValue().Group()
	if attrs[0].Key != "size" || attrs[0].Value.Int64() != 2 {
		t.Errorf("size attribute = %v", attrs[0])
	}
	if got := attrs[1].Value.Any().([]string); !slices.Equal(got, f.KeepNames()) {
		t.Errorf("items = %v, want sorted keep names", got)
	}
}

func TestFilter_SourceFileReaderKeepsEmitter(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: "apps"}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "workloads/demo"}
	release := testutil.HelmReleaseID("apps", "demo")
	sibling := testutil.HelmReleaseID("apps", "sibling")
	objects := controllertest.MapLister{parent.Named(): parent, owner.Named(): owner}
	idx := filterFixture{
		files: map[manifest.NamedResource]string{
			parent.Named(): "flux/parent.yaml",
			owner.Named():  "apps/owner.yaml",
			release:        "workloads/demo/hr.yaml",
			sibling:        "workloads/demo/sibling.yaml",
		},
		objects: objects,
	}.index()
	idx.Readers["config/prod.yaml"] = []manifest.NamedResource{release}
	f := NewFilter(NewSet([]string{"config/prod.yaml"}), idx, objects)
	for _, id := range []manifest.NamedResource{release, owner.Named(), parent.Named()} {
		assert.Equal(t, f.ShouldReconcile(id), true)
	}
	assert.Equal(t, f.ShouldReconcile(sibling), false)
	for _, emitter := range []manifest.NamedResource{owner.Named(), parent.Named()} {
		assert.Equal(t, len(f.AddEmitted(emitter, refObj(sibling))), 0)
		assert.Equal(t, f.ShouldReconcile(sibling), false)
	}
	child := testutil.HelmReleaseID("apps", "rendered")
	assert.Diff(t, f.AddEmitted(release, refObj(child)), []manifest.NamedResource{child})
}

func TestFilter_AcceptedChildren(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"initial", "runtime", "unselected"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.ResourceSet{Name: "parent", Namespace: "ns"}
			child := &manifest.ResourceSet{Name: "child", Namespace: "ns"}
			leaf := &manifest.RawObject{APIVersion: "v1", Kind: manifest.KindConfigMap, Name: "leaf", Namespace: "ns"}
			emitter := &manifest.Kustomization{Name: "emitter", Namespace: "ns"}
			st.AddObjects([]manifest.BaseManifest{parent, emitter})
			st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
			st.RecordEmitted(child.Named(), &store.Emission{Children: []store.Child{{Obj: leaf}}, Complete: true})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				parent.Named(): {File: "parent.yaml"}, emitter.Named(): {File: "emitter.yaml"},
			}}
			file := "emitter.yaml"
			if phase == "initial" {
				file = "parent.yaml"
			}
			f := NewFilter(NewSet([]string{file}), index, st)
			switch phase {
			case "runtime":
				f.AddEmitted(emitter.Named(), parent)
			case "unselected":
				f.AddEmitted(parent.Named(), child)
			}
			for _, obj := range []manifest.BaseManifest{child, leaf} {
				assert.Equal(t, f.ShouldReconcile(obj.Named()), phase != "unselected")
			}
		})
	}
}

func TestFilter_ReverseAdmissionScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		dep  manifest.NamedResource
	}{
		{"auth", &manifest.GitRepository{Name: "repo", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "input"}}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}},
		{"provider", &manifest.ResourceSet{Name: "set", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Name: "input"}}}, manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "input"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objects := controllertest.MapLister{tc.obj.Named(): tc.obj}
			index := filterFixture{files: map[manifest.NamedResource]string{tc.obj.Named(): "consumer.yaml", tc.dep: "input.yaml"}, objects: objects}.index()
			f := NewFilter(NewSet([]string{"input.yaml"}), index, objects)
			assert.Equal(t, f.ShouldReconcile(tc.dep), true)
			assert.Equal(t, f.ShouldReconcile(tc.obj.Named()), false)
		})
	}
}

func TestFilter_ReferenceSnapshotUnchanged(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{Name: "app", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}}}}
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster"}
	untouched := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "sentinel"}
	backing := []manifest.NamedResource{source, untouched, untouched, untouched}
	want := slices.Clone(backing)
	objects := controllertest.MapLister{ks.Named(): ks}
	index := filterFixture{files: map[manifest.NamedResource]string{ks.Named(): "app.yaml", source: "source.yaml"}, refs: map[manifest.NamedResource][]manifest.NamedResource{ks.Named(): backing[:1]}, objects: objects}.index()
	NewFilter(NewSet([]string{"source.yaml"}), index, objects)
	assert.Diff(t, backing, want)
}

func TestFilter_ArtifactMaterializerDependencyOnly(t *testing.T) {
	t.Parallel()
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: target.Name}}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: target.Name}}
	other := &manifest.Kustomization{Name: "other", Namespace: "apps"}
	declaration := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator"}
	otherTarget := target
	otherTarget.Name = "app-unrelated"
	idx := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		consumer.Named(): {File: "consumer.yaml"},
		producer.Named(): {File: "producer.yaml"},
		other.Named():    {File: "other.yaml"},
		target:           {Productions: []manifest.Production{{Owner: producer.Named(), Declaration: declaration}, {Owner: producer.Named(), Declaration: declaration}, {Owner: other.Named(), Path: "artifact.yaml", File: "artifact.yaml"}}},
		otherTarget:      {Productions: []manifest.Production{{Owner: other.Named(), Declaration: declaration}}},
	}}
	objects := controllertest.MapLister{consumer.Named(): consumer, producer.Named(): producer, other.Named(): other}
	f := NewFilter(NewSet([]string{"consumer.yaml"}), idx, objects)
	if !f.ShouldReconcile(producer.Named()) || f.ShouldReconcile(other.Named()) || f.ShouldReconcile(otherTarget) {
		t.Fatalf("production closure: %v", f.KeepNames())
	}
	assert.Diff(t, f.producersFor(target), []manifest.NamedResource{producer.Named()})
	sentinel := &manifest.Kustomization{Name: "sentinel", Namespace: "apps"}
	f.AddEmitted(producer.Named(), sentinel)
	if f.ShouldReconcile(sentinel.Named()) {
		t.Fatal("dependency-only materializer included unrelated output")
	}
	child := &manifest.Kustomization{Name: "runtime", Namespace: "apps", SourceRef: consumer.SourceRef}
	f.AddEmitted(consumer.Named(), child)
	if !f.ShouldReconcile(child.Named()) || !f.ShouldReconcile(producer.Named()) {
		t.Fatal("runtime content dependency lost its producer")
	}
}
