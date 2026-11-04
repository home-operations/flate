package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (f *admissionFixture) addUngated(id manifest.NamedResource) {
	if f.Enabled() {
		f.addRecursive(id)
	}
}

func (f *admissionFixture) addRecursive(id manifest.NamedResource) []manifest.NamedResource {
	parent := manifest.NamedResource{Kind: "fixture", Name: "selected"}
	f.mu.Lock()
	f.keep[parent] = primary
	f.mu.Unlock()
	return f.AddEmitted(parent, store.Child{Obj: refObj(id)}, f.objs)
}

func (f *admissionFixture) productionOwners(id manifest.NamedResource) []manifest.NamedResource {
	var owners []manifest.NamedResource
	s := f.last
	productions := s.productions(id)
	for i := range productions {
		p := productions[i]
		if owner := p.Materializer(id); owner != (manifest.NamedResource{}) && !slices.Contains(owners, owner) {
			owners = append(owners, owner)
		}
	}
	slices.SortFunc(owners, manifest.NamedResource.Compare)
	return owners
}

func refObj(id manifest.NamedResource) *manifest.RawObject {
	apiVersion := "v1"
	if id.Group != "" {
		apiVersion = id.Group + "/v1"
	}
	return &manifest.RawObject{APIVersion: apiVersion, Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
}

func TestFilter_ResolveDirectMatch(t *testing.T) {
	t.Parallel()
	hr := testutil.HelmReleaseID("ns", "x")
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/x/helmrelease.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{hr: "apps/x/helmrelease.yaml"},
				root:    "",
				objects: filtertest.EmptyLister(),
			}).index(),
			inventory: filtertest.EmptyLister(),
			root:      "",
		},
	)
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

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"components/volsync/pvc.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				ksPlex:  "apps/media/plex/app/ks.yaml",
				hrPlex:  "apps/media/plex/app/helmrelease.yaml",
				ksAtuin: "apps/default/atuin/app/ks.yaml",
				hrAtuin: "apps/default/atuin/app/helmrelease.yaml",
			}, root: "", objects: filtertest.MapLister{ksPlex: plex, ksAtuin: atuin}}).index(),
			inventory: filtertest.MapLister{ksPlex: plex, ksAtuin: atuin},
			root:      "",
		},
	)

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

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/media/plex/app/helmrelease.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				metaID: "flux/cluster/ks.yaml",
				plexID: "apps/media/plex/app/ks.yaml",
				hrPlex: "apps/media/plex/app/helmrelease.yaml",
			}, root: "", objects: filtertest.MapLister{metaID: meta, plexID: plex}}).index(),
			inventory: filtertest.MapLister{metaID: meta, plexID: plex},
			root:      "",
		},
	)

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

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/base/self-hosted/actual/helmrelease.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				parentID: "clusters/main/apps.yaml",
				leafID:   "apps/main/self-hosted/actual.yaml",
				hrActual: "apps/base/self-hosted/actual/helmrelease.yaml",
			}, root: "", objects: filtertest.MapLister{parentID: parent, leafID: leaf}}).index(),
			inventory: filtertest.MapLister{parentID: parent, leafID: leaf},
			root:      "",
		},
	)

	for _, id := range []manifest.NamedResource{hrActual, leafID, parentID} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

func TestFilter_AncestorKSDoesNotPullInUnrelatedSiblings(t *testing.T) {
	t.Parallel()
	meta := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system", Path: "apps"}
	plex := &manifest.Kustomization{Name: "plex", Namespace: "media", Path: "apps/media/plex/app"}
	atuin := &manifest.Kustomization{Name: "atuin", Namespace: "default", Path: "apps/default/atuin/app"}
	source := &manifest.GitRepository{Name: "source", Namespace: "flux-system"}
	release := &manifest.HelmRelease{
		Name:      "plex",
		Namespace: "media",
		Chart:     manifest.HelmChart{Name: "charts/plex", Source: source.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "charts/plex",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       source.Named().Kind,
						Name:       source.Named().Name,
						Namespace:  source.Named().Namespace,
						APIVersion: "",
					},
				},
			},
		},
	}
	independent := &manifest.Kustomization{Name: "chart-independent", Namespace: "default", Path: "independent"}
	hrAtuin := testutil.HelmReleaseID("default", "atuin")
	objects := store.New()
	objects.AddObjects([]manifest.BaseManifest{meta, plex, atuin, release, source, independent})
	input := &store.SourceArtifact{LocalPath: "/repo", FS: fstest.MapFS{}}
	objects.SetArtifact(manifest.BootstrapSourceID, input)
	objects.SetArtifact(source.Named(), input)
	for _, file := range []string{"apps/media/plex/app/helmrelease.yaml", "charts/plex/templates/cm.yaml"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			testFilterAncestorKSDoesNotPullInUnrelatedSiblingsCase(
				t,
				&testFilterAncestorKSDoesNotPullInUnrelatedSiblingsCaseInputs{
					meta:        &meta,
					plex:        &plex,
					atuin:       &atuin,
					source:      &source,
					release:     &release,
					independent: &independent,
					hrAtuin:     &hrAtuin,
					objects:     &objects,
					file:        &file,
				},
			)
		},
		)
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
	lister := filtertest.MapLister{externalID: external, plexID: plex}
	changed := []string{"apps/media/plex/app/helmrelease.yaml"}

	// Without the exclusion the root claim wins ancestor status — this
	// pins the cascade the fix removes (and the root-prefix match that
	// enables it).
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet(changed),
			index: new(filterFixture{
				files:    sourceFiles,
				root:     "",
				objects:  lister,
				refs:     nil,
				excluded: nil,
				readers:  nil,
				changed:  change.NewSet(changed),
			}).index(),
			inventory: lister,
			root:      "",
		},
	)
	if !f.ShouldReconcile(externalID) {
		t.Fatalf("precondition: root-claiming KS should be an ancestor without exclusions; keep=%v", f.KeepNames())
	}

	f = newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet(changed),
			index: new(filterFixture{
				files:    sourceFiles,
				root:     "",
				objects:  lister,
				refs:     nil,
				excluded: map[manifest.NamedResource]struct{}{externalID: {}},
				readers:  nil,
				changed:  change.NewSet(changed),
			}).index(),
			inventory: lister,
			root:      "",
		},
	)
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
		Name:      "consumer",
		Namespace: "apps",
		Path:      "consumers",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
		},
	}
	cm := &manifest.ConfigMap{Name: "settings"}
	files := map[manifest.NamedResource]string{
		producer.Named(): "flux/producer.yaml",
		child.Named():    "flux/child.yaml",
		consumer.Named(): "flux/consumer.yaml",
		cm.Named():       "../pinned/producers/cm.yaml",
	}
	objs := filtertest.MapLister{
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
		{name: "authored-pin", changed: "flux/producer.yaml", wantProducer: true, wantConsumer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testFilterPinnedOwnershipKeepsProducerCase(
				t,
				&testFilterPinnedOwnershipKeepsProducerCaseInputs{
					producer: &producer,
					child:    &child,
					consumer: &consumer,
					cm:       &cm,
					files:    &files,
					objs:     &objs,
					tt:       &tt,
				},
			)
		},
		)
	}
}

func TestFilter_ArtifactProducerIgnoresCheckoutRootClaim(t *testing.T) {
	t.Parallel()
	root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "./"}
	pinned := &manifest.Kustomization{Name: "pinned", Namespace: "apps", Path: "producers"}
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	file := "../artifact/producers/cm.yaml"
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"flux/unrelated.yaml"}), index: new(filterFixture{
			files: map[manifest.NamedResource]string{
				cm: file,
			},
			objects: filtertest.MapLister{root.Named(): root, pinned.Named(): pinned},
			changed: change.NewSet([]string{"flux/unrelated.yaml"}),
			root:    t.TempDir(),
			readers: func(path string) []manifest.FileReader {
				if path == file {
					return []manifest.FileReader{{Reader: pinned.Named(), Source: manifest.BootstrapSourceID}}
				}
				return nil
			},
		}).index(), inventory: filtertest.MapLister{root.Named(): root, pinned.Named(): pinned}, root: ""},
	)
	if got := f.productionOwners(cm); !slices.Equal(got, []manifest.NamedResource{pinned.Named()}) {
		t.Fatalf("production owners(%s) = %v, want [%s]", cm, got, pinned.Named())
	}
}

func TestFilter_FileOwnerFallbackCatchesResourcesEscape(t *testing.T) {
	t.Parallel()
	apps := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps/homelab"}
	appsID := apps.Named()
	const baseFile = "apps/base/postgres/cluster.yaml"

	fileOwners := func(file string) []manifest.FileReader {
		if file == baseFile {
			return []manifest.FileReader{{Reader: appsID, Source: manifest.BootstrapSourceID}}
		}
		return nil
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{baseFile}),
			index: new(filterFixture{
				files:    map[manifest.NamedResource]string{appsID: "apps/homelab/kustomization.yaml"},
				root:     "",
				objects:  filtertest.MapLister{appsID: apps},
				refs:     nil,
				excluded: nil,
				readers:  fileOwners,
				changed:  change.NewSet([]string{baseFile}),
			}).index(),
			inventory: filtertest.MapLister{appsID: apps},
			root:      "",
		},
	)
	if !f.ShouldReconcile(appsID) {
		t.Fatalf("owner supplied by the fileOwners fallback must be kept; keep=%v", f.KeepNames())
	}

	apps.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "cluster"}
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	other := &manifest.Kustomization{Name: "unrelated", Namespace: "flux-system", Path: "apps/other"}
	objects := filtertest.MapLister{apps.Named(): apps, source.Named(): source, other.Named(): other}
	for _, file := range []string{baseFile, "inputs/patch", "inputs/unrelated"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			fileOwners := func(file string) []manifest.FileReader {
				if file == baseFile || file == "inputs/patch" {
					return []manifest.FileReader{{Reader: apps.Named(), Source: manifest.BootstrapSourceID}}
				}
				return nil
			}
			changed := change.NewSet([]string{file})
			f := newAdmission(
				t,
				admissionConfig{
					changes: changed,
					index: new(filterFixture{
						files:   map[manifest.NamedResource]string{apps.Named(): "apps/homelab/kustomization.yaml"},
						objects: objects,
						readers: fileOwners,
						changed: changed,
					}).index(),
					inventory: objects,
					root:      "",
				},
			)
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

	fileOwners := func(file string) []manifest.FileReader {
		if file == baseFile {
			return []manifest.FileReader{{Reader: appsID, Source: manifest.BootstrapSourceID}}
		}
		return nil
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{baseFile}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				postgresID: "apps/base/postgres/kustomization.yaml",
				appsID:     "apps/homelab/kustomization.yaml",
			}, root: "", objects: filtertest.MapLister{postgresID: postgres, appsID: apps}, refs: nil, excluded: nil, readers: fileOwners, changed: change.NewSet([]string{baseFile})}).index(),
			inventory: filtertest.MapLister{postgresID: postgres, appsID: apps},
			root:      "",
		},
	)
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

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/base/postgres/cluster.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{appsID: "apps/homelab/kustomization.yaml"},
				root:    "",
				objects: filtertest.MapLister{appsID: apps},
			}).index(),
			inventory: filtertest.MapLister{appsID: apps},
			root:      "",
		},
	)
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
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"a.yaml", "b.yaml", "c.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{a: "a.yaml", b: "b.yaml", c: "c.yaml"},
				root:    "",
				objects: filtertest.MapLister{a: ksA, b: ksB, c: ksCluster},
			}).index(),
			inventory: filtertest.MapLister{a: ksA, b: ksB, c: ksCluster},
			root:      "",
		},
	)
	ns := f.KeepNamespaces()
	got := make([]string, 0, len(ns))
	for k := range ns {
		got = append(got, k)
	}
	slices.Sort(got)
	want := []string{"ns-a", "ns-b"}
	assert.ErrorIf(t, !slices.Equal(got, want), "KeepNamespaces=%v want %v", got, want)
}

func TestFilter_TransitiveDepsKustomization(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "flux-system",
			Namespace: "flux-system",
		},
		DependsOn: []kustomizev1.DependencyReference{{Namespace: "flux-system", Name: "repositories"}},
	}
	ksID := ks.Named()
	gitID := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "flux-system",
	}
	depID := testutil.KustomizationID("flux-system", "repositories")

	objects := filtertest.MapLister{ksID: ks, depID: refObj(depID)}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"ks.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{ksID: "ks.yaml"},
				objects: objects,
			}).index(),
			inventory: objects,
			root:      "",
		},
	)

	assert.Equal(t, f.ShouldReconcile(gitID), true)
	assert.Equal(t, f.ShouldReconcile(depID), false)
}

type testFilterAncestorKSDoesNotPullInUnrelatedSiblingsCaseInputs struct {
	meta        **manifest.Kustomization
	plex        **manifest.Kustomization
	atuin       **manifest.Kustomization
	source      **manifest.GitRepository
	release     **manifest.HelmRelease
	independent **manifest.Kustomization
	hrAtuin     *manifest.NamedResource
	objects     **store.Store
	file        *string
}

func testFilterAncestorKSDoesNotPullInUnrelatedSiblingsCase(
	t *testing.T,
	testCaseInput *testFilterAncestorKSDoesNotPullInUnrelatedSiblingsCaseInputs,
) {
	t.Helper()

	index := new(filterFixture{files: map[manifest.NamedResource]string{
		(*testCaseInput.meta).Named():        "flux/cluster/ks.yaml",
		(*testCaseInput.plex).Named():        "apps/media/plex/app/ks.yaml",
		(*testCaseInput.atuin).Named():       "apps/default/atuin/app/ks.yaml",
		(*testCaseInput.hrAtuin):             "apps/default/atuin/app/helmrelease.yaml",
		(*testCaseInput.release).Named():     "apps/media/plex/app/helmrelease.yaml",
		(*testCaseInput.source).Named():      "flux/source.yaml",
		(*testCaseInput.independent).Named(): "charts/plex/independent.yaml",
	}, root: "/repo", objects: (*testCaseInput.objects), refs: map[manifest.NamedResource][]manifest.NamedResource{(*testCaseInput.release).Named(): {(*testCaseInput.source).Named()}}}).index()
	index.Readers["charts/plex/templates/cm.yaml"] = []manifest.FileReader{
		{Reader: (*testCaseInput.release).Named(), Source: (*testCaseInput.source).Named(), Exact: true},
	}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{(*testCaseInput.file)}),
			index:     index,
			inventory: (*testCaseInput.objects),
			root:      "",
		},
	)
	for _, id := range []manifest.NamedResource{(*testCaseInput.meta).Named(), (*testCaseInput.plex).Named(), (*testCaseInput.release).Named(), (*testCaseInput.source).Named()} {
		if !f.ShouldReconcile(id) {
			t.Errorf("required resource %s omitted: %v", id, f.KeepNames())
		}
	}
	for _, id := range []manifest.NamedResource{(*testCaseInput.atuin).Named(), (*testCaseInput.hrAtuin), (*testCaseInput.independent).Named()} {
		if f.ShouldReconcile(id) {
			t.Errorf("unrelated resource %s selected: %v", id, f.KeepNames())
		}
	}
	owners, _ := index.OwnersOfFile("charts/plex/independent.yaml")
	{
		assert.FatalIf(
			t,
			slices.Contains(owners, (*testCaseInput.release).Named()),
			"%s",
			"chart read acquired render ownership",
		)
	}
}

type testFilterPinnedOwnershipKeepsProducerCaseInputs struct {
	producer **manifest.Kustomization
	child    **manifest.Kustomization
	consumer **manifest.Kustomization
	cm       **manifest.ConfigMap
	files    *map[manifest.NamedResource]string
	objs     *filtertest.MapLister
	tt       *struct {
		name         string
		changed      string
		wantProducer bool
		wantConsumer bool
		wantChild    bool
	}
}

func testFilterPinnedOwnershipKeepsProducerCase(
	t *testing.T,
	testCaseInput *testFilterPinnedOwnershipKeepsProducerCaseInputs,
) {
	t.Helper()

	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{testCaseInput.tt.changed}), index: new(filterFixture{
			files:   (*testCaseInput.files),
			objects: (*testCaseInput.objs),
			changed: change.NewSet([]string{testCaseInput.tt.changed}),
			pinned:  map[manifest.NamedResource]struct{}{(*testCaseInput.producer).Named(): {}},
			readers: func(file string) []manifest.FileReader {
				if file == "shared/cm.yaml" || file == "../pinned/producers/cm.yaml" {
					return []manifest.FileReader{
						{Reader: (*testCaseInput.producer).Named(), Source: manifest.BootstrapSourceID},
					}
				}
				return nil
			},
		}).index(), inventory: (*testCaseInput.objs), root: ""},
	)
	for _, id := range []manifest.NamedResource{
		(*testCaseInput.cm).Named(), {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
	} {
		if got := f.productionOwners(
			id,
		); !slices.Equal(
			got,
			[]manifest.NamedResource{(*testCaseInput.producer).Named()},
		) {
			t.Errorf("production owners(%s) = %v, want [%s]", id, got, (*testCaseInput.producer).Named())
		}
	}
	foreign := (*testCaseInput.cm).Named()
	foreign.Group, foreign.Namespace = "foreign.example", "apps"
	assert.Equal(t, len(f.productionOwners(foreign)), 0)
	foreign = (*testCaseInput.producer).Named()
	foreign.Group = "foreign.example"
	assert.Equal(t, f.ShouldReconcile(foreign), false)
	for _, want := range []struct {
		id   manifest.NamedResource
		keep bool
	}{
		{(*testCaseInput.producer).Named(), testCaseInput.tt.wantProducer},
		{(*testCaseInput.consumer).Named(), testCaseInput.tt.wantConsumer},
		{(*testCaseInput.child).Named(), testCaseInput.tt.wantChild},
	} {
		if got := f.ShouldReconcile(want.id); got != want.keep {
			t.Errorf("ShouldReconcile(%s) = %v, want %v; keep=%v", want.id, got, want.keep, f.KeepNames())
		}
	}
	if testCaseInput.tt.wantConsumer && testCaseInput.tt.changed != "flux/producer.yaml" {
		sentinel := testutil.KustomizationID("", "sentinel")
		f.AddEmitted((*testCaseInput.producer).Named(), store.Child{Obj: refObj(sentinel)}, f.objs)
		{
			assert.FatalIf(
				t,
				f.ShouldReconcile(sentinel),
				"%s",
				"dependency-only pinned producer selected an unrelated child",
			)
		}
	}
}
