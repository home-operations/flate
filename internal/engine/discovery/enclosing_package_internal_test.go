package discovery

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPlan_EnclosingPackageOperands(t *testing.T) {
	t.Parallel()
	const brokenRecipe = `patches:
- target: {kind: FluxInstance}
  patch: |-
    - op: add
      path: /spec/kustomize
      value:
        patches:
        - target: {kind: Kustomization, name: flux-system}
          patch: |
            - op: add
              path: /missing/path
              value: invalid
`
	for _, tc := range []enclosingPackageOperandsCase{
		{name: "reaching application", builder: "resources: [entry/plain.yaml, application.yaml]\n", want: true},
		{
			name: "reaching application with decode failure",
			builder: `resources: [entry/plain.yaml, application.yaml, release.yaml]
patches:
- target: {kind: HelmRelease, name: bad}
  patch: |-
    - op: replace
      path: /spec
      value: invalid
`,
			extra: map[string]string{"apps/release.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: bad, namespace: apps}
spec: {}
`},
			want: true,
		},
		{
			name:    "same application from nested builders",
			builder: "resources: [entry/plain.yaml, application.yaml]\n",
			extra:   map[string]string{"kustomization.yaml": "resources: [apps]\n"},
			want:    true,
		},
		{
			name:    "bad outer recipe leaves nested occurrence eligible",
			builder: "resources: [entry/plain.yaml, application.yaml]\n",
			extra:   map[string]string{"kustomization.yaml": "resources: [apps]\n" + brokenRecipe},
			want:    true, broken: true,
		},
		{
			name:    "bad private recipe leaves no match",
			builder: "resources: [entry/plain.yaml, application.yaml]\n" + brokenRecipe,
			want:    true, broken: true,
		},
		{
			name: "rejected private recipe shares an ordinary generated identity",
			builder: `resources: [entry/plain.yaml, application.yaml]
patches:
- target: {kind: FluxInstance}
  patch: |-
    - op: replace
      path: /spec/sync/path
      value: apps/disjoint
`,
			extra: map[string]string{
				"apps/disjoint/kustomization.yaml": "resources: []\nnamePrefix: unrelated-\n",
			},
			want: true, rejected: true,
		},
		{
			name:    "runnable application stays out of private evidence",
			builder: "resources: [entry/plain.yaml, application.yaml, runnable.yaml]\n",
			extra: map[string]string{"apps/runnable.yaml": `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ordinary, namespace: flux-system}
spec:
  interval: 1m
  path: elsewhere
  sourceRef: {kind: GitRepository, name: elsewhere}
`},
			want: true,
		},
		{name: "unlinked application", builder: "resources: [application.yaml]\n"},
		{name: "ordinary resources", builder: "resources: [entry/plain.yaml]\n"},
		{
			name:    "instance without sync",
			builder: "resources: [entry/plain.yaml, application.yaml]\n",
			extra: map[string]string{"apps/application.yaml": "apiVersion: fluxcd.controlplane.io/v1\n" +
				"kind: FluxInstance\nmetadata: {name: instance, namespace: flux-system}\n"},
			build: true,
		},
		{
			name:    "foreign FluxInstance kind",
			builder: "resources: [entry/plain.yaml, application.yaml]\n",
			extra: map[string]string{"apps/application.yaml": "apiVersion: example.test/v1\n" +
				"kind: FluxInstance\nmetadata: {name: instance, namespace: flux-system}\n"},
		},
		{
			name:    "transform without an instance",
			builder: "resources: [entry/plain.yaml]\nnamePrefix: private-\n",
			build:   true,
		},
		{name: "no authored builder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEnclosingPackageOperands(t, tc)
		})
	}
	t.Run("source includes", testEnclosingPackageSourceIncludes)
	t.Run("Windows origin keys", testEnclosingPackageWindowsOrigins)
}

type enclosingPackageOperandsCase struct {
	name, builder string
	extra         map[string]string
	want, broken  bool
	build         bool
	rejected      bool
}

func testEnclosingPackageOperands(t *testing.T, tc enclosingPackageOperandsCase) {
	t.Helper()
	const application = "application.yaml"
	const declaration = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: flux-system}
spec:
  distribution: {version: "2.x", registry: ghcr.io/fluxcd}
  sync:
    kind: GitRepository
    url: https://github.com/example/fleet.git
    ref: refs/heads/main
    path: apps
`
	const direct = "entry/plain.yaml"
	root := t.TempDir()
	files := fstest.MapFS{
		"apps/" + direct:      {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: plain}\n")},
		"apps/" + application: {Data: []byte(declaration)},
	}
	if tc.builder != "" {
		files["apps/kustomization.yaml"] = &fstest.MapFile{Data: []byte(tc.builder)}
	}
	for path, data := range tc.extra {
		files[path] = &fstest.MapFile{Data: []byte(data)}
	}
	for path, file := range files {
		testutil.WriteFile(t, root, path, string(file.Data))
	}
	cacheOwner := cachetest.New(t, "", cache.New)
	trees := kustomize.NewTreeCache(cacheOwner, nil, task.NewBounded(2))
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files}
	var recorder manifest.WorkRecorder
	cfg := Config{
		Store: store.New(), PrepareDocuments: base.PrepareDocuments,
		Input: input, Path: filepath.Join(root, "apps", "entry"), RepoRoot: root,
		Cache: cacheOwner, Trees: trees, Work: recorder.Scope(),
		SelfURLs: []string{"https://github.com/example/fleet.git"},
	}
	session, err := Begin(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	assert.NoError(t, constructTestSources(t.Context(), session))
	entries, err := session.Plan(t.Context())
	assert.NoError(t, err)
	assert.FatalIf(t, len(entries) != 1 || !entries[0].aggregate, "expected one synthetic entry: %v", entries)
	planned := entries[0].prepared.generatorPackages
	var wantPackages []string
	if tc.want || tc.build {
		wantPackages = []string{"apps"}
		if tc.extra["kustomization.yaml"] != "" {
			wantPackages = append(wantPackages, ".")
		}
	}
	assert.Diff(t, planned, wantPackages)
	admission := session.admission()
	admission.generatorPackages = planned
	inventories := assertEnclosingInventoryReuse(t, admission, recorder.Scope(), planned)

	generated := testutil.KustomizationID("flux-system", "flux-system")
	var current *manifest.Kustomization
	if tc.rejected {
		current = &manifest.Kustomization{
			Name:      generated.Name,
			Namespace: generated.Namespace,
			Path:      "apps/entry",
		}
		cfg.Store.AddObject(current)
	}
	var reads store.ReadSet
	reads.Init(cfg.Store, manifest.BootstrapSourceID, nil)
	t.Cleanup(reads.Close)
	q := &entryQualification{loader: session.loader}
	admission.qualification = q
	walk := qualificationWalk{q: q, admission: admission, store: cfg.Store, reads: &reads}
	for _, inventory := range inventories {
		batch := qualificationBatch{children: inventory.children, private: true}
		_, err := walk.follow(t.Context(), &batch, manifest.NamedResource{})
		assert.NoError(t, err)
	}
	var want []manifest.NamedResource
	id := manifest.NamedResource{
		Group: "fluxcd.controlplane.io", Kind: "FluxInstance", Namespace: "flux-system", Name: "instance",
	}
	if tc.want {
		want = []manifest.NamedResource{id}
	}
	if tc.broken && len(inventories) > 1 {
		want = append(want, id)
	}
	got := slices.Grow([]manifest.NamedResource(nil), len(walk.probed))
	for _, obj := range walk.probed {
		got = append(got, obj.Named())
	}
	assert.Diff(t, got, want)
	assert.Equal(t, len(q.matches) == 1, tc.want && !tc.rejected && (!tc.broken || len(inventories) > 1))
	if tc.rejected {
		assertRejectedPrivateOccurrence(t, &walk, current)
	}
	if tc.broken {
		assertPrivatePreparationError(t, &walk, inventories[0])
	}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "apps", manifest.NamedResource{}).
		Named()
	assert.Equal(t, session.prepared[owner] == nil, true)
}

func assertEnclosingInventoryReuse(
	t *testing.T, admission *Admission, work manifest.WorkScope, planned []string,
) []*declarationRender {
	t.Helper()
	inventories, err := admission.generatorInventories(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(inventories), len(planned))
	preparation := work.Source(manifest.BootstrapSourceID).RenderWork().Preparation()
	assert.Equal(t, preparation.Counts.OwnerPrepares, uint64(len(planned)))
	retained, err := admission.generatorInventories(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, preparation.Counts.OwnerPrepares, uint64(len(planned)))
	recorded := work.Snapshot(0)
	var builds int
	for i := range recorded {
		record := &recorded[i]
		if record.Key.Phase == manifest.WorkKustomize && record.Key.Owner == manifest.BootstrapSourceID {
			assert.Equal(t, record.Counts.Attempts, uint64(1))
			builds++
		}
	}
	assert.Equal(t, builds, len(planned))
	for i, dir := range slices.Backward(planned) {
		inventory := inventories[len(planned)-1-i]
		assert.Equal(t, inventory.owner.Path, dir)
		assert.Equal(t, retained[len(planned)-1-i], inventory)
	}
	return inventories
}

func assertRejectedPrivateOccurrence(t *testing.T, walk *qualificationWalk, current *manifest.Kustomization) {
	t.Helper()
	generated := current.Named()
	assert.Equal(t, walk.q.pending, false)
	for required := range walk.reads.Required() {
		t.Errorf("rejected recipe required %s", required)
	}
	assert.Diff(t, walk.store.Warnings(), []manifest.Warning(nil))
	assert.Equal(t, walk.store.GetDeclaration(generated), manifest.BaseManifest(current))
	assert.Equal(t, walk.store.GetArtifact(generated, store.ArtifactContent), store.Artifact(nil))

	matched, err := walk.visit(t.Context(), &applicationEvidence{object: current})
	assert.NoError(t, err)
	assert.Equal(t, matched, true)
	assert.Equal(t, walk.q.matches[0].object, manifest.BaseManifest(current))
}

func assertPrivatePreparationError(t *testing.T, walk *qualificationWalk, inventory *declarationRender) {
	t.Helper()
	invalid := walk.probed[0]
	_, found, preparationErr := walk.admission.PreparedGeneration(invalid)
	assert.Equal(t, found, true)
	assert.Equal(t, errors.Is(preparationErr, manifest.ErrInput), true)
	_, ordinaryErr := walk.visitGeneration(t.Context(), &applicationEvidence{object: invalid})
	assert.Equal(t, ordinaryErr, preparationErr)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	walk.probed = nil
	_, err := walk.follow(ctx, &qualificationBatch{
		children: inventory.children, private: true,
	}, manifest.NamedResource{})
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	matched, err := walk.visitGeneration(ctx, &applicationEvidence{object: invalid, private: true})
	assert.Equal(t, matched, false)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
}

func testEnclosingPackageWindowsOrigins(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "windows" {
		t.Skip("Windows host path folding")
	}

	const (
		shared = "apps/team"
		entry  = shared + "/entry"
	)
	release := func(name string) *fstest.MapFile {
		return &fstest.MapFile{Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n" +
			"metadata: {name: " + name + ", namespace: apps}\nspec: {}\n")}
	}
	workload := release("workload")
	builder := &fstest.MapFile{Data: []byte("resources: [entry/workload.yaml]\n")}
	root := t.TempDir()
	input := &store.SourceArtifact{
		Provider:  store.ProviderInput,
		LocalPath: filepath.Join(root, "input"),
		LocalRoot: filepath.Join(root, "PhysIcal"),
		FS: fstest.MapFS{
			"application.yaml": {Data: []byte(testutil.KustomizationYAML("application", "apps",
				"  interval: 1m\n  path: ./apps/team\n  sourceRef: {kind: GitRepository, name: alias}\n"))},
			"apps/kustomization.yaml":      {Data: []byte("resources: [team/entry/control.yaml]\n")},
			shared + "/kustomization.yaml": builder,
			entry + "/workload.yaml":       workload,
			entry + "/control.yaml":        release("control"),
		},
	}
	alias := &store.SourceArtifact{
		Provider:  store.ProviderAlias,
		LocalPath: filepath.Join(root, "alias"),
		LocalRoot: filepath.Join(root, "pHYSical"),
		FS: fstest.MapFS{
			shared + "/kustomization.yaml": builder,
			entry + "/workload.yaml":       workload,
		},
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "alias", Namespace: "apps"}
	sourceID := source.Named()
	st.AddObject(source)
	st.SetArtifact(sourceID, alias)
	cacheOwner := cachetest.New(t, "", cache.New)
	var recorder manifest.WorkRecorder
	work := recorder.Scope()
	ctx := t.Context()
	session, err := Begin(ctx, Config{
		Store: st, PrepareDocuments: base.PrepareDocuments, Input: input, RepoRoot: input.LocalPath,
		Path:      filepath.Join(input.LocalPath, entry),
		Selection: selection.Request{Roots: []manifest.NamedResource{testutil.HelmReleaseID("apps", "")}},
		Cache:     cacheOwner, Trees: kustomize.NewTreeCache(cacheOwner, nil, task.NewBounded(2)), Work: work,
	})
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	session.resolvedSources.record(sourceID, &entryResolution{
		object: source, input: input, artifact: alias, provider: store.ProviderAlias, entry: true,
	})
	assert.NoError(t, constructTestSources(ctx, session))
	entries, err := session.Plan(ctx)
	assert.NoError(t, err)
	assert.FatalIf(t, len(entries) != 1, "expected one planned entry: %v", entries)
	assert.Equal(t, entries[0].Path, filepath.Join(input.LocalPath, entry))

	publishedAlias, _ := st.GetArtifact(sourceID, store.ArtifactContent).(*store.SourceArtifact)
	bootstrapOrigin := session.cfg.Input.ApplicationOrigin(shared)
	aliasOrigin := publishedAlias.ApplicationOrigin(shared)
	key := kustpath.HostKey(bootstrapOrigin)
	assert.FatalIf(t, bootstrapOrigin == aliasOrigin || filepath.ToSlash(bootstrapOrigin) == key ||
		filepath.ToSlash(aliasOrigin) == key || kustpath.HostKey(aliasOrigin) != key,
		"expected two noncanonical spellings of one origin: %q and %q", bootstrapOrigin, aliasOrigin)

	application := testutil.KustomizationID("apps", "application")
	control := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "apps", manifest.NamedResource{}).Named()
	owners := []manifest.NamedResource{application, control}
	slices.SortFunc(owners, manifest.NamedResource.Compare)
	bootstrapPrepares := work.Source(manifest.BootstrapSourceID).RenderWork().Preparation()
	appPrepares := work.Recorder(&manifest.WorkKey{Owner: application, Source: sourceID}).
		Scope().
		RenderWork().
		Preparation()
	before := bootstrapPrepares.Counts.OwnerPrepares + appPrepares.Counts.OwnerPrepares
	ready, err := session.Load(ctx)
	assert.NoError(t, err)
	assert.FatalIf(t, !ready, "expected loaded selection: %v", session.PendingSources())
	var candidates []manifest.NamedResource
	for id, frame := range session.prepared {
		if frame.lookup != noLookup {
			candidates = append(candidates, id)
		}
	}
	slices.SortFunc(candidates, manifest.NamedResource.Compare)
	assert.Diff(t, candidates, owners)
	after := bootstrapPrepares.Counts.OwnerPrepares + appPrepares.Counts.OwnerPrepares
	assert.Equal(t, appPrepares.Counts.OwnerPrepares, uint64(1))
	// The independent enclosing package accounts for one preparation.
	assert.Equal(t, after-before, uint64(2))
	admission := session.admission()
	for _, id := range owners {
		frame := session.prepared[id]
		assert.FatalIf(t, frame == nil, "missing lookup inventory for %s", id)
		first, err := admission.packageInventory(ctx, frame.owner, frame.source)
		assert.NoError(t, err)
		second, err := admission.packageInventory(ctx, frame.owner, frame.source)
		assert.NoError(t, err)
		assert.Equal(t, second, first)
	}
	assert.Equal(t, bootstrapPrepares.Counts.OwnerPrepares+appPrepares.Counts.OwnerPrepares, after)
}

func testEnclosingPackageSourceIncludes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	source := &manifest.GitRepository{
		Name: "self", Namespace: "apps", URL: "https://github.com/example/fleet.git",
		Include: []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "included"}}},
	}
	st := store.New()
	st.AddObject(source)
	q := &entryQualification{}
	a := &Admission{
		store: st, input: input, path: filepath.Join(root, "apps"),
		qualification: q, resolvedSources: &entrySourcePublisher{mutable: map[manifest.NamedResource]entryResolution{}},
	}
	id := source.Named()
	a.resolvedSources.record(id, &entryResolution{input: input, artifact: input, object: source})
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent), store.Artifact(nil))

	instance := &manifest.RawObject{
		APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance",
		Name: "instance", Namespace: source.Namespace,
		Spec: map[string]any{"sync": map[string]any{
			"kind": manifest.KindGitRepository, "name": source.Name, "path": "apps",
			"url": source.URL, "ref": "refs/heads/main",
		}},
	}
	var reads store.ReadSet
	reads.Init(st, instance.Named(), nil)
	defer reads.Close()
	walk := qualificationWalk{q: q, admission: a, store: st, reads: &reads}
	matched, err := walk.visitGeneration(t.Context(), &applicationEvidence{object: instance, private: true})
	assert.NoError(t, err)
	assert.Equal(t, matched, false)
	assert.Equal(t, q.pending, false)
	assert.Equal(t, len(q.candidates), 0)
	assert.Equal(t, len(q.matches), 0)
	assert.Equal(t, len(maps.Collect(reads.Required())), 0)

	prepared, found, err := a.PreparedGeneration(instance)
	assert.NoError(t, err)
	assert.FatalIf(t, !found, "missing private generation")
	for i := range prepared.Children {
		child := &prepared.Children[i]
		if _, applies := runnableApplication(child.Obj); !applies {
			continue
		}
		matched, err = a.readApplication(t.Context(), st, &reads, &applicationEvidence{object: child.Obj})
		assert.NoError(t, err)
		assert.Equal(t, matched, false)
		assert.Equal(t, q.pending, true)
		assert.Diff(t, q.candidates, []manifest.NamedResource{id})
		assert.Diff(t, maps.Collect(reads.Required()), map[manifest.NamedResource]bool{id: false})
		return
	}
	t.Fatal("missing generated application")
}
