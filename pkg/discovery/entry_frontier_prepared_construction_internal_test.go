package discovery

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // Captures process-global Kustomize build diagnostics.
func TestEntryFrontier_PreparedConstructionDeclarations(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, tc := range []struct {
		name                string
		eager, dependencies bool
	}{
		{name: "prepared path"},
		{name: "eager full path", eager: true},
		{name: "prepared dependencies", dependencies: true},
		{name: "eager full dependencies", eager: true, dependencies: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			fixture := newPreparedConstructionFixture(t, tc.dependencies)
			if tc.eager {
				result, err := Run(t.Context(), fixture.cfg)
				assert.NoError(t, err)
				assertPreparedConstructionBinding(t, &result.Binding, fixture.cfg.Store, tc.dependencies)
				fixture.assertSubstitutions(t, result.DependencySubstitutions.Resolve)
				var reads store.ReadSet
				reads.Init(fixture.cfg.Store, fixture.parent.Named(), nil)
				defer reads.Retire()
				_, current := fixture.cfg.Loader.BuiltChildren(fixture.packageRoot(), fixture.parent.Named(), &reads)
				assert.Equal(t, current, false)
			} else {
				fixture.stages(t)
			}
			assertConstructionHasNoOutput(t, fixture.cfg.Store, fixture.parent)
			assert.Equal(t, strings.Count(logs.String(), `"msg":"kustomize: build"`), 2)
			assert.Equal(t, fixture.fetches.Load(), int32(0))
		})
	}
}

type preparedConstructionFixture struct {
	cfg          Config
	parent       *manifest.Kustomization
	fetches      atomic.Int32
	dependencies bool
}

func newPreparedConstructionFixture(t *testing.T, inputs bool) *preparedConstructionFixture {
	t.Helper()
	root := t.TempDir()
	resources := "applier.yaml"
	patch := ""
	dependency := ""
	if inputs {
		resources += ", config.yaml, follow.yaml"
		patch = "      - op: replace\n        path: /spec/dependsOn/0/name\n        value: prod-config\n"
		dependency = "  dependsOn: [{name: unpatched}]\n"
	}
	source := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"parent/kustomization.yaml": {
			Data: []byte(
				"resources: [" + resources + "]\npatches:\n  - target: {kind: Kustomization, name: applier}\n    patch: |-\n      - op: replace\n        path: /spec/path\n        value: ./entry\n" + patch,
			),
		},
		"parent/applier.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"applier",
					"apps",
					"  path: ./unpatched\n"+dependency+"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			),
		},
		"entry/kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	if inputs {
		files := source.FS.(fstest.MapFS)
		files["parent/config.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML("prod-config", "apps", "  path: ./config\n")),
		}
		files["parent/follow.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML("follow", "apps", "  path: ./followed\n")),
		}
		files["followed/supplier.yaml"] = &fstest.MapFile{
			Data: []byte(
				testutil.KustomizationYAML("supplier", "apps", "  postBuild: {substitute: {PREFIX: prod-}}\n"),
			),
		}
	}
	documents, err := manifest.DecodeDocs(
		strings.NewReader(
			testutil.KustomizationYAML(
				"parent",
				"apps",
				"  path: ./parent\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		),
		manifest.RequiredDocuments,
	)
	assert.NoError(t, err)
	parsed, err := manifest.ParseDoc(documents[0].Document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	parent := parsed.Object.(*manifest.Kustomization)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, source)
	st.SetArtifact(parent.SourceID(), source)
	st.AddObject(parent, parsed.Document)
	sourceObject, _ := parent.SourceID().BindLocal(root)
	st.AddObject(sourceObject)
	fixture := &preparedConstructionFixture{
		parent:       parent,
		dependencies: inputs,
		cfg: Config{
			Store:  st,
			Input:  source,
			Path:   filepath.Join(root, "entry"),
			Trees:  testTrees(t),
			Loader: loader.New(st),
		},
	}
	t.Cleanup(fixture.cfg.Loader.Retire)
	fixture.cfg.Trees.SetGitBaseFetcher(
		func(_ context.Context, _ kustomize.GitBaseRequest, _ cache.RetryConfig) (*store.SourceArtifact, error) {
			fixture.fetches.Add(1)
			return nil, manifest.NewError(manifest.ErrInput, nil, "unexpected construction source acquisition")
		},
	)
	return fixture
}

func (f *preparedConstructionFixture) packageRoot() string {
	return filepath.Join(f.cfg.Input.LocalPath, "parent")
}

func (f *preparedConstructionFixture) stages(t *testing.T) {
	t.Helper()
	d := newDiscoverer(&f.cfg)
	d.loader.SourceRoot = f.cfg.Input.LocalPath
	assert.NoError(t, d.loadManifests(t.Context(), f.cfg.Input.LocalPath, false))
	f.assertSubstitutions(t, d.substitutions.Resolve)
	assert.Equal(t, f.cfg.Store.GetObject(testutil.KustomizationID("apps", "applier")), nil)
	assertConstructionHasNoOutput(t, f.cfg.Store, f.parent)
	assert.NoError(t, d.prepareProducer(t.Context(), f.parent, f.cfg.Input, nil))
	prepared := d.prepared[f.parent.Named()]
	wantChildren := 1
	if f.dependencies {
		wantChildren = 3
	}
	assert.Equal(t, len(prepared.children), wantChildren)
	assert.Equal(t, prepared.receipt != nil, true)
	assert.NoError(t, d.prepareProducer(t.Context(), f.parent, f.cfg.Input, nil))
	assert.Equal(t, d.prepared[f.parent.Named()].receipt, prepared.receipt)
	var reads store.ReadSet
	reads.Init(f.cfg.Store, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	children, current := d.loader.BuiltChildren(f.packageRoot(), f.parent.Named(), &reads)
	assert.Equal(t, current, true)
	assert.Equal(t, len(children), wantChildren)
	for i := range children {
		assert.Equal(t, children[i].Entry == nil, true)
		assert.Equal(t, children[i].SourceSupport, f.parent.SourceID())
	}
	applier := slices.IndexFunc(
		children,
		func(child store.Child) bool { return child.Obj.Named() == testutil.KustomizationID("apps", "applier") },
	)
	assert.FatalIf(t, applier < 0, "prepared applier missing")
	application, ok := manifest.ApplicationFor(children[applier].Obj)
	assert.Equal(t, ok, true)
	assert.Equal(t, application.Path, "./entry")
	record, indexed := d.loader.Index.Lookup(children[applier].Obj.Named())
	assert.Equal(t, indexed, true)
	assert.Equal(t, record.Declaration, children[applier].Obj)
	assert.Equal(t, record.BuilderOwner, f.parent.Named())
	assert.Diff(t, children[applier].Productions, record.Productions)
	if f.dependencies {
		obj := children[applier].Obj.(*manifest.Kustomization)
		assert.Equal(t, obj.DependsOn[0].Name, "prod-config")
	}
	assertConstructionHasNoOutput(t, f.cfg.Store, f.parent)
	d.loader.Admit(f.cfg.Input.LocalPath)
	assertConstructionHasNoOutput(t, f.cfg.Store, f.parent)
	assert.NoError(t, d.acceptPrepared(t.Context()))
	assertConstructionHasNoOutput(t, f.cfg.Store, f.parent)
	if f.dependencies {
		incoming := children[applier]
		incoming.Entry = &application
		assert.Equal(t, d.loader.Options.Session == nil, true)
		narrowed, err := selection.New(
			f.cfg.Store,
			d.loader,
			selection.Request{
				Roots: []manifest.NamedResource{
					{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps"},
				},
			},
		)
		assert.NoError(t, err)
		d.loader.Options.Session = narrowed
		proposal, err := prepareSelection(t.Context(), t, d.loader,
			selection.PrepareRequest{Invocation: true, Generation: []store.Child{incoming}, Reads: &reads},
		)
		d.loader.Options.Session = nil
		assert.NoError(t, err)
		defer proposal.Retire()
		assert.Equal(t, slices.Contains(proposal.Needed, testutil.KustomizationID("apps", "prod-config")), true)
		assert.Equal(t, slices.Contains(proposal.Needed, testutil.KustomizationID("apps", "unpatched")), false)
	}
	assert.NoError(t, d.loadManifests(t.Context(), f.cfg.Input.LocalPath, true))
	assert.FatalIf(
		t,
		d.binding.Producer == (manifest.NamedResource{}),
		"shared entry walk did not bind the prepared applier",
	)
	assertPreparedConstructionBinding(t, &d.binding, f.cfg.Store, f.dependencies)
}

func (f *preparedConstructionFixture) assertSubstitutions(t *testing.T, resolve func(string) string) {
	t.Helper()
	if !f.dependencies {
		return
	}
	assert.Equal(t, resolve("${PREFIX:-dev-}config"), "prod-config")
	supplier, present := f.cfg.Loader.Index.Lookup(testutil.KustomizationID("apps", "supplier"))
	assert.FatalIf(
		t,
		!present || supplier.File != "followed/supplier.yaml",
		"independent followed supplier missing: %+v",
		supplier,
	)
}

func assertPreparedConstructionBinding(t *testing.T, binding *Binding, st *store.Store, inputs bool) {
	t.Helper()
	assert.Equal(t, binding.Producer, testutil.KustomizationID("apps", "applier"))
	assert.Equal(t, binding.Path, "entry")
	assert.Equal(t, binding.Entry == nil, true)
	applier, ok := st.GetObject(binding.Producer).(*manifest.Kustomization)
	assert.FatalIf(t, !ok, "bound applier was not admitted")
	assert.Equal(t, applier.Path, "./entry")
	assert.Equal(t, binding.Entry == nil, true)
	assert.Equal(t, binding.Source, applier.SourceID())
	if inputs {
		assert.FatalIf(t, len(applier.DependsOn) != 1, "effective dependency missing: %+v", applier.DependsOn)
		assert.Equal(t, applier.DependsOn[0].Name, "prod-config")
	} else {
		assert.Equal(t, len(applier.DependsOn), 0)
	}
}

func assertConstructionHasNoOutput(t *testing.T, st *store.Store, parent *manifest.Kustomization) {
	t.Helper()
	assert.Equal(t, st.GetArtifact(parent.Named(), store.ArtifactContent), nil)
	applier := testutil.KustomizationID("apps", "applier")
	assert.Equal(t, st.GetArtifact(applier, store.ArtifactContent), nil)
	assert.Equal(t, st.EmissionOwner(applier), manifest.NamedResource{})
	applierStatus, _ := st.GetStatus(applier)
	assert.Equal(t, applierStatus.Status == store.StatusReady, false)
	_, _, emission := st.GetEmission(parent.Named())
	assert.FatalIf(
		t,
		emission.Complete || emission.Render != nil || emission.Footprint() != nil,
		"construction fabricated runtime output: %+v",
		emission,
	)
	emission.Finish()
	status, _ := st.GetStatus(parent.Named())
	assert.Equal(t, status.Status == store.StatusReady, false)
	assert.Equal(t, emission.Render == nil, true)
}
