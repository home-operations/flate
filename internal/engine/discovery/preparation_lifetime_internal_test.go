package discovery

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/gittree"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_RejectedPreparationRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [owner.yaml, ready.yaml]\n")
	testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML(
		"owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n",
	))
	testutil.WriteFile(t, root, "entry/ready.yaml", testutil.KustomizationYAML("ready", "apps", "  path: ./ready\n"))
	testutil.WriteFile(t, root, "ready/kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	result, err := Run(t.Context(), testConfig(t, Config{
		Input: source, Path: filepath.Join(root, "entry"), Store: st,
	}))
	assert.NoError(t, err)
	assert.Equal(t, result.Binding.Producer.ReportHidden(), true)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root + "-replacement", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), testutil.KustomizationID("apps", "owner")), false)
}

func TestPrepareDocuments_ReplacementRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "first"}
	first := owner.SourceID()
	st.SetArtifact(first, source)
	d := Session{cfg: testConfig(t, Config{Store: st}), loader: loader.New(st)}
	_, err := d.prepareDocuments(t.Context(), owner, source, nil)
	assert.NoError(t, err)
	replacement := *owner
	replacement.SourceRef.Name = "second"
	second := replacement.SourceID()
	st.SetArtifact(second, source)
	_, err = d.prepareDocuments(t.Context(), &replacement, source, nil)
	assert.NoError(t, err)
	st.SetArtifact(first, &store.SourceArtifact{LocalPath: root + "-first", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), false)
	st.SetArtifact(second, &store.SourceArtifact{LocalPath: root + "-second", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), true)
}

func TestPrepareDocuments_ImplicitSourceIgnore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ignore  string
		exclude bool
	}{
		{name: "included", ignore: "# no exclusions\n"},
		{name: "excluded", ignore: "apps/cm.yaml\n", exclude: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, ".sourceignore", tc.ignore)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t, root, "apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: default}\ndata: {greeting: hi}\n",
			)
			st := store.New()
			source := &store.SourceArtifact{
				LocalPath: root, LocalRoot: root, FS: os.DirFS(root), Content: "snapshot", InputAlias: false,
			}
			view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Source, Current: source.FS})
			assert.NoError(t, err)
			source = source.WithSourceView(view)
			st.SetArtifact(manifest.BootstrapSourceID, source)
			owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
			st.AddObject(owner)
			l := loader.New(st)
			defer l.Close()
			d := Session{cfg: testConfig(t, Config{Store: st}), loader: l}
			defer d.Close()
			prepared, err := d.prepareDocuments(t.Context(), owner, source, nil)
			if tc.exclude {
				assert.FatalIf(t, err == nil, "excluded referenced input rendered successfully")
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, strings.Contains(err.Error(), "cm.yaml"), true)
				assert.Equal(t, strings.Contains(err.Error(), "file does not exist"), true)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, prepared.complete, true)
			assert.FatalIf(t, len(prepared.children) != 1, "prepared children = %d", len(prepared.children))
			output, err := yaml.Marshal(prepared.children[0].Doc)
			assert.NoError(t, err)
			assert.Equal(t, string(output), `apiVersion: v1
data:
  greeting: hi
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: apps
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: hello
  namespace: default
`)
		})
	}
}

func TestPreparePendingProducer_ReferencedSubstitutionSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(t, root, "child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./${TARGET}\n"))
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	st.SetArtifact(manifest.BootstrapSourceID, source)
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	owner.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
	}
	st.AddObject(owner)
	l := loader.New(st)
	defer l.Close()
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	d := Session{cfg: testConfig(t, Config{Store: st}), loader: l}
	defer d.Close()
	for _, target := range []string{"one", "two"} {
		st.AddObject(&manifest.ConfigMap{
			Name: "settings", Namespace: "apps", Data: map[string]any{"TARGET": target},
		})
		if previous := d.prepared[owner.Named()]; previous != nil {
			assert.Equal(t, previous.support.Valid(), false)
		}
		prepared, prepareErr := d.preparePendingProducer(t.Context(), owner, nil)
		assert.NoError(t, prepareErr)
		assert.Equal(t, prepared, true)
		frame := d.prepared[owner.Named()]
		assert.FatalIf(t, frame == nil || len(frame.children) != 1, "prepared frame = %+v", frame)
		assert.Equal(t, frame.children[0].Obj.(*manifest.Kustomization).Path, "./"+target)
		assert.Equal(t, frame.support.Valid(), true)
		assert.Equal(t, owner.PostBuild.Substitute == nil, true)
	}
}

func TestPrepareProducer_RegistrationCancellation(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"producer", "pending producer", "qualification"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
			st := store.New()
			source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
			st.SetArtifact(manifest.BootstrapSourceID, source)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
			st.AddObject(owner)
			l := loader.New(st)
			defer l.Close()
			l.SourceRoot = root
			d := Session{cfg: testConfig(t, Config{Store: st}), loader: l}
			defer d.Close()
			failure := errors.New("retained binding failure")
			d.binding.Producer, d.binding.Failure = owner.Named(), failure
			prepared, err := d.prepareDocuments(t.Context(), owner, source, nil)
			assert.NoError(t, err)
			assert.Equal(t, prepared.complete, true)
			assert.Equal(t, prepared.registered, false)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			switch route {
			case "producer":
				err = d.prepareProducer(ctx, owner, source, nil)
			case "pending producer":
				var registered bool
				registered, err = d.preparePendingProducer(ctx, owner, nil)
				assert.Equal(t, registered, false)
			case "qualification":
				var reads store.ReadSet
				reads.Init(st, owner.Named(), nil)
				defer reads.Retire()
				defer reads.ReleaseCapture()
				q := &entryQualification{loader: l, session: &d}
				walk := qualificationWalk{q: q, store: st, reads: &reads}
				err = walk.prepare(ctx, &applicationEvidence{object: owner}, owner, source)
				assert.Equal(t, q.changed, false)
			}
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			assert.Equal(t, prepared.registered, false)
			assert.Equal(t, d.prepared[owner.Named()], prepared)
			assert.Equal(t, d.binding.Failure, failure)
			d.Close()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root + "-replacement", FS: source.FS},
			)
			assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), false)
		})
	}
}

func TestAcceptPrepared_RetiredSupport(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": "resources: [cm.yaml]\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n",
	})
	fixture := testReceiptTrees(t, t.TempDir(), sourcecache.RenderPersistenceDisabledReason() == "")
	st := store.New()
	source := &store.SourceArtifact{
		Kind:      manifest.KindGitRepository,
		URL:       "https://fixture.invalid/first",
		LocalPath: root,
		LocalRoot: root,
		FS:        os.DirFS(root),
	}
	st.SetArtifact(manifest.BootstrapSourceID, source)
	owner := &manifest.Kustomization{Name: "renderer", Namespace: "apps", Path: "."}
	st.AddObject(owner)
	l := loader.New(st)
	defer l.Close()
	l.SourceRoot = root
	d := Session{
		cfg: Config{
			Path: root, Store: st, Cache: fixture.owner, Trees: fixture.trees, PrepareDocuments: base.PrepareDocuments,
		},
		loader: l,
	}
	defer d.Close()
	assert.NoError(t, d.prepareProducer(t.Context(), owner, source, nil))
	initial := d.prepared[owner.Named()]
	assert.Equal(t, kustomize.RenderInputs(initial.receipt)["cm.yaml"], true)
	assert.NoError(t, d.acceptPrepared(t.Context()))
	projection, err := initial.receipt.Value().(interface {
		RenderPublication(context.Context, *sourcecache.RenderAcceptance) (sourcecache.RenderPublication, error)
	}).RenderPublication(t.Context(), &sourcecache.RenderAcceptance{Content: initial.content, Sources: []manifest.SourceEvidence{}})
	assert.NoError(t, err)
	key := sourcecache.BlobKey{Content: projection.Frame.Key, Render: sourcecache.RenderAccepted}
	var before []byte
	if fixture.owner.CanPersistRenders() {
		generation, _, _, lookupErr := fixture.owner.Blob(
			t.Context(),
			key,
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		assert.NoError(t, lookupErr)
		before, err = fs.ReadFile(generation, "content")
		assert.NoError(t, err)
	}
	priorSources := []manifest.SourceEvidence{{
		Source: source.HistoryIdentity(manifest.BootstrapSourceID), Path: "cm.yaml",
	}}
	delete(d.prepared, owner.Named())
	defer initial.retire(st)
	replacement := *source
	replacement.URL = "https://fixture.invalid/current"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	assert.NoError(t, d.prepareProducer(t.Context(), owner, &replacement, nil))
	prepared := d.prepared[owner.Named()]
	assert.Equal(t, prepared != initial, true)
	assert.Equal(t, prepared.support.Valid(), true)
	sources := append(slices.Clone(priorSources), st.ChildSourcePaths(owner.Named(), prepared.children, l.Index)...)
	provenance, err := fixture.owner.ProducersForPaths(t.Context(), sources)
	assert.NoError(t, err)
	if fixture.owner.CanPersistRenders() {
		assert.Equal(t, len(provenance[owner.Named()]) > 0, true)
	}
	prepared.retireSupport(st)
	changed := replacement
	changed.Content = "replacement"
	st.SetArtifact(manifest.BootstrapSourceID, &changed)
	assert.Equal(t, prepared.support.Valid(), false)
	assert.NoError(t, d.acceptPrepared(t.Context()))
	afterProvenance, err := fixture.owner.ProducersForPaths(t.Context(), sources)
	assert.NoError(t, err)
	assert.Diff(t, afterProvenance, provenance)
	if fixture.owner.CanPersistRenders() {
		generation, _, _, lookupErr := fixture.owner.Blob(
			t.Context(),
			key,
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		assert.NoError(t, lookupErr)
		after, readErr := fs.ReadFile(generation, "content")
		assert.NoError(t, readErr)
		assert.Diff(t, after, before)
		var metadata any
		frame, openErr := sourcecache.OpenRenderFrame(t.Context(), "kustomize-render", generation, &metadata)
		assert.NoError(t, openErr)
		_, complete := frame.Stored.History(owner.Named(), initial.content)
		assert.Equal(t, complete, true)
	}
}
