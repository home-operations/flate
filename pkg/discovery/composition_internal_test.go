package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDiscovery_IndexedIncludeDeclaration(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		name := "missing declaration with stale content"
		if indexed {
			name = "indexed outside entry"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			including := &manifest.GitRepository{
				Name:      "including",
				Namespace: "ns",
				Include: []sourcev1.GitRepositoryInclude{
					{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}},
				},
			}
			st.AddObject(including)
			reference := including.Composition().Includes[0].Reference
			files := fstest.MapFS{"entry/kustomization.yaml": {Data: []byte("resources: []\n")}}
			if indexed {
				files["declarations/input.yaml"] = &fstest.MapFile{
					Data: []byte(
						"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: input, namespace: ns}\nspec: {url: 'https://input.invalid/repo'}\n",
					),
				}
			}
			bootstrap := &store.SourceArtifact{LocalPath: root, FS: files}
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			st.SetArtifact(
				reference.NamedResource,
				&store.SourceArtifact{
					Content: "stale",
					FS:      fstest.MapFS{"file": {Data: []byte("stale readable content")}},
				},
			)
			l := loader.New(st)
			defer l.Close()
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			assert.Equal(t, l.IsFileIndexed(reference.NamedResource), indexed)
			assert.Equal(t, st.GetDeclaration(reference.NamedResource), manifest.BaseManifest(nil))
			if indexed {
				record, _ := l.Index.Lookup(reference.NamedResource)
				assert.Equal(t, record.File, "declarations/input.yaml")
				assert.Equal(
					t,
					strings.HasPrefix(record.Path, filepath.Join(root, "entry")+string(filepath.Separator)),
					false,
				)
			}
			a := &Admission{
				store:  st,
				loader: l,
				input:  bootstrap,
				path:   filepath.Join(root, "entry"),
				resolvedSources: &entrySourcePublisher{
					store:   st,
					mutable: map[manifest.NamedResource]entryResolution{},
				},
			}
			var reads store.ReadSet
			reads.Init(st, including.Named(), nil)
			defer reads.Retire()
			accepted, err := a.Publish(
				t.Context(),
				reference.NamedResource,
				&reads,
				func(child store.Child) (bool, error) {
					reads.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
					return true, nil
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, accepted, indexed)
			assert.Equal(t, st.GetDeclaration(reference.NamedResource) != nil, indexed)
			assert.Equal(t, a.resolvedSources.snapshot()[reference.NamedResource].synthetic, false)
			var check store.ReadSet
			check.Init(st, including.Named(), nil)
			defer check.Retire()
			waiter := depwait.Waiter{Store: &check, Missing: depwait.NewMissingPolicy(nil)}
			result := waiter.Classify(t.Context(), reference.DependencyRef, manifest.DrainCascade)
			want := schedule.DependencyFailed
			if indexed {
				want = schedule.Terminal
			}
			assert.Equal(t, result.Kind, want)
			if !indexed {
				st.SetArtifact(reference.NamedResource, nil)
				consumer := &manifest.Kustomization{
					Name:      "ordinary",
					Namespace: "ns",
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: manifest.KindGitRepository,
						Name: "input",
					},
				}
				st.AddObject(consumer)
				var ordinary store.ReadSet
				ordinary.Init(st, consumer.Named(), nil)
				defer ordinary.Retire()
				accepted, err = a.Publish(
					t.Context(),
					reference.NamedResource,
					&ordinary,
					func(store.Child) (bool, error) { return true, nil },
				)
				assert.NoError(t, err)
				assert.Equal(t, accepted, true)
				assert.Equal(t, a.resolvedSources.snapshot()[reference.NamedResource].synthetic, true)
			}
		})
	}
}

func TestDiscovery_ComposedSeedOwnership(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"initial", "runtime"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			cacheOwner := cachetest.New(t, "", cache.New)
			trees := kustomize.NewTreeCache(cacheOwner, nil, nil)
			defer trees.Close()
			release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: claimed, namespace: ns}\nspec:\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: including, namespace: ns}\n"
			owner := &manifest.Kustomization{
				Name:      "owner",
				Namespace: "ns",
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository,
					Name: "including",
				},
			}
			including := &manifest.GitRepository{
				Name:      "including",
				Namespace: "ns",
				URL:       "https://self.invalid/repo",
				Include: []sourcev1.GitRepositoryInclude{
					{
						GitRepositoryRef: manifest.LocalObjectReference{Name: "input"},
						FromPath:         "claimed.yaml",
						ToPath:           "apps/claimed.yaml",
					},
				},
			}
			otherSource := &manifest.GitRepository{
				Name:      "other",
				Namespace: including.Namespace,
				URL:       including.URL,
				Include:   slices.Clone(including.Include),
			}
			otherSource.Include[0].FromPath = "other.yaml"
			otherOwner := owner.Clone()
			otherOwner.Name, otherOwner.SourceRef.Name = "other-owner", otherSource.Name
			input := &manifest.GitRepository{Name: "input", Namespace: "ns", URL: "https://input.invalid/repo"}
			base := fstest.MapFS{
				"owner.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"owner",
							"ns",
							"  path: ./apps\n  sourceRef: {kind: GitRepository, name: including}\n",
						),
					),
				},
				"orphan.yaml":             {Data: []byte(strings.Replace(release, "name: claimed", "name: orphan", 1))},
				"apps/kustomization.yaml": {Data: []byte("resources: [claimed.yaml]\n")},
			}
			base["other-owner.yaml"] = &fstest.MapFile{
				Data: []byte(
					testutil.KustomizationYAML(
						"other-owner",
						"ns",
						"  path: ./apps\n  sourceRef: {kind: GitRepository, name: other}\n",
					),
				),
			}
			st.AddObject(otherOwner)
			st.AddObject(otherSource)
			st.AddObject(input)
			st.SetArtifact(
				input.Named(),
				&store.SourceArtifact{
					Content: "input",
					FS: fstest.MapFS{
						"claimed.yaml": {Data: []byte(release)},
						"other.yaml": {
							Data: []byte(strings.Replace(release, "name: claimed", "name: other-claimed", 1)),
						},
					},
				},
			)
			st.AddObject(owner)
			if phase == "initial" {
				st.AddObject(including)
			}
			cfg := Config{
				Input:    &store.SourceArtifact{LocalPath: root, FS: base},
				Path:     root,
				RepoRoot: root,
				SelfURLs: []string{"https://self.invalid/repo"},
				Store:    st,
				Cache:    cacheOwner,
				Trees:    trees,
			}
			if phase == "runtime" {
				d, err := Begin(t.Context(), cfg)
				assert.NoError(t, err)
				defer d.Close()
				assert.NoError(t, constructTestSources(t.Context(), d))
				_, err = d.resolveTestSources(t.Context(), false)
				assert.NoError(t, err)
				a := d.finishAdmission(t.Context())
				st.AddObject(including)
				var reads store.ReadSet
				reads.Init(st, owner.Named(), nil)
				defer reads.Retire()
				accepted, err := a.Publish(
					t.Context(),
					including.Named(),
					&reads,
					func(child store.Child) (bool, error) {
						reads.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
						return true, nil
					},
				)
				assert.NoError(t, err)
				assert.Equal(t, accepted, false)
				reads.Retire()
				reads.Init(st, owner.Named(), nil)
				accepted, err = a.Publish(
					t.Context(),
					including.Named(),
					&reads,
					func(child store.Child) (bool, error) {
						reads.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
						return true, nil
					},
				)
				assert.NoError(t, err)
				assert.Equal(t, accepted, true)
				tasks := task.NewBounded(2)
				controller := sourcectrl.New(st, tasks, cacheOwner)
				controller.Configure(sourcectrl.FetchOptions{Admission: a}, cache.RetryConfig{})
				var constructErr error
				tasks.Go(t.Context(), "construct source", func(ctx context.Context) {
					constructErr = controller.Construct(ctx, including.Named())
				})
				tasks.BlockTillDone()
				assert.NoError(t, constructErr)
			}
			result, err := Run(t.Context(), cfg)
			assert.NoError(t, err)
			defer result.Close()
			claimed, orphan := testutil.HelmReleaseID("ns", "claimed"), testutil.HelmReleaseID("ns", "orphan")
			record, indexed := result.Index.Lookup(claimed)
			assert.Equal(t, indexed, true)
			assert.Equal(t, record.Parent, owner.Named())
			completed := st.GetArtifact(including.Named(), store.ArtifactContent).(*store.SourceArtifact)
			prefix, relErr := filepath.Rel(root, filepath.Join(completed.LocalPath, "apps"))
			assert.NoError(t, relErr)
			assert.Diff(t, result.Index.Claims[filepath.ToSlash(prefix)+"/"], []manifest.NamedResource{owner.Named()})
			assert.Equal(t, slices.Contains(result.Index.Claims["apps/"], owner.Named()), false)
			assert.Equal(t, st.GetDeclaration(claimed), manifest.BaseManifest(nil))
			otherClaimed := testutil.HelmReleaseID("ns", "other-claimed")
			otherRecord, otherIndexed := result.Index.Lookup(otherClaimed)
			assert.Equal(t, otherIndexed, true)
			assert.Equal(t, otherRecord.Parent, otherOwner.Named())
			otherArtifact := st.GetArtifact(otherSource.Named(), store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, completed.LocalPath == otherArtifact.LocalPath, false)
			assert.Equal(
				t,
				completed.HistoryIdentity(including.Named()) == otherArtifact.HistoryIdentity(otherSource.Named()),
				false,
			)
			for _, owned := range []struct {
				id, owner, source manifest.NamedResource
				artifact          *store.SourceArtifact
			}{
				{claimed, owner.Named(), including.Named(), completed},
				{otherClaimed, otherOwner.Named(), otherSource.Named(), otherArtifact},
			} {
				productions := st.Productions(owned.id, result.Index)
				file, relErr := filepath.Rel(root, filepath.Join(owned.artifact.LocalPath, "apps", "claimed.yaml"))
				assert.NoError(t, relErr)
				assert.Diff(
					t,
					productions,
					[]manifest.Production{
						{Owner: owned.owner, Authored: owned.id, Source: owned.source, File: filepath.ToSlash(file)},
						{
							Owner: owned.owner, Authored: owned.id, Source: owned.source, File: filepath.ToSlash(file),
							Path: filepath.Join(owned.artifact.LocalPath, "apps", "claimed.yaml"),
						},
					},
				)
				data, readErr := fs.ReadFile(owned.artifact.FS, "apps/claimed.yaml")
				assert.NoError(t, readErr)
				assert.Equal(t, strings.Contains(string(data), "name: "+owned.id.Name+","), true)
			}

			record, indexed = result.Index.Lookup(orphan)
			assert.Equal(t, indexed, true)
			assert.Equal(t, record.Parent, manifest.NamedResource{})
			assert.Equal(t, st.GetDeclaration(orphan) != nil, true)
			_, statErr := fs.Stat(base, "apps/claimed.yaml")
			assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), true)
			data, readErr := fs.ReadFile(base, "apps/kustomization.yaml")
			assert.NoError(t, readErr)
			assert.Equal(t, string(data), "resources: [claimed.yaml]\n")
		})
	}
}

func TestDiscovery_ComposedBootstrapWaitsForInputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &manifest.GitRepository{Name: "input", Namespace: "ns"}
	including := &manifest.GitRepository{
		Name:      "including",
		Namespace: "ns",
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}}},
	}
	owner := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "ns",
		Path:      "./entry",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "including"},
	}
	for _, object := range []manifest.BaseManifest{input, including, owner} {
		st.AddObject(object)
	}
	bootstrap := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	selected := &store.SourceArtifact{
		LocalPath:  filepath.Join(root, "selected"),
		LocalRoot:  root,
		InputAlias: true,
		FS:         fstest.MapFS{},
	}
	a := &Admission{
		store: st,
		input: bootstrap,
		path:  filepath.Join(root, "entry"),
		resolvedSources: &entrySourcePublisher{store: st, mutable: map[manifest.NamedResource]entryResolution{
			including.Named(): {object: including, input: bootstrap, artifact: selected},
		}},
	}
	l := loader.New(st)
	t.Cleanup(l.Close)
	q := &entryQualification{loader: l, trees: testTrees(t)}
	a.qualification = q
	var previous kustomize.EntryEvidence
	for _, value := range []string{"before", "after"} {
		st.SetArtifact(input.Named(), &store.SourceArtifact{Content: value, FS: fstest.MapFS{}})
		if value == "after" {
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			evidence := applicationEvidence{object: owner}
			matched, err := a.readApplication(t.Context(), st, &reads, &evidence)
			assert.NoError(t, err)
			assert.Equal(t, matched, false)
			assert.Equal(t, q.pending, true)
			assert.Equal(t, evidence.source.artifact, (*store.SourceArtifact)(nil))
			reads.Retire()
		}
		support := new(store.ReadSet)
		support.Init(st, including.Named(), nil)
		t.Cleanup(support.Retire)
		support.GetArtifact(input.Named(), store.ArtifactContent)
		completed := (&store.SourceArtifact{Content: value, LocalPath: filepath.Join(root, "virtual"), LocalRoot: filepath.Join(root, "virtual"), FS: fstest.MapFS{
			"entry/kustomization.yaml": {Data: []byte("resources: [included.yaml]\n")},
			"entry/included.yaml":      {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: included}\ndata: {value: " + value + "}\n")},
		}}).WithReads(
			support,
		)
		st.SetArtifact(including.Named(), completed)
		var reads store.ReadSet
		reads.Init(st, owner.Named(), nil)
		t.Cleanup(reads.Retire)
		evidence := applicationEvidence{object: owner}
		matched, err := a.readApplication(t.Context(), st, &reads, &evidence)
		assert.NoError(t, err)
		assert.Equal(t, matched, true)
		assert.Equal(t, evidence.source.artifact, completed)
		assert.Equal(t, a.appliesEntry(&evidence.application), true)
		if value == "before" {
			binding := completed.Input(store.ArtifactContent | store.ArtifactRevision)
			binding.LocalPath, binding.LocalRoot = "", ""
			previous = kustomize.EntryEvidence{
				Application: testutil.ObjectDocument(owner),
				Receipt:     manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b }),
				Inputs: []kustomize.EntryInput{
					{Resource: including.Named(), Content: completed.Content, Binding: binding},
				},
			}
		}
		q.previous, q.matches, q.accepted = previous, nil, nil
		reused, pending := q.reusePrevious(t.Context(), a, st, &reads)
		assert.Equal(t, reused, value == "before")
		assert.Equal(t, pending, false)
		request := preparedRequest(t, &Config{}, owner, evidence.source.artifact)
		documents, _, renderErr := kustomize.Render(t.Context(), q.trees, request, cache.RetryConfig{})
		assert.NoError(t, renderErr)
		assert.Equal(t, len(documents), 1)
		assert.Diff(
			t,
			documents[0].Document,
			map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "included"},
				"data":       map[string]any{"value": value},
			},
		)
		data, readErr := fs.ReadFile(evidence.source.artifact.FS, "entry/included.yaml")
		assert.NoError(t, readErr)
		assert.Equal(
			t,
			string(data),
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: included}\ndata: {value: "+value+"}\n",
		)
	}
}

func TestEntryPrerequisites_ComposedSupport(t *testing.T) {
	t.Parallel()
	st := store.New()
	input := &manifest.GitRepository{Name: "input", Namespace: "ns"}
	including := &manifest.GitRepository{
		Name:      "including",
		Namespace: "ns",
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}}},
	}
	owner := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "ns",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "including"},
	}
	for _, object := range []manifest.BaseManifest{input, including, owner} {
		st.AddObject(object)
	}
	st.SetArtifact(input.Named(), &store.SourceArtifact{Content: "one", FS: fstest.MapFS{}})
	var support store.ReadSet
	support.Init(st, including.Named(), nil)
	defer support.Close()
	support.GetArtifact(input.Named(), store.ArtifactContent)
	st.SetArtifact(
		including.Named(),
		(&store.SourceArtifact{Content: "composed", FS: fstest.MapFS{}}).WithReads(&support),
	)
	var reads store.ReadSet
	reads.Init(st, owner.Named(), nil)
	defer reads.Close()
	l := loader.New(st)
	defer l.Close()
	e := entryPrerequisites{loader: l, reads: &reads}
	assert.Equal(t, e.eligible(owner, nil), true)
	assert.Equal(t, reads.Valid(), true)
	st.SetArtifact(input.Named(), &store.SourceArtifact{Content: "two", FS: fstest.MapFS{}})
	assert.Equal(t, reads.Valid(), false)
}

func TestAdmission_PublishRequiredDeclaration(t *testing.T) {
	t.Parallel()
	for _, required := range []bool{false, true} {
		name := "ordinary"
		if required {
			name = "include"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &manifest.GitRepository{Name: "input", Namespace: "ns", URL: "https://input.invalid/repository"}
			including := &manifest.GitRepository{
				Name:      "including",
				Namespace: "ns",
				Include: []sourcev1.GitRepositoryInclude{
					{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}},
				},
			}
			var owner manifest.BaseManifest = including
			if !required {
				owner = &manifest.Kustomization{
					Name:      "owner",
					Namespace: "ns",
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: manifest.KindGitRepository,
						Name: "input",
					},
				}
			}
			st.AddObject(owner)
			bootstrap := &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
			a := &Admission{
				store: st,
				input: bootstrap,
				resolvedSources: &entrySourcePublisher{store: st, mutable: map[manifest.NamedResource]entryResolution{
					input.Named(): {object: input, input: bootstrap},
				}},
			}
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Close()
			var accepted store.Child
			published, err := a.Publish(t.Context(), input.Named(), &reads, func(child store.Child) (bool, error) {
				accepted = child
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, required)
			assert.Equal(t, accepted.Obj == input, required)
			assert.Equal(t, st.GetDeclaration(input.Named()) == input, required)
			assert.Equal(t, st.GetArtifact(input.Named(), store.ArtifactContent), store.Artifact(nil))
		})
	}
}

func TestAdmission_RequiredIncludeCannotRestoreAlias(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	including := &manifest.GitRepository{
		Name:      "including",
		Namespace: "ns",
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}}},
	}
	required := including.Composition().Includes[0].Reference
	entry := &manifest.Kustomization{
		Name:      "reader",
		Namespace: "ns",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "input"},
	}
	ordinary := slices.Collect(manifest.References(entry, manifest.ReferenceRelations))[0]
	for _, order := range []string{"required first", "required last"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			refs := []manifest.Reference{required, ordinary}
			if order == "required last" {
				slices.Reverse(refs)
			}
			groups := map[manifest.NamedResource]*entrySourceGroup{}
			for _, ref := range refs {
				addEntrySource(groups, ref.NamedResource, including.Named(), &ref)
			}
			assert.Equal(t, groups[required.NamedResource].reference.RequiresDeclaration, true)
		})
	}
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	old, _ := required.NamedResource.BindLocal(root)
	previous := entryResolution{object: old, input: input, synthetic: true}
	a := &Admission{
		store: store.New(),
		input: input,
		resolvedSources: &entrySourcePublisher{
			mutable: map[manifest.NamedResource]entryResolution{required.NamedResource: previous},
		},
	}
	resolved := a.classifySource(required.NamedResource, &sourceRequest{reference: required})
	assert.Equal(t, resolved.synthetic, false)
	assert.Equal(t, resolved.object, manifest.BaseManifest(nil))
	restored := a.classifySource(ordinary.NamedResource, &sourceRequest{reference: ordinary})
	assert.Equal(t, restored.synthetic, true)
	assert.Equal(t, restored.object, old)
}

func TestDiscovery_PreparedIncludeAuthority(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			cacheOwner := cachetest.New(t, "", cache.New)
			trees := kustomize.NewTreeCache(cacheOwner, nil, nil)
			defer trees.Close()
			files := fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte(`resources: [source.yaml, owner.yaml, instance.yaml]
patches:
- target: {kind: GitRepository, name: including}
  patch: '[{"op":"replace","path":"/spec/include/0/repository/name","value":"prepared-input"}]'
- target: {kind: Kustomization, name: owner}
  patch: '[{"op":"replace","path":"/spec/sourceRef/name","value":"including"}]'
- target: {kind: FluxInstance, name: flux}
  patch: '[{"op":"replace","path":"/spec/sync/name","value":"prepared-projected"}]'
`)},
				"entry/source.yaml": {Data: []byte(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: including, namespace: ns}
spec:
  url: https://self.invalid/repo
  include:
  - repository: {name: raw-input}
    fromPath: payload.yaml
    toPath: leaf/copied.yaml
`)},
				"entry/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "ns",
					"  path: ./leaf\n  sourceRef: {kind: GitRepository, name: raw-source}\n"))},
				"entry/instance.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: ns}
spec:
  sync: {kind: GitRepository, name: raw-projected, url: 'https://projected.invalid/repo', ref: refs/heads/main, path: ./entry}
`)},
				"leaf/kustomization.yaml": {Data: []byte("resources: [copied.yaml]\n")},
			}
			bootstrap := &store.SourceArtifact{LocalPath: root, FS: files}
			d, err := Begin(t.Context(), Config{
				Path: filepath.Join(root, "entry"), Input: bootstrap, Store: st,
				SelfURLs: []string{"https://self.invalid/repo"}, Cache: cacheOwner, Trees: trees,
				Selection: selection.Request{Changed: changed},
			})
			assert.NoError(t, err)
			defer d.Close()
			assert.NoError(t, constructTestSources(t.Context(), d))
			d.loader.SourceRoot = root
			_, err = d.loader.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, d.loader.FinalizeIndex(t.Context(), root, false))
			assert.Equal(t, d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}), !changed)
			including := (&manifest.GitRepository{Name: "including", Namespace: "ns"}).Named()
			rawInput := (&manifest.GitRepository{Name: "raw-input", Namespace: "ns"}).Named()
			rawSource := (&manifest.GitRepository{Name: "raw-source", Namespace: "ns"}).Named()
			rawProjected := (&manifest.GitRepository{Name: "raw-projected", Namespace: "ns"}).Named()
			evidence := d.loader.RootEvidence(including, nil, true)
			assert.Equal(t, evidence.Occurrence.RequiresBuild, true)
			assert.Equal(t, evidence.Record.Builder, "")
			groups := map[manifest.NamedResource]*entrySourceGroup{}
			assert.NoError(t, d.addLoadedSources(t.Context(), groups, true))
			assert.Equal(t, groups[including], (*entrySourceGroup)(nil))
			assert.Equal(t, groups[rawInput], (*entrySourceGroup)(nil))
			pathObjects := d.loader.PathObjects()
			assert.Equal(t, len(pathObjects), 2)
			for _, child := range pathObjects {
				assert.Equal(t, child.RequiresBuild, true)
			}
			groups = map[manifest.NamedResource]*entrySourceGroup{}
			d.addPathSources(groups, true)
			assert.Equal(t, groups[rawSource], (*entrySourceGroup)(nil))
			assert.Equal(t, groups[rawProjected], (*entrySourceGroup)(nil))
			groups = map[manifest.NamedResource]*entrySourceGroup{}
			d.addMissingIndexedSources(groups)
			assert.Equal(t, groups[rawInput], (*entrySourceGroup)(nil))
			assert.Equal(t, groups[rawSource], (*entrySourceGroup)(nil))
			parent := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
			st.AddObject(parent)
			assert.NoError(t, d.prepareProducer(t.Context(), parent, bootstrap, nil))
			evidence = d.loader.RootEvidence(including, nil, true)
			assert.Equal(t, evidence.Record.Builder != "", true)
			groups = map[manifest.NamedResource]*entrySourceGroup{}
			assert.NoError(t, d.addLoadedSources(t.Context(), groups, true))
			preparedInput := (&manifest.GitRepository{Name: "prepared-input", Namespace: "ns"}).Named()
			assert.FatalIf(t, groups[preparedInput] == nil, "prepared include reference missing")
			assert.Equal(t, groups[preparedInput].reference.RequiresDeclaration, true)
			assert.Equal(t, groups[rawInput], (*entrySourceGroup)(nil))
			assert.Equal(t, groups[rawSource], (*entrySourceGroup)(nil))
			assert.Equal(t, groups[rawProjected], (*entrySourceGroup)(nil))
			groups = map[manifest.NamedResource]*entrySourceGroup{}
			d.addPathSources(groups, true)
			assert.FatalIf(t, groups[including] == nil, "prepared ordinary source missing")
			projected := (&manifest.GitRepository{Name: "prepared-projected", Namespace: "ns"}).Named()
			assert.FatalIf(t, groups[projected] == nil, "prepared projected source missing")
			assert.Equal(t, groups[projected].projected, true)
			groups = map[manifest.NamedResource]*entrySourceGroup{}
			d.addMissingIndexedSources(groups)
			if !changed {
				assert.Equal(t, evidence.Scope.EntryKustomize, true)
				assert.Equal(t, evidence.Scope.Eligibility != selection.RootInactive, true)
				assert.FatalIf(
					t,
					groups[including] == nil,
					"prepared active-entry ordinary reference missing in full mode",
				)
			}
			for _, child := range d.prepared[parent.Named()].children {
				st.AddObject(child.Obj, child.Doc)
			}
			input := &manifest.GitRepository{Name: "prepared-input", Namespace: "ns", URL: "https://input.invalid/repo"}
			st.AddObject(input)
			payload := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: copied, namespace: ns}\n"
			st.SetArtifact(input.Named(), &store.SourceArtifact{Content: "input", FS: fstest.MapFS{
				"payload.yaml": {Data: []byte(payload)},
			}})
			assert.NoError(t, constructTestSources(t.Context(), d))
			_, err = d.resolveTestSources(t.Context(), true)
			assert.NoError(t, err)
			completed, ok := st.GetArtifact(including, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok, "prepared composed tree missing")
			data, err := fs.ReadFile(completed.FS, "leaf/copied.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(data), payload)
			assert.Equal(t, st.GetObject(rawInput), manifest.BaseManifest(nil))
			assert.Equal(t, st.GetArtifact(rawInput, store.ArtifactContent), store.Artifact(nil))
		})
	}
}
