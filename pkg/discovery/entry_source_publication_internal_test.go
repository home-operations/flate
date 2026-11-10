package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_Publish(t *testing.T) {
	t.Parallel()
	var absent *Admission
	published, err := absent.Publish(t.Context(), manifest.NamedResource{}, manifest.WorkScope{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, published, false)
	for _, phase := range []string{"construction", "runtime"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/file", "working tree")
			st := store.New()
			d := Session{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
			t.Cleanup(d.loader.Close)
			t.Cleanup(d.Close)
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.NoError(t, err)
			a := d.admission()
			if phase == "runtime" {
				assert.Equal(t, d.finishAdmission(t.Context()), a)
				assert.Equal(t, a.session == nil, true)
			}
			assert.Equal(t, a.resolvedSources, d.resolvedSources)
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "accepted",
			}
			var offered manifest.BaseManifest
			calls := 0
			published, err := a.Publish(t.Context(), id, manifest.WorkScope{}, func(child store.Child) (bool, error) {
				calls++
				offered = child.Obj
				assert.Equal(t, child.Obj.Named(), id)
				assert.Equal(t, st.GetObject(id) == nil, true)
				assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
				_, alreadyRecorded := a.resolvedSources.snapshot()[id]
				assert.Equal(t, alreadyRecorded, false)
				st.AddObject(child.Obj)
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, true)
			assert.Equal(t, calls, 1)
			resolution, recorded := a.resolvedSources.snapshot()[id]
			assert.Equal(t, recorded, true)
			assert.Equal(t, resolution.object == offered, true)
			assert.Equal(t, st.GetObject(id) == offered, true)
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == resolution.artifact, true)
			assert.Equal(t, resolution.input == a.input, true)
			assert.Equal(t, manifest.Equal(resolution.object, resolution.git), true)
			assert.Equal(t, resolution.git.URL, "file://"+root)
			assert.Equal(t, resolution.synthetic, true)
			assert.Equal(t, resolution.pinned, false)
			assert.Equal(t, len(resolution.warnings), 0)
			assert.Equal(t, resolution.viewKey != "", true)
			assert.Equal(t, resolution.artifact.InputAlias, true)
			assert.Equal(t, resolution.artifact.LocalRoot, root)
			assert.Equal(t, resolution.artifact.Key, "")
			body, readErr := fs.ReadFile(resolution.artifact.FS, "apps/file")
			assert.NoError(t, readErr)
			assert.Equal(t, string(body), "working tree")
			status, ok := st.GetStatus(id)
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Status, store.StatusReady)
			assert.Equal(t, status.Reason, store.ReasonSucceeded)
			assert.Equal(t, status.Message, "local entry source")

			failure := errors.New("acceptance failed")
			for _, rejected := range []struct {
				name     string
				accepted bool
				err      error
			}{{name: "rejected"}, {name: "acceptance-error", accepted: true, err: failure}} {
				id.Name = rejected.name
				published, err = a.Publish(t.Context(), id, manifest.WorkScope{}, func(store.Child) (bool, error) {
					return rejected.accepted, rejected.err
				})
				assert.Equal(t, err, rejected.err)
				assert.Equal(t, published, false)
				assert.Equal(t, st.GetObject(id) == nil, true)
				assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
				_, recorded = a.resolvedSources.snapshot()[id]
				assert.Equal(t, recorded, false)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			id.Name = "resolution-error"
			published, err = a.Publish(ctx, id, manifest.WorkScope{}, func(store.Child) (bool, error) {
				t.Fatal("acceptance ran after failed resolution")
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, false)
			_, recorded = a.resolvedSources.snapshot()[id]
			assert.Equal(t, recorded, false)
			id.Kind, id.Name = manifest.KindOCIRepository, "unsupported"
			published, err = a.Publish(t.Context(), id, manifest.WorkScope{}, func(store.Child) (bool, error) {
				t.Fatal("acceptance ran without a local source")
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, false)

			id.Kind, id.Name = manifest.KindGitRepository, "accepted-cancellation"
			ctx, cancel = context.WithCancel(t.Context())
			t.Cleanup(cancel)
			published, err = a.Publish(ctx, id, manifest.WorkScope{}, func(store.Child) (bool, error) {
				cancel()
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, true)
			status, ok = st.GetStatus(id)
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Status, store.StatusReady)
		})
	}
}

func TestAdmission_ResolvedSourcesFreeze(t *testing.T) {
	t.Parallel()
	assertResolution := func(t *testing.T, got, want entryResolution) {
		t.Helper()
		assert.Equal(t, got.object == want.object, true)
		assert.Equal(t, got.input, want.input)
		assert.Equal(t, got.git, want.git)
		assert.Equal(t, got.synthetic, want.synthetic)
		assert.Equal(t, got.pinned, want.pinned)
		assert.Equal(t, got.artifact, want.artifact)
		assert.Diff(t, got.warnings, want.warnings)
		assert.Equal(t, got.viewKey, want.viewKey)
	}
	for _, mode := range []string{"run", "freeze"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/file", "working tree")
			st := store.New()
			cfg := Config{Path: root, Store: st}
			var session *Session
			var result *Result
			var err error
			if mode == "run" {
				result, err = Run(t.Context(), cfg)
			} else {
				session, err = Begin(t.Context(), cfg)
				assert.NoError(t, err)
				result, err = session.Freeze(t.Context())
			}
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			a := result.Admission
			owner := a.resolvedSources
			assert.Equal(t, owner.current.Load() != nil, true)
			assert.Equal(t, owner.mutable == nil, true)
			assert.Equal(t, owner.store, st)
			assert.Equal(t, owner.root, root)
			assert.Equal(t, a.session == nil, true)
			assert.Equal(t, len(owner.snapshot()), 1)
			before := owner.snapshot()
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "late",
			}
			published, err := a.Publish(t.Context(), id, manifest.WorkScope{}, func(store.Child) (bool, error) {
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, true)
			_, retainedInOld := before[id]
			assert.Equal(t, retainedInOld, false)
			runtime := owner.snapshot()[id]
			assert.Equal(t, len(owner.snapshot()), 2)
			owner.freeze()
			if session != nil {
				again, freezeErr := session.Freeze(t.Context())
				assert.NoError(t, freezeErr)
				assert.Equal(t, again, result)
				assert.Equal(t, session.finishAdmission(t.Context()), a)
				assert.Equal(t, session.resolvedSources, owner)
				assert.Equal(t, session.entryState.session == nil, true)
			}
			assert.Equal(t, a.session == nil, true)
			assert.Equal(t, len(owner.snapshot()), 2)
			assert.Equal(t, owner.snapshot()[id].object == runtime.object, true)
			assert.Equal(t, owner.snapshot()[id].artifact == runtime.artifact, true)

			object := &manifest.GitRepository{Name: "metadata", Namespace: "sources", URL: "file://" + root}
			st.AddObject(object)
			complete := entryResolution{
				object:   object,
				input:    a.input,
				git:      object,
				pinned:   true,
				artifact: &store.SourceArtifact{FS: a.input.FS, LocalPath: root, Revision: "pinned"},
				warnings: []manifest.Warning{
					{Category: manifest.WarnSourceRefUnavailable, Message: "retained metadata", Count: 1},
				},
				viewKey: "pinned-view",
			}
			owner.publish(&complete, false)
			assertResolution(t, owner.snapshot()[object.Named()], complete)
			assert.Equal(t, st.GetObject(object.Named()) == complete.object, true)
			assert.Equal(t, st.GetArtifact(object.Named(), store.ArtifactContent) == complete.artifact, true)
		})
	}
	t.Run("construction refresh and republication", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "apps/file", "working tree")
		st := store.New()
		d := Session{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
		t.Cleanup(d.loader.Close)
		t.Cleanup(d.Close)
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.NoError(t, err)
		a := d.admission()
		id := manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "alias",
		}
		published, err := a.Publish(
			t.Context(),
			id,
			manifest.WorkScope{},
			func(store.Child) (bool, error) { return true, nil },
		)
		assert.NoError(t, err)
		assert.Equal(t, published, true)
		before := d.resolvedSources.snapshot()[id]
		assert.Equal(t, st.DeleteObject(id), true)
		st.AddObject(before.object)
		_, err = d.resolveEntrySources(t.Context(), false)
		assert.NoError(t, err)
		assertResolution(t, d.resolvedSources.snapshot()[id], before)
		assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == before.artifact, true)
		assert.Equal(t, d.aliasViews[before.viewKey] == before.artifact, true)
		next := *d.cfg.Input
		next.Revision = "refreshed"
		st.SetArtifact(manifest.BootstrapSourceID, &next)
		_, err = d.resolveEntrySources(t.Context(), false)
		assert.NoError(t, err)
		after := d.resolvedSources.snapshot()[id]
		assert.Equal(t, after.input, &next)
		assert.Equal(t, after.object == before.object, true)
		assert.Equal(t, after.git == before.git, true)
		assert.Equal(t, after.synthetic, before.synthetic)
		assert.Equal(t, after.pinned, before.pinned)
		assert.Diff(t, after.warnings, before.warnings)
		assert.Equal(t, after.viewKey, before.viewKey)
		assert.Equal(t, after.artifact.Revision, "refreshed")
		assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == after.artifact, true)
		assert.Equal(t, len(d.aliasViews), 0)
		assert.Equal(t, d.hasPins, false)
	})
}

func TestAdmission_PublishConcurrent(t *testing.T) {
	t.Parallel()
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate=%t", duplicate), func(t *testing.T) {
			t.Parallel()
			testAdmissionPublishConcurrent(t, duplicate)
		})
	}
}

func testAdmissionPublishConcurrent(t *testing.T, duplicate bool) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/file", "working tree")
	st := store.New()
	result, err := Run(t.Context(), Config{Path: root, Store: st, Trees: testTrees(t)})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	a := result.Admission
	seed := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "seed",
	}
	published, err := a.Publish(t.Context(), seed, manifest.WorkScope{}, func(store.Child) (bool, error) {
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, published, true)
	before := a.resolvedSources.snapshot()
	captured := before[seed]
	ids := [2]manifest.NamedResource{seed, seed}
	ids[0].Name, ids[1].Name = "alpha", "zeta"
	if duplicate {
		ids[1] = ids[0]
	}
	var expected [2]entryResolution
	for i, id := range ids {
		expected[i], err = a.resolve(t.Context(), id, nil, manifest.WorkScope{})
		assert.NoError(t, err)
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var workers sync.WaitGroup
	var accepted [2]bool
	var failures [2]error
	var offered [2]manifest.BaseManifest
	for i, id := range ids {
		workers.Go(func() {
			accepted[i], failures[i] = a.Publish(
				t.Context(),
				id,
				manifest.WorkScope{},
				func(child store.Child) (bool, error) {
					offered[i] = child.Obj
					ready <- struct{}{}
					<-release
					return true, nil
				},
			)
		})
	}
	<-ready
	<-ready
	close(release)
	for id, resolution := range before {
		assert.Equal(t, resolution.object.Named(), id)
		assert.Equal(t, resolution.artifact.FS != nil, true)
	}
	workers.Wait()
	assert.Equal(t, len(before), 2)
	assert.Equal(t, before[seed].object == captured.object, true)
	assert.Equal(t, before[seed].artifact == captured.artifact, true)
	wantRecords := 4
	if duplicate {
		wantRecords = 3
	}
	assert.Equal(t, len(a.resolvedSources.snapshot()), wantRecords)
	for i, id := range ids {
		assert.NoError(t, failures[i])
		assert.Equal(t, accepted[i], true)
		got, ok := a.resolvedSources.snapshot()[id]
		assert.Equal(t, ok, true)
		assert.Equal(t, manifest.Equal(got.object, offered[i]), true)
		assert.Equal(t, manifest.Equal(got.object, expected[i].object), true)
		assert.Equal(t, manifest.Equal(got.git, expected[i].git), true)
		assert.Equal(t, got.input, a.input)
		assert.Equal(t, got.synthetic, expected[i].synthetic)
		assert.Equal(t, got.pinned, expected[i].pinned)
		assert.Equal(t, got.viewKey, expected[i].viewKey)
		assert.Diff(t, got.warnings, expected[i].warnings)
		artifact := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		for _, value := range []*store.SourceArtifact{got.artifact, artifact} {
			assert.Diff(t, value.Input(store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin),
				expected[i].artifact.Input(store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin))
			assert.Equal(t, value.URL, expected[i].artifact.URL)
			assert.Equal(t, value.Size, expected[i].artifact.Size)
			assert.Diff(t, value.Metadata, expected[i].artifact.Metadata)
			assert.Diff(t, value.Warnings, expected[i].artifact.Warnings)
			assert.Equal(t, value.Reads, expected[i].artifact.Reads)
			body, readErr := fs.ReadFile(value.FS, "apps/file")
			assert.NoError(t, readErr)
			assert.Equal(t, string(body), "working tree")
			fingerprint, fingerprintErr := value.ContentFingerprint(t.Context(), value.FS)
			assert.NoError(t, fingerprintErr)
			wantFingerprint, fingerprintErr := expected[i].artifact.ContentFingerprint(
				t.Context(),
				expected[i].artifact.FS,
			)
			assert.NoError(t, fingerprintErr)
			assert.Equal(t, fingerprint, wantFingerprint)
		}
		assert.Equal(t, manifest.Equal(st.GetObject(id), got.object), true)
		status, ok := st.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(
			t,
			status,
			store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSucceeded, Message: "local entry source"},
		)
	}
	names := []string{seed.String(), ids[0].String()}
	if !duplicate {
		names = append(names, ids[1].String())
	}
	slices.Sort(names)
	assert.Diff(t, a.Warnings(), []manifest.Warning{
		{
			Category: manifest.WarnBootstrapAliases,
			Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
			Detail:   append(names, root),
			Count:    1,
		},
	})
	assert.Equal(t, a.session == nil, true)
	assert.Equal(t, a.qualification.session == nil, true)
}

func TestAdmission_Warnings(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	url := "https://example.test/cluster"
	first := &manifest.GitRepository{Name: "alpha", Namespace: "sources", URL: url}
	second := &manifest.GitRepository{Name: "zeta", Namespace: "sources", URL: url}
	st.AddObjects([]manifest.BaseManifest{first, second})
	result, err := Run(t.Context(), Config{Path: root, Store: st, SelfURLs: []string{url}})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	a := result.Admission
	initial := []manifest.Warning{
		{
			Category: manifest.WarnBootstrapAliases,
			Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
			Detail:   []string{first.Named().String(), second.Named().String(), root},
			Count:    1,
		},
	}
	assert.Diff(t, st.Warnings(), initial)
	canonical := *a.input
	canonical.InputAlias = true
	st.SetArtifact(manifest.BootstrapSourceID, &canonical)
	committed := &manifest.GitRepository{Name: "committed", Namespace: "sources", URL: url}
	a.resolvedSources.publish(&entryResolution{
		object: committed, input: a.input, pinned: true,
		artifact: &store.SourceArtifact{FS: a.input.FS, LocalPath: root, Revision: "committed"},
	}, true)
	for _, warning := range []manifest.Warning{
		{
			Category: manifest.WarnStaleValues, Resource: first.Named(), Related: []manifest.NamedResource{second.Named()},
			Subject: manifest.DiagnosticFor(first.Named(), first), Message: "ordinary", Detail: []string{"kept", "in order"},
		},
		{Category: manifest.WarnBaselineStatusUnavailable, Message: "ordinary before aliases", Detail: []string{"retained"}},
	} {
		st.AddWarning(warning)
	}
	ordinary := slices.DeleteFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Category == manifest.WarnBootstrapAliases
	})
	late := first.Named()
	late.Name = "middle"
	published, err := a.Publish(
		t.Context(),
		late,
		manifest.WorkScope{},
		func(store.Child) (bool, error) { return true, nil },
	)
	assert.NoError(t, err)
	assert.Equal(t, published, true)
	want := append(slices.Clone(ordinary), manifest.Warning{
		Category: manifest.WarnBootstrapAliases,
		Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
		Detail:   []string{first.Named().String(), late.String(), second.Named().String(), root},
		Count:    1,
	})
	slices.SortFunc(want, manifest.CompareWarning)
	stored := st.Warnings()
	assert.Diff(t, a.Warnings(), want)
	assert.Diff(t, a.Warnings(), want)
	assert.Diff(t, st.Warnings(), stored)
	assert.Equal(t, st.DeleteObject(late), true)
	assert.Equal(t, st.DeleteObject(second.Named()), true)
	assert.Diff(t, a.Warnings(), ordinary)
	assert.Equal(t, st.DeleteObject(first.Named()), true)
	assert.Diff(t, a.Warnings(), ordinary)
}

func testDiscardPinnedWorkingTreeFilesPreservesSourcesRuntimeAlias(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "nested/dirty.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty}\n")
	st := store.New()
	d := Session{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
	t.Cleanup(d.loader.Close)
	t.Cleanup(d.Close)
	_, err := d.resolveEntrySources(t.Context(), false)
	assert.NoError(t, err)
	a := d.finishAdmission(t.Context())
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "late",
	}
	published, err := a.Publish(
		t.Context(),
		id,
		manifest.WorkScope{},
		func(store.Child) (bool, error) { return true, nil },
	)
	assert.NoError(t, err)
	assert.Equal(t, published, true)
	alias := a.resolvedSources.snapshot()[id]
	pinned := &manifest.GitRepository{Name: "pinned", Namespace: "apps"}
	st.AddObject(pinned)
	st.SetArtifact(pinned.Named(), &store.SourceArtifact{
		LocalPath: t.TempDir(), LocalRoot: root, Key: "pinned", Content: "pinned", FS: fstest.MapFS{},
	})
	d.loader.Admit(root)
	for i, subpath := range []string{"./", "./nested"} {
		ks := &manifest.Kustomization{
			Name: fmt.Sprintf("claim-%d", i), Namespace: "apps", Path: subpath,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: pinned.Name},
		}
		st.AddObject(ks)
		d.loader.Index.Records[ks.Named()] = manifest.DiscoveryRecord{
			File: ks.Name + ".yaml",
			Path: filepath.Join(root, ks.Name+".yaml"),
			Refs: []manifest.NamedResource{pinned.Named()},
		}
	}
	d.loader.SourceRoot = root
	d.loader.Index.Records[id] = manifest.DiscoveryRecord{
		File: "nested/source.yaml",
		Path: filepath.Join(root, "nested", "source.yaml"),
	}
	dirty := &manifest.ConfigMap{Name: "dirty"}
	st.AddObject(dirty)
	file := filepath.Join(alias.artifact.LocalPath, "nested", "dirty.yaml")
	relative, err := filepath.Rel(root, file)
	assert.NoError(t, err)
	d.loader.Index.Records[dirty.Named()] = manifest.DiscoveryRecord{File: filepath.ToSlash(relative), Path: file}
	d.loader.Admit(root)
	claims := d.pinnedWorkingTreeClaims(root, d.loader.PathObjects())
	assert.FatalIf(t, len(claims.claims) == 0, "overlapping pinned claims are absent: %+v", d.loader.PathObjects())
	assert.Equal(
		t,
		slices.Contains(claims.claims, manifest.KSClaim{ID: testutil.KustomizationID("apps", "claim-0"), Prefix: ""}),
		true,
	)
	assert.Equal(
		t,
		slices.Contains(
			claims.claims,
			manifest.KSClaim{ID: testutil.KustomizationID("apps", "claim-1"), Prefix: "nested/"},
		),
		true,
	)
	d.discardPinnedWorkingTreeFiles(root)
	assert.Equal(t, st.GetObject(id) == alias.object, true)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == alias.artifact, true)
	status, ok := st.GetStatus(id)
	assert.Equal(t, ok, true)
	assert.Equal(t, status.Status, store.StatusReady)
	_, ok = d.loader.Index.Lookup(id)
	assert.Equal(t, ok, true)
	assert.Equal(t, st.GetObject(dirty.Named()) == nil, true)
	_, ok = d.loader.Index.Lookup(dirty.Named())
	assert.Equal(t, ok, false)
}
