package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoad_ReadIntentReuse(t *testing.T) {
	t.Parallel()
	for _, stage := range []selection.CommitStage{selection.Construction, selection.Execution} {
		for _, physicalFirst := range []bool{false, true} {
			name := "application first"
			if physicalFirst {
				name = "physical first"
			}
			t.Run(fmt.Sprintf("%d/%s", stage, name), func(t *testing.T) {
				t.Parallel()
				testLoadReadIntentReuse(t, stage, physicalFirst)
			})
		}
	}
}

func testLoadReadIntentReuse(t *testing.T, stage selection.CommitStage, physicalFirst bool) {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [objects.yaml, negative.yaml, empty.yaml]\n")},
		"objects.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"entry",
					"apps",
					"  path: ./entry\n",
				) + "---\n" + testutil.KustomizationYAML(
					"supplier",
					"apps",
					"  postBuild: {substitute: {PREFIX: prod-}}\n",
				) + `---
apiVersion: v1
kind: List
items:
  - apiVersion: v1
    kind: List
    items:
      - apiVersion: v1
        kind: ConfigMap
        metadata: {name: data}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: projected, namespace: apps}
    spec: {path: ./entry}
`,
			),
		},
		"negative.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: negative}\n")},
		"empty.yaml":    {Data: []byte("# empty\n")},
	}
	reads := map[string]*atomic.Int64{}
	for file := range files {
		reads[file] = &atomic.Int64{}
	}
	var directories atomic.Int64
	input := &store.SourceArtifact{
		LocalPath: root,
		FS:        parsedReadFS{FS: files, reads: reads, directories: &directories},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})

	owner := testutil.KustomizationID("apps", "owner")
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	commit := func(stage selection.CommitStage) {
		before := l.Options.Session.Snapshot()
		var reads store.ReadSet
		reads.Init(st, manifest.NamedResource{}, nil)
		defer reads.Retire()
		reads.RequireData(owner)
		proposal, err := prepareSelection(
			t.Context(),
			t,
			l,
			selection.PrepareRequest{Changes: l.Options.Changes, Reads: &reads},
		)
		assert.NoError(t, err)
		_, committed, err := l.commitSelection(t.Context(), t, proposal, stage)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		if stage == selection.Construction {
			assert.Equal(t, len(proposalSelected(l.Options.Session.Snapshot(), l.Index)), 0)
			assert.Equal(t, l.Options.Session.Snapshot().Output(owner), false)
			assert.Equal(t, l.Options.Session.Snapshot().Changed(owner), false)
			assert.Equal(t, before.OrdinaryDemand(owner), false)
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(owner), true)
			assert.Equal(t, l.Options.Session.Snapshot().OrdinaryDemand(owner), true)
		} else {
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(owner), true)
		}
	}
	load := func() { _, err := l.Load(t.Context(), root, owner, nil); assert.NoError(t, err) }
	if physicalFirst {
		commit(stage)
		load()
	}
	if !physicalFirst {
		load()
	}
	count := reads["objects.yaml"].Load()
	listings := directories.Load()
	possible := slices.ContainsFunc(l.documents.cache[filepath.Join(root, "objects.yaml")].objects,
		func(child parsedObject) bool { return parsedApplication(&child) })
	assert.Equal(t, possible, true)
	load()
	assert.Equal(t, reads["objects.yaml"].Load(), count)
	assert.Equal(t, directories.Load(), listings)
	if !physicalFirst {
		cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
		assert.Equal(t, cached.coverage, manifest.ApplicationDocuments)
		assert.Equal(t, len(cached.objects), 1)
		assert.Equal(t, cached.objects[0].fact.facts.declared, "")
		assert.Equal(t, len(cached.objects[0].fact.facts.targets), 0)
		assert.Equal(t, len(l.inputVisibility()), 0)
		assert.Equal(t, l.documents.cache[filepath.Join(root, "negative.yaml")].objects != nil, true)
		assert.Equal(t, l.documents.cache[filepath.Join(root, "empty.yaml")].objects != nil, true)
		commit(stage)
		load()
		assert.Equal(t, reads["objects.yaml"].Load(), count+1)
	}
	assert.Equal(t, directories.Load(), listings)
	cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
	assert.Equal(t, cached.coverage, manifest.IncidentalDocuments)
	assert.Equal(t, len(cached.objects), 4)
	supplier := l.CatalogObject(testutil.KustomizationID("apps", "supplier")).(*manifest.Kustomization)
	assert.Equal(t, supplier.PostBuild.Substitute["PREFIX"], "prod-")
	count = reads["objects.yaml"].Load()
	load()
	assert.Equal(t, reads["objects.yaml"].Load(), count)
	assert.Equal(t, directories.Load(), listings)
	commit(selection.Execution)
	load()
	assert.Equal(t, reads["objects.yaml"].Load(), count)
	assert.Equal(t, directories.Load(), listings)
}

func TestDecodeFileObjects_ApplicationPrecondition(t *testing.T) {
	t.Parallel()
	for _, api := range []string{
		"kustomize.toolkit.fluxcd.io/v1",
		"kustomize.toolkit.fluxcd.io/v1beta1",
		"kustomize.toolkit.fluxcd.io/v1beta2",
		`kustomize.toolkit.fluxcd.io/v\x31`,
		"fluxcd.controlplane.io/v1",
		`fluxcd.controlplane.io/v\x31`,
	} {
		t.Run(api, func(t *testing.T) {
			t.Parallel()
			kind, spec := "Kustomization", "path: ./entry"
			if strings.HasPrefix(api, manifest.FluxOperatorDomain) {
				kind, spec = "FluxInstance", "sync: {kind: GitRepository, url: https://fixture.invalid/repo, ref: refs/heads/main, path: ./entry}"
			}
			root := t.TempDir()
			// Authored escapes MUST reach the YAML decoder unchanged.
			document := "apiVersion: \"" + api + "\"\nkind: " + kind + "\nmetadata: {name: app, namespace: apps}\nspec: {" + spec + "}\n"
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
				LocalPath: root, FS: fstest.MapFS{"application.yaml": {Data: []byte(document)}},
			})
			l := New(st)
			l.SourceRoot = root
			l.mountSources()
			reader := l.documents
			reader.intent = manifest.ApplicationDocuments
			var objects []parsedObject
			err := reader.decodeFileObjects(
				filepath.Join(root, "application.yaml"),
				manifest.ParseDocOptions{},
				true,
				func(object parsedObject) {
					objects = append(objects, object)
				},
			)
			assert.NoError(t, err)
			assert.FatalIf(t, len(objects) != 1, "application objects = %d, want 1", len(objects))
			assert.NoError(t, objects[0].outcome.Err)
			application, present := manifest.ApplicationFor(objects[0].Obj)
			assert.Equal(t, present, true)
			assert.Equal(t, application.Path, "./entry")
		})
	}
}
