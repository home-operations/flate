package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_OrderingEmittedPathProof(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                       string
		foreign, incompatible, independent, visited, completeEmpty bool
		want                                                       bool
	}{
		{name: "compatible_namespace"},
		{name: "foreign_source", foreign: true},
		{name: "incompatible_namespace", incompatible: true},
		{name: "independent_parent", independent: true},
		{name: "visited_path", visited: true},
		{name: "complete_empty", completeEmpty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmissionOrderingEmittedPathProofCase(t, &tc)
		},
		)
	}
}

func TestAdmission_OrderingEmittedTemplateProof(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"emitted", "known_target", "foreign", "unowned", "complete_empty"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testAdmissionOrderingEmittedTemplateProofCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testAdmissionOrderingEmittedPathProofCase(
	t *testing.T,
	testCaseInput *struct {
		name          string
		foreign       bool
		incompatible  bool
		independent   bool
		visited       bool
		completeEmpty bool
		want          bool
	},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./consumer, dependsOn: [{name: target, namespace: infra}]}\n",
			),
		},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "cluster", Namespace: "sources"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	l := New(st)
	l.SourceRoot = root
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := &manifest.Kustomization{Name: "infra-parent", Namespace: "flux-system"}
	parent.Path, parent.TargetNamespace = "unvisited", "infra"
	parent.SourceRef.Kind, parent.SourceRef.Name, parent.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
	if testCaseInput.foreign {
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	}
	if testCaseInput.incompatible {
		parent.TargetNamespace = "other"
	}
	if testCaseInput.visited {
		l.documents.cache[filepath.Join(root, parent.Path)] = document{traversed: true}
	}
	producer := &manifest.ResourceSet{Name: "namespaces", Namespace: "flux-system"}
	st.AddObject(producer)
	var reads store.ReadSet
	reads.Init(st, producer.Named(), nil)
	defer reads.Close()
	if testCaseInput.independent {
		st.AddObject(parent)
	} else {
		_, _, next := reads.GetEmission(producer.Named())
		defer next.Finish()
		next.Children, next.Complete = []store.Child{{Obj: parent}}, true
		reads.Bind(&next)
		st.RecordEmitted(producer.Named(), &next, nil)
		assert.Equal(t, st.GetObject(parent.Named()), manifest.BaseManifest(parent))
	}
	if testCaseInput.completeEmpty {
		var emptyReads store.ReadSet
		emptyReads.Init(st, parent.Named(), nil)
		defer emptyReads.Close()
		_, _, empty := emptyReads.GetEmission(parent.Named())
		defer empty.Finish()
		empty.Complete = true
		emptyReads.Bind(&empty)
		st.RecordEmitted(parent.Named(), &empty, nil)
		st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, parent.Named()), testCaseInput.want)
	assert.Equal(t, slices.Contains(proposal.Selected, parent.Named()), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, parent.Named()), false)
}

func testAdmissionOrderingEmittedTemplateProofCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec: {path: ./consumer, dependsOn: [{name: target}]}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	consumer := (&manifest.Kustomization{Name: "consumer", Namespace: "apps"}).Named()
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	producer := &manifest.ResourceSet{Name: "templates", Namespace: "apps"}
	if (*testCaseInput) == "known_target" {
		producer.ResourcesTemplate = "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: target, namespace: apps}\nspec: {path: ./target}\n"
	}
	st.AddObject(parent)
	source := manifest.BootstrapSourceID
	if (*testCaseInput) == "foreign" {
		source.Name = "foreign"
		st.SetArtifact(source, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	}
	if (*testCaseInput) == "unowned" {
		st.AddObject(producer)
	} else {
		var reads store.ReadSet
		reads.Init(st, parent.Named(), nil)
		defer reads.Retire()
		_, _, output := reads.GetEmission(parent.Named())
		output.Children = []store.Child{{Obj: producer, Productions: []manifest.Production{{Source: source}}}}
		output.Complete = true
		reads.Bind(&output)
		st.RecordEmitted(parent.Named(), &output, nil)
		output.Finish()
	}
	if (*testCaseInput) == "complete_empty" {
		var reads store.ReadSet
		reads.Init(st, producer.Named(), nil)
		defer reads.Retire()
		_, _, output := reads.GetEmission(producer.Named())
		output.Complete = true
		reads.Bind(&output)
		st.RecordEmitted(producer.Named(), &output, nil)
		output.Finish()
		st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, producer.Named()), (*testCaseInput) == "known_target")
	assert.Equal(t, slices.Contains(proposal.Selected, producer.Named()), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer.Named()), false)
}
