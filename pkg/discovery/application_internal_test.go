package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryQualification_ReceiptInputs(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"identical", "ancestor chart changed", "acquisition failure", "canceled", "supplied unchanged", "supplied changed", "supplied removed", "repository precedence"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testEntryQualificationReceiptInputsCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func testEntryQualificationReceiptInputsCase(t *testing.T, testCaseInput *string) {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/child.yaml": {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"))},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	app := testutil.KustomizationYAML(
		"applier",
		"apps",
		"  path: ./entry\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
	)
	documents, err := manifest.DecodeDocs(strings.NewReader(app), manifest.RequiredDocuments)
	assert.NoError(t, err)
	evidence := kustomize.EntryEvidence{
		Application: documents[0].Document,
		Receipt:     manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b }),
	}
	result, err := Run(t.Context(), Config{
		Input: input, Path: filepath.Join(root, "entry"), Store: st, Trees: testTrees(t),
		Selection: manifest.Selection{Changed: true},
	})
	assert.NoError(t, err)
	a := result.Admission
	q := a.qualification
	if q == nil {
		t.Fatal("unresolved entry has no receipt qualification")
	}
	for _, name := range []string{"ancestor-chart", "nested-chart"} {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "apps",
			Name:      name,
		}
		artifact := &store.SourceArtifact{
			Kind:     id.Kind,
			FS:       fstest.MapFS{},
			Key:      "same-family",
			Revision: "same-tag",
			Content:  manifest.Fingerprint(name),
		}
		binding := artifact.Input(store.ArtifactContent | store.ArtifactRevision)
		evidence.Inputs = append(
			evidence.Inputs,
			kustomize.EntryInput{Resource: id, Content: artifact.Content, Binding: binding},
		)
		q.refresh = append(q.refresh, id)
	}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "settings"}
	data := manifest.NewSecret(secret, nil, map[string]any{"VALUE": "original"})
	switch *testCaseInput {
	case "supplied unchanged", "supplied changed", "supplied removed", "repository precedence":
		evidence.Inputs = append(evidence.Inputs,
			kustomize.EntryInput{
				Resource: secret,
				Content:  manifest.Fingerprint(data.Snapshot()),
			})
		if (*testCaseInput) != "supplied removed" {
			value := "original"
			if (*testCaseInput) == "supplied changed" || (*testCaseInput) == "repository precedence" {
				value = "changed"
			}
			st.AddDeclaration(store.Declaration{
				Obj: manifest.NewSecret(secret,
					nil,
					map[string]any{"VALUE": value}),
				Origin: manifest.SuppliedDeclaration,
			})
		}
		if (*testCaseInput) == "repository precedence" {
			st.AddObject(data, testutil.ObjectDocument(data))
		}
	}
	q.previous = evidence
	pending, _, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, pending == nil, true)
	assert.Equal(t, a.applications.Load() == nil, true)
	assert.Diff(t, a.EntryInputs(), q.refresh)
	for i := range evidence.Inputs {
		expected := &evidence.Inputs[i]
		if !expected.Binding.Present {
			continue
		}
		content := expected.Content
		if (*testCaseInput) == "ancestor chart changed" && i == 0 {
			content = manifest.Fingerprint("new chart behind same tag")
		}
		st.SetArtifact(
			expected.Resource,
			&store.SourceArtifact{
				Kind:     expected.Resource.Kind,
				FS:       fstest.MapFS{},
				Key:      "same-family",
				Revision: "same-tag",
				Content:  content,
			},
		)
		st.UpdateStatus(expected.Resource, store.StatusInfo{Status: store.StatusReady})
	}
	if (*testCaseInput) == "acquisition failure" {
		st.SetArtifact(q.refresh[0], nil)
		st.UpdateFailure(q.refresh[0], manifest.NewError(manifest.ErrInput, errors.New("acquisition failed"), ""))
	}
	ctx := t.Context()
	if (*testCaseInput) == "canceled" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	bound, _, err := a.BindEntry(ctx, st)
	assert.Equal(t, errors.Is(err, context.Canceled), (*testCaseInput) == "canceled")
	assert.Equal(
		t,
		bound != nil,
		(*testCaseInput) == "identical" || (*testCaseInput) == "supplied unchanged" ||
			(*testCaseInput) == "repository precedence",
	)
	assert.Equal(t, a.applications.Load() == nil, true)
}

func TestRun_EntryQualificationLifetime(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"bound", "unresolved changed-only", "nil trees"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"entry/child.yaml": {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"))},
			}
			if scenario == "bound" {
				input["entry/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [child.yaml]\n")}
			}
			st := store.New()
			l := loader.New(st)
			l.Options.HistoryOwners = []manifest.NamedResource{testutil.KustomizationID("apps", "child")}
			cfg := Config{
				Store:  st,
				Loader: l,
				Input:  &store.SourceArtifact{LocalPath: root, FS: input},
				Path:   filepath.Join(root, "entry"),
				Trees:  testTrees(t),
			}
			if scenario == "unresolved changed-only" {
				cfg.Selection = manifest.Selection{Changed: true}
			}
			if scenario == "nil trees" {
				cfg.Trees = nil
			}
			session, err := Begin(t.Context(), cfg)
			assert.NoError(t, err)
			result, err := session.Freeze(t.Context())
			assert.NoError(t, err)
			a := result.Admission
			assert.Equal(t, a.entryBound.Load(), scenario == "bound")
			assert.Equal(t, a.qualification != nil, scenario == "unresolved changed-only")
			assert.Equal(t, len(l.Options.HistoryOwners) == 0, scenario != "unresolved changed-only")
			if scenario != "unresolved changed-only" {
				assert.Equal(t, session.entryState.loader == nil, true)
				return
			}
			q := a.qualification
			assert.Equal(t, q, &session.entryState)
			assert.Equal(t, q.loader, l)
			applier := testutil.KustomizationYAML(
				"applier",
				"apps",
				"  path: ./entry\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			)
			documents, err := manifest.DecodeDocs(strings.NewReader(applier), manifest.RequiredDocuments)
			assert.NoError(t, err)
			parsed, err := manifest.ParseDoc(documents[0].Document, manifest.ParseDocOptions{})
			object := parsed.Object
			assert.NoError(t, err)
			st.AddObject(object)
			a.AddApplication(st, object)
			bound, binding, err := a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			assert.Equal(t, bound != nil, true)
			assert.Equal(t, binding.Producer, object.Named())
			assert.Equal(t, a.entryBound.Load(), true)
			assert.Equal(t, a.qualification == nil, true)
			assert.Equal(t, q.loader == nil, true)
		})
	}
}
