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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryQualification_ReceiptInputs(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"identical", "ancestor chart changed", "acquisition failure", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			app := testutil.KustomizationYAML("applier", "apps", "  path: ./entry\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
			documents, err := manifest.DecodeDocs(strings.NewReader(app), manifest.RequiredDocuments)
			assert.NoError(t, err)
			evidence := kustomize.EntryEvidence{Application: documents[0].Document, Receipt: manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b })}
			q := &entryQualification{trees: testTrees(t), visited: map[manifest.NamedResource]bool{}}
			for _, name := range []string{"ancestor-chart", "nested-chart"} {
				id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: name}
				artifact := &store.SourceArtifact{Kind: id.Kind, FS: fstest.MapFS{}, Key: "same-family", Revision: "same-tag", Content: manifest.Fingerprint(name)}
				binding := artifact.Input(store.ArtifactContent | store.ArtifactRevision)
				evidence.Inputs = append(evidence.Inputs, kustomize.EntryInput{Resource: id, Content: artifact.Content, Binding: binding})
				q.refresh = append(q.refresh, id)
			}
			q.previous = evidence
			a := &Admission{input: input, path: filepath.Join(root, "entry"), qualification: q}
			pending, _, err := a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			assert.Equal(t, pending == nil, true)
			assert.Equal(t, a.applications.Load() == nil, true)
			assert.Diff(t, a.EntryProducers(), q.refresh)
			for i, expected := range evidence.Inputs {
				content := expected.Content
				if scenario == "ancestor chart changed" && i == 0 {
					content = manifest.Fingerprint("new chart behind same tag")
				}
				st.SetArtifact(expected.Resource, &store.SourceArtifact{Kind: expected.Resource.Kind, FS: fstest.MapFS{}, Key: "same-family", Revision: "same-tag", Content: content})
				st.UpdateStatus(expected.Resource, store.StatusInfo{Status: store.StatusReady})
			}
			if scenario == "acquisition failure" {
				st.SetArtifact(q.refresh[0], nil)
				st.UpdateFailure(q.refresh[0], manifest.NewError(manifest.ErrInput, errors.New("acquisition failed"), ""))
			}
			ctx := t.Context()
			if scenario == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			bound, _, err := a.BindEntry(ctx, st)
			assert.Equal(t, errors.Is(err, context.Canceled), scenario == "canceled")
			assert.Equal(t, bound != nil, scenario == "identical")
			assert.Equal(t, a.applications.Load() == nil, true)
		})
	}
}
