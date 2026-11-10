package helm

import (
	"bytes"
	"io/fs"
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepareTemplate_FinalContentSnapshots(t *testing.T) {
	t.Parallel()
	for _, regime := range []string{"source observation", "immutable source", "transferred preparation"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
			})
			client := NewClient(cachetest.New(t, "", cache.New))
			hr := newHR()
			var preparation, successor Preparation
			defer preparation.Retire()
			defer successor.Retire()
			input := RenderOptions{
				Preparation: &preparation,
				Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
			}
			if regime == "immutable source" {
				artifact := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
				artifact.Content = "immutable-source"
				sources := store.New()
				sources.SetArtifact(hr.ChartSource(), &artifact)
				input.Sources = NewSources(sources)
			}
			first, err := client.prepareTemplate(t.Context(), hr, nil, &input)
			assert.NoError(t, err)
			current := input
			omitted := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: hr.Namespace, Name: "missing"}
			current.Omitted = []manifest.NamedResource{omitted, omitted}
			if regime == "transferred preparation" {
				assert.Equal(t, successor.Take(&preparation, client, hr, &current), true)
				current.Preparation = &successor
			}
			next, err := client.prepareTemplate(t.Context(), hr, nil, &current)
			assert.NoError(t, err)
			assert.Equal(t, next.key, first.key)
			assert.Equal(t, next.binding.same(&first.binding), true)
			assert.Equal(t, manifest.ValueEqual(next.content, first.content), false)
			current.Omitted[0].Name = "mutated"
			assert.Diff(t, next.input.Omitted, []manifest.NamedResource{omitted})
			payload, err := testutil.EncodeRender(
				[]map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "raw"}}},
			)
			assert.NoError(t, err)
			header := renderHeader{
				Key:      first.key,
				Binding:  manifest.Fingerprint(first.binding),
				Analysis: &chartAnalysis{Version: "0.1.0"},
			}
			frame, err := cache.NewFrame(frameDomain, header, payload)
			assert.NoError(t, err)
			publication, err := frame.PreparePublication(t.Context(), cache.FramePublicationInput{
				Owner: hr.Named(), Content: first.content, Records: emptyRenderInventory, Complete: true,
			})
			assert.NoError(t, err)
			previous := renderFrame{
				header:  header,
				output:  payload,
				content: first.content,
				binding: first.binding,
				stored:  publication.Frame(),
			}
			receipt := manifest.NewRenderReceipt(previous, equalRenderFrame)
			record, documents, err := next.validateRecord(t.Context(), client, &renderRecord{
				key:      first.key,
				Output:   payload,
				Analysis: header.Analysis,
				binding:  first.binding,
				frame:    previous,
				receipt:  receipt,
			})
			assert.NoError(t, err)
			assert.Equal(t, record.receipt.Equal(receipt), false)
			assert.Equal(t, manifest.ValueEqual(record.frame.content, next.content), true)
			_, accepted := record.frame.stored.History(hr.Named(), next.content)
			assert.Equal(t, accepted, false)
			_, accepted = previous.stored.History(hr.Named(), first.content)
			assert.Equal(t, accepted, true)
			var observed RenderAdmission
			next.input.Admit = func(admission RenderAdmission) (bool, error) {
				observed = admission
				return false, nil
			}
			result := templateResult{record: record, docs: documents, receipt: record.receipt}
			_, err = result.admit(t.Context(), client, &next)
			assert.NoError(t, err)
			assert.Equal(t, observed.Accepted, false)
			updated, err := record.frame.stored.PreparePublication(t.Context(), cache.FramePublicationInput{
				Owner: hr.Named(), Content: record.frame.content, Records: emptyRenderInventory, Complete: true,
			})
			assert.NoError(t, err)
			_, accepted = updated.Frame().History(hr.Named(), next.content)
			assert.Equal(t, accepted, true)
			_, accepted = updated.Frame().History(hr.Named(), first.content)
			assert.Equal(t, accepted, false)
		})
	}
}

func TestTemplate_FinalContentRebinding(t *testing.T) {
	t.Parallel()
	for _, regime := range []string{"memory", "disk", "transferred preparation"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			if !renderPersistenceEligible(t) {
				t.Skip("requires a clean VCS-stamped build for positive persistence")
			}
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
				"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
			})
			cacheDir := t.TempDir()
			client := NewClient(cachetest.New(t, cacheDir, cache.New))
			hr := newHR()
			var preparation, successor Preparation
			defer preparation.Retire()
			defer successor.Retire()
			input := RenderOptions{
				Preparation: &preparation,
				Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
			}
			initial, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
			assert.NoError(t, err)
			assert.NoError(t, client.RecordRender(t.Context(), RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
				Sources: []manifest.SourceEvidence{},
			}))
			_, complete := client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, true)
			previous := receipt.Value().(renderFrame)
			current := input
			a := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: hr.Namespace, Name: "a"}
			b := a
			b.Name = "b"
			current.Omitted = []manifest.NamedResource{b, a, b}
			if regime == "disk" {
				assert.NoError(t, client.cache.Close())
				client = NewClient(cachetest.New(t, cacheDir, cache.New))
				current.Preparation = nil
			}
			if regime == "transferred preparation" {
				assert.Equal(t, successor.Take(&preparation, client, hr.Clone(), &current), true)
				current.Preparation = &successor
			}
			request, err := client.prepareTemplate(t.Context(), hr.Clone(), nil, &current)
			assert.NoError(t, err)
			assert.Equal(t, request.key, previous.header.Key)
			assert.Equal(t, request.binding.same(&previous.binding), true)
			assert.Diff(t, request.input.Omitted, []manifest.NamedResource{a, b})
			current.Omitted[0].Name = "mutated"
			assert.Diff(t, request.input.Omitted, []manifest.NamedResource{a, b})
			current.Omitted = []manifest.NamedResource{a, b}
			admission, record := request.contentRender(t.Context(), client)
			assert.Equal(t, record != nil, true)
			assert.Equal(t, admission.Accepted, false)
			assert.Equal(t, admission.Receipt.Equal(receipt), false)
			assert.Equal(t, bytes.Equal(record.Output, previous.output), true)
			assert.Equal(t, manifest.ValueEqual(record.frame.content, request.content), true)
			_, oldMatches := record.frame.stored.History(hr.Named(), previous.content)
			_, currentMatches := record.frame.stored.History(hr.Named(), request.content)
			assert.Equal(t, oldMatches, true)
			assert.Equal(t, currentMatches, false)
			_, complete = client.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, false)
			var observed RenderAdmission
			request.input.Admit = func(value RenderAdmission) (bool, error) {
				observed = value
				return false, nil
			}
			result := templateResult{record: record, receipt: admission.Receipt}
			_, err = result.admit(t.Context(), client, &request)
			assert.NoError(t, err)
			assert.Equal(t, observed.Accepted, false)
			var work manifest.WorkRecorder
			work.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
			current.Work = work.Scope().RenderWork()
			current.Admit = request.input.Admit
			reused, updated, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, current))
			assert.NoError(t, err)
			assert.Diff(t, reused, initial)
			assert.Equal(t, observed.Accepted, false)
			assert.Equal(t, updated.Equal(receipt), false)
			for _, row := range work.Scope().Snapshot(0) {
				if row.Key.Phase == manifest.WorkHelm {
					assert.Equal(t, row.Counts.Attempts, uint64(0))
				}
			}
			assert.NoError(t, client.RecordRender(t.Context(), RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: updated,
				Records: emptyRenderInventory,
				Sources: []manifest.SourceEvidence{},
			}))
			_, complete = client.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, true)
			_, complete = client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, false)
			_, _, err = client.Template(t.Context(), hr, nil, current)
			assert.NoError(t, err)
			assert.Equal(t, observed.Accepted, true)
		})
	}
}

func TestFramePublication_UnavailableContent(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
	})
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	documents, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)
	assert.NoError(t, client.RecordRender(t.Context(), RenderAcceptance{
		Owner: hr.Named(), Receipt: receipt, Records: emptyRenderInventory, Sources: []manifest.SourceEvidence{},
	}))
	frame := receipt.Value().(renderFrame)
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	prior, descriptor, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	before, err := fs.ReadFile(prior, "content")
	assert.NoError(t, err)
	frame.content = make(chan struct{})
	unsupported := manifest.NewRenderReceipt(frame, equalRenderFrame)
	assert.NoError(t, client.RecordRender(t.Context(), RenderAcceptance{
		Owner: hr.Named(), Receipt: unsupported, Records: emptyRenderInventory, Sources: []manifest.SourceEvidence{},
	}))
	after, current, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	data, err := fs.ReadFile(after, "content")
	assert.NoError(t, err)
	assert.Equal(t, current.Digest, descriptor.Digest)
	assert.Diff(t, data, before)
	decoded, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, documents)
	history, complete := client.PreviousRender(t.Context(), hr, nil, input)
	assert.Equal(t, complete, true)
	assert.Equal(t, len(maps.Collect(history)), 0)
}
