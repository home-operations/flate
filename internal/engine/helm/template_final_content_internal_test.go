package helm

import (
	"bytes"
	"errors"
	"io/fs"
	"maps"
	"slices"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestTemplate_QualificationRequiresPreparedScope(t *testing.T) {
	t.Parallel()
	client := NewClient(cachetest.New(t, "", cache.New), nil)
	hr := newHR()
	values := func(path string) map[string]any {
		return map[string]any{"specInput": map[string]any{"sync": map[string]any{
			"name": "sync", "url": "https://fixture.invalid/repo",
			"ref": "refs/heads/main", "path": path,
		}}}
	}
	const direct = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: sync, namespace: flux-system}
spec: {{ toYaml .Values.specInput | nindent 2 }}
`
	const literal = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: sync, namespace: flux-system}
spec:
  sync: {name: sync, url: "https://fixture.invalid/repo", ref: refs/heads/main, path: ./apps, kind: GitRepository}
`
	const list = `apiVersion: v1
kind: List
metadata: {name: instances}
spec: {{ toYaml .Values.specInput | nindent 2 }}
items:
  - apiVersion: fluxcd.controlplane.io/v1
    kind: FluxInstance
    metadata: {name: sync, namespace: flux-system}
    spec:
      sync: {name: sync, url: "https://fixture.invalid/repo", ref: refs/heads/main, path: ./apps, kind: GitRepository}
`
	for _, tc := range []struct {
		name, path, defaults, template, included string
		possible                                 bool
	}{
		{"covered", "./apps", "specInput: {sync: {kind: GitRepository}}", direct, "", true},
		{"different covered path", "./apps/plex", "specInput: {sync: {kind: GitRepository}}", direct, "", true},
		{"disjoint", "./other", "specInput: {sync: {kind: GitRepository}}", direct, "", false},
		{"different kind", "./apps", "specInput: {sync: {kind: OCIRepository}}", direct, "", false},
		{"missing kind", "./apps", "{}", direct, "", false},
		{"literal", "./other", "{}", literal, "", true},
		{"list", "./other", "{}", list, "", true},
		{"included CRD directory file", "./other", "specInput: {sync: {kind: GitRepository}}", direct, literal, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := map[string]string{
				"mychart/Chart.yaml":              "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
				"mychart/values.yaml":             tc.defaults,
				"mychart/templates/instance.yaml": tc.template,
			}
			if tc.included != "" {
				files["mychart/crds/instance.yaml"] = tc.included
			}
			root := testutil.WriteTree(t, files)
			var observed, projected bool
			docs, _, err := client.Template(t.Context(), hr, values(tc.path), RenderOptions{
				Sources: localChartResolver(t, "chart-repo", "flux-system", root),
				EntryReach: func(objects []manifest.BaseManifest) bool {
					projected = true
					return slices.ContainsFunc(objects, func(object manifest.BaseManifest) bool {
						entry, valid := manifest.ApplicationFor(object)
						return valid && entry.Source.Kind == manifest.KindGitRepository &&
							(entry.Path == "./apps" || entry.Path == "./apps/plex")
					})
				},
				Admit: func(admission RenderAdmission) (bool, error) {
					observed = true
					assert.Equal(t, admission.Possible, tc.possible)
					return !admission.Possible, nil
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, observed, true)
			assert.Equal(t, projected, tc.template == direct && tc.included == "")
			assert.Equal(t, len(docs) != 0, tc.possible)
			if tc.template == literal || tc.template == list || tc.included != "" {
				assert.ErrorIf(t, !slices.ContainsFunc(docs, func(doc manifest.DocOccurrence) bool {
					parsed, err := manifest.ParseDoc(doc.Document, manifest.ParseDocOptions{})
					assert.NoError(t, err)
					app, valid := manifest.ApplicationFor(parsed.Object)
					return valid && app.Path == "./apps"
				}), "render did not emit the reachable FluxInstance")
			}
		})
	}
}

func TestPrepareTemplate_FinalContentSnapshots(t *testing.T) {
	t.Parallel()
	for _, regime := range []string{"source observation", "immutable source", "transferred preparation"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
			})
			client := NewClient(cachetest.New(t, "", cache.New), nil)
			hr := newHR()
			var preparation, successor Preparation
			defer func() { preparation = Preparation{} }()
			defer func() { successor = Preparation{} }()
			input := RenderOptions{
				Preparation: &preparation,
				Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
			}
			if regime == "immutable source" {
				artifact := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
				artifact.Content = "immutable-source"
				sources := store.New()
				sources.SetArtifact(hr.ChartSource(), &artifact)
				input.Sources = NewSources(sources, nil)
			}
			first, err := client.prepareTemplate(t.Context(), hr, nil, &input)
			assert.NoError(t, err)
			current := input
			omitted := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: hr.Namespace, Name: "missing"}
			current.Omitted = []manifest.NamedResource{omitted, omitted}
			if regime == "transferred preparation" {
				assert.Equal(t, preparation.Matches(client, hr, &current), true)
				successor, preparation = preparation, Preparation{}
				current.Preparation = &successor
			}
			next, err := client.prepareTemplate(t.Context(), hr, nil, &current)
			assert.NoError(t, err)
			assert.Equal(t, next.key, first.key)
			assert.Equal(t, next.binding.Equal(&first.binding, equalTemplatePolicy), true)
			assert.Equal(t, manifest.ValueEqual(next.content, first.content), false)
			current.Omitted[0].Name = "mutated"
			assert.Diff(t, next.input.Omitted, []manifest.NamedResource{omitted})
			payload, err := testutil.EncodeRender(
				[]map[string]any{{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "raw"}}},
			)
			assert.NoError(t, err)
			header := renderHeader{
				Analysis: &chartAnalysis{Version: "0.1.0"},
			}
			shared := cache.RenderFrame{Key: first.key, Binding: first.binding.Fingerprint(), Output: payload}
			frame, err := shared.Encode(frameDomain, header)
			assert.NoError(t, err)
			publication, err := frame.PreparePublication(t.Context(), cache.FramePublicationInput{
				Owner: hr.Named(), Content: first.content, Records: emptyRenderInventory, Complete: true,
			})
			assert.NoError(t, err)
			header.persist, header.content = client.cache.CanPersistRenders(), first.content
			previous := renderFrame{
				Key:     shared.Key,
				Binding: shared.Binding,
				Output:  payload,
				Stored:  publication.Frame(),
				Header:  header,
				Inputs:  first.binding,
			}
			receipt := renderDomain.Receipt(previous)
			record, documents, err := next.validateRecord(t.Context(), &renderRecord{
				Output:   payload,
				Analysis: header.Analysis,
				frame:    previous,
				receipt:  receipt,
			}, &next.binding, false)
			assert.NoError(t, err)
			assert.Equal(t, record.receipt.Equal(receipt), false)
			assert.Equal(t, manifest.ValueEqual(record.frame.Header.content, next.content), true)
			_, accepted := record.frame.Stored.History(hr.Named(), next.content)
			assert.Equal(t, accepted, false)
			_, accepted = previous.Stored.History(hr.Named(), first.content)
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
			updated, err := record.frame.Stored.PreparePublication(t.Context(), cache.FramePublicationInput{
				Owner: hr.Named(), Content: record.frame.Header.content, Records: emptyRenderInventory, Complete: true,
			})
			assert.NoError(t, err)
			_, accepted = updated.Frame().History(hr.Named(), next.content)
			assert.Equal(t, accepted, true)
			_, accepted = updated.Frame().History(hr.Named(), first.content)
			assert.Equal(t, accepted, false)
		})
	}
}

//nolint:gocognit // Content rebinding MUST retain pending, accepted, and reopened assertions in each backing regime.
func TestTemplate_FinalContentRebinding(t *testing.T) {
	t.Parallel()
	for _, regime := range []string{"memory", "disk", "transferred preparation"} {
		t.Run(regime, func(t *testing.T) {
			t.Parallel()
			persistent := cache.RenderPersistenceDisabledReason() == ""
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
				"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
			})
			cacheDir := t.TempDir()
			client := NewClient(cachetest.New(t, cacheDir, cache.New), nil)
			hr := newHR()
			var preparation, successor Preparation
			defer func() { preparation = Preparation{} }()
			defer func() { successor = Preparation{} }()
			input := RenderOptions{
				Preparation: &preparation,
				Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
			}
			initial, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
			assert.NoError(t, err)
			assert.NoError(t, client.cache.RecordRender(t.Context(), cache.RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
				Sources: []manifest.SourceEvidence{},
			}))
			_, complete := client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, persistent)
			previous := receipt.Value().(renderFrame)
			current := input
			a := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: hr.Namespace, Name: "a"}
			b := a
			b.Name = "b"
			current.Omitted = []manifest.NamedResource{b, a, b}
			if regime == "disk" {
				assert.NoError(t, client.cache.Close())
				client = NewClient(cachetest.New(t, cacheDir, cache.New), nil)
				current.Preparation = nil
			}
			if regime == "transferred preparation" {
				assert.Equal(t, preparation.Matches(client, hr.Clone(), &current), true)
				successor, preparation = preparation, Preparation{}
				current.Preparation = &successor
			}
			request, err := client.prepareTemplate(t.Context(), hr.Clone(), nil, &current)
			assert.NoError(t, err)
			assert.Equal(t, request.key, previous.Key)
			assert.Equal(t, request.binding.Equal(&previous.Inputs, equalTemplatePolicy), true)
			assert.Diff(t, request.input.Omitted, []manifest.NamedResource{a, b})
			current.Omitted[0].Name = "mutated"
			assert.Diff(t, request.input.Omitted, []manifest.NamedResource{a, b})
			current.Omitted = []manifest.NamedResource{a, b}
			record, docs, err := request.lookupRender(t.Context(), false)
			assert.NoError(t, err)
			result := templateResult{record: record, docs: docs}
			if record != nil {
				result.receipt = record.receipt
			}
			admission := result.admission(&request)
			assert.Equal(t, record != nil, persistent || regime != "disk")
			var observed RenderAdmission
			request.input.Admit = func(value RenderAdmission) (bool, error) {
				observed = value
				return false, nil
			}
			if record != nil {
				assert.Equal(t, admission.Accepted, false)
				assert.Equal(t, admission.Receipt.Equal(receipt), false)
				assert.Equal(t, bytes.Equal(record.Output, previous.Output), true)
				assert.Equal(t, manifest.ValueEqual(record.frame.Header.content, request.content), true)
				if persistent {
					_, oldMatches := record.frame.Stored.History(hr.Named(), previous.Header.content)
					_, currentMatches := record.frame.Stored.History(hr.Named(), request.content)
					assert.Equal(t, oldMatches, true)
					assert.Equal(t, currentMatches, false)
				} else {
					assert.Equal(t, record.frame.Stored == nil, true)
				}
				result := templateResult{record: record, receipt: admission.Receipt}
				_, err = result.admit(t.Context(), client, &request)
				assert.NoError(t, err)
			}
			_, complete = client.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, false)
			var work manifest.WorkRecorder
			work.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
			current.Work = work.Scope().RenderWork()
			current.Admit = request.input.Admit
			reused, updated, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, current))
			assert.NoError(t, err)
			assert.Diff(t, reused, initial)
			assert.Equal(t, observed.Accepted, false)
			assert.Equal(t, updated.Equal(receipt), false)
			wantAttempts := uint64(0)
			if !persistent && regime == "disk" {
				wantAttempts = 1
			}
			for _, row := range work.Scope().Snapshot(0) {
				if row.Key.Phase != manifest.WorkHelm {
					continue
				}
				assert.Equal(t, row.Counts.Attempts, wantAttempts)
			}
			assert.NoError(t, client.cache.RecordRender(t.Context(), cache.RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: updated,
				Records: emptyRenderInventory,
				Sources: []manifest.SourceEvidence{},
			}))
			_, complete = client.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, persistent)
			_, complete = client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, false)
			_, _, err = client.Template(t.Context(), hr, nil, current)
			assert.NoError(t, err)
			assert.Equal(t, observed.Accepted, persistent)
		})
	}
}

func TestFramePublication_UnavailableContent(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
	})
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New), nil)
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	documents, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)
	assert.NoError(t, client.cache.RecordRender(t.Context(), cache.RenderAcceptance{
		Owner: hr.Named(), Receipt: receipt, Records: emptyRenderInventory, Sources: []manifest.SourceEvidence{},
	}))
	frame := receipt.Value().(renderFrame)
	key := cache.BlobKey{Content: frame.Key, Render: cache.RenderAccepted}
	prior, descriptor, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	var before []byte
	if persistent {
		assert.NoError(t, err)
		before, err = fs.ReadFile(prior, "content")
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, prior == nil, true)
		err = nil
	}
	assert.NoError(t, err)
	frame.Header.content = make(chan struct{})
	unsupported := renderDomain.Receipt(frame)
	assert.NoError(t, client.cache.RecordRender(t.Context(), cache.RenderAcceptance{
		Owner: hr.Named(), Receipt: unsupported, Records: emptyRenderInventory, Sources: []manifest.SourceEvidence{},
	}))
	after, current, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	if persistent {
		assert.NoError(t, err)
		data, readErr := fs.ReadFile(after, "content")
		assert.NoError(t, readErr)
		assert.Equal(t, current.Digest, descriptor.Digest)
		assert.Diff(t, data, before)
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, after == nil, true)
		assert.Equal(t, current.Digest, descriptor.Digest)
	}
	decoded, err := testutil.DecodeRender(frame.Output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, documents)
	history, complete := client.PreviousRender(t.Context(), hr, nil, input)
	assert.Equal(t, complete, persistent)
	assert.Equal(t, history != nil, persistent)
	if persistent {
		assert.Equal(t, len(maps.Collect(history)), 0)
	}
}
