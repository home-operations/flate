package helm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
	"oras.land/oras-go/v2/errdef"
)

func TestTemplate_AcceptedHistory(t *testing.T) {
	t.Parallel()
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, chartDir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n")
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	client := NewClient(owner)
	hr := newHR()
	hr.TargetNamespace = "runtime"
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", chartDir)}
	docs, receipt, err := client.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	if receipt == nil {
		t.Fatal("successful raw template supplied no receipt")
	}
	frame := receipt.Value().(renderFrame)
	frame.header.Binding = manifest.Fingerprint(frame.binding)
	frame.header.Analysis = (chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
	frame.encoded, err = encodeFrame(frame.header, frame.output)
	assert.NoError(t, err)
	frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	original := bytes.Clone(frame.output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).output, original), true)
	raw, err := manifest.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	if _, _, _, err := owner.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("standalone template wrote durable output: %v", err)
	}
	assert.NoError(t, client.RecordRender(t.Context(), hr.Named(), receipt, nil))
	assert.NoError(t, client.RecordRender(t.Context(), hr.Named(), nil, emptyRenderInventory))
	input.Admit = func(string) (bool, error) { t.Fatal("history called Admit"); return false, nil }
	if previous, complete := client.PreviousRender(t.Context(), hr, nil, input); complete || previous != nil {
		t.Fatal("unaccepted template supplied history")
	}
	t.Run("durable", func(t *testing.T) {
		t.Parallel()
		if !renderPersistenceEligible(t) {
			t.Skip("requires a clean VCS-stamped build for positive persistence")
		}
		child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
		records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Refs: []manifest.NamedResource{manifest.BootstrapSourceID}, ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/blue"}}}}
		assert.NoError(t, client.RecordRender(t.Context(), hr.Named(), receipt, maps.All(records)))
		fresh := NewClient(cachetest.New(t, root, cache.New))
		previous, complete := fresh.PreviousRender(t.Context(), hr, nil, input)
		assert.Equal(t, complete, true)
		got := maps.Collect(previous)
		assert.Diff(t, got, records)

		for _, tc := range []struct {
			name   string
			mutate func(*renderHeader)
		}{
			{"request key", func(h *renderHeader) { h.Key = "foreign" }},
			{"missing analysis", func(h *renderHeader) { h.Analysis = nil }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
				invalid := frame.header
				tc.mutate(&invalid)
				invalidReceipt, err := encodeFrame(invalid, frame.output)
				assert.NoError(t, err)
				invalidFrame, err := decodeFrame(invalidReceipt)
				assert.NoError(t, err)
				accepted, err := encodeAccepted(hr.Named(), invalidFrame, maps.All(records))
				assert.NoError(t, err)
				_, _, _, err = isolated.cache.Blob(t.Context(), key, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					_, err := io.Copy(w, io.MultiReader(bytes.NewReader(invalidReceipt), bytes.NewReader(accepted)))
					return cache.ProducerResult{}, err
				})
				assert.NoError(t, err)

				prepared, err := isolated.prepareTemplate(t.Context(), hr, nil, input)
				assert.NoError(t, err)
				defer func() {
					if problem := recover(); problem != nil {
						t.Errorf("invalid raw metadata panicked instead of missing: %v", problem)
					}
				}()
				found, err := isolated.lookupRender(t.Context(), &prepared.loaded, prepared.key)
				assert.NoError(t, err)
				if found != nil {
					t.Fatal("invalid raw request metadata reused cached output")
				}
				if _, complete := isolated.PreviousRender(t.Context(), hr, nil, input); complete {
					t.Fatal("invalid raw request metadata supplied history")
				}
			})
		}
		t.Run("typed record key", func(t *testing.T) {
			t.Parallel()
			isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			assert.NoError(t, isolated.RecordRender(t.Context(), hr.Named(), receipt, maps.All(records)))
			prepared, err := isolated.prepareTemplate(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			remembered, err := isolated.lookupRender(t.Context(), &prepared.loaded, prepared.key)
			assert.NoError(t, err)
			if remembered == nil {
				t.Fatal("matching request did not load a typed record")
			}
			prepared.loaded.record = remembered
			reader, _, _, err := isolated.cache.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
			assert.NoError(t, err)
			data, err := fs.ReadFile(reader, "content")
			assert.NoError(t, err)
			foreign := key
			foreign.Content = "foreign"
			reader, _, _, err = isolated.cache.Blob(t.Context(), foreign, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, err := w.Write(data)
				return cache.ProducerResult{}, err
			})
			assert.NoError(t, err)
			assert.Equal(t, cache.AcquisitionIdentity(reader), remembered.identity)

			found, err := isolated.lookupRender(t.Context(), &prepared.loaded, "foreign")
			assert.NoError(t, err)
			if found != nil {
				t.Fatal("same acquisition identity lent a typed record to a foreign request key")
			}
		})
		t.Run("frozen preparation", func(t *testing.T) {
			t.Parallel()
			renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			current := hr.Clone()
			current.CommonMetadata = &helmv2.CommonMetadata{Annotations: map[string]string{"policy": "before"}}
			assert.NoError(t, json.Unmarshal([]byte(`{"dependsOn":[{"name":"before"}],"valuesFrom":[{"kind":"ConfigMap","name":"before"}],"healthCheckExprs":[{"apiVersion":"v1","kind":"Pod","current":"true"}]}`), &current.HelmReleaseSpec))
			before := current.Clone()
			options := RenderOptions{Sources: input.Sources, SkipKinds: []string{"Skipped"}}
			_, pending, err := renderer.Template(t.Context(), current, nil, options)
			assert.NoError(t, err)
			current.CommonMetadata.Annotations["policy"], options.SkipKinds[0] = "after", "Different"
			current.DependsOn[0].Name, current.ValuesFrom[0].Name, current.HealthCheckExprs[0].Current = "after", "after", "false"
			assert.NoError(t, renderer.RecordRender(t.Context(), hr.Named(), pending, maps.All(records)))
			expected := RenderOptions{Sources: input.Sources, SkipKinds: []string{"Skipped"}}
			if _, complete := renderer.PreviousRender(t.Context(), before, nil, expected); !complete {
				t.Fatal("caller mutation changed the prepared receipt")
			}
			if _, complete := renderer.PreviousRender(t.Context(), current, nil, expected); complete {
				t.Fatal("mutated declaration borrowed the prepared receipt")
			}
			if _, complete := renderer.PreviousRender(t.Context(), before, nil, options); complete {
				t.Fatal("mutated policy borrowed the prepared receipt")
			}
		})

		t.Run("unaccepted preparation", func(t *testing.T) {
			t.Parallel()
			renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			original := RenderOptions{Sources: input.Sources}
			_, first, err := renderer.Template(t.Context(), hr, nil, original)
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), hr.Named(), first, maps.All(records)))
			changed := original
			changed.WipeSecrets = true
			_, pending, err := renderer.Template(t.Context(), hr, nil, changed)
			assert.NoError(t, err)
			if pending == nil {
				t.Fatal("raw successor supplied no receipt")
			}
			if _, complete := renderer.PreviousRender(t.Context(), hr, nil, original); !complete {
				t.Error("unaccepted preparation displaced the durable frame")
			}
			if _, complete := renderer.PreviousRender(t.Context(), hr, nil, changed); complete {
				t.Error("unaccepted preparation became durable history authority")
			}
		})

		t.Run("publication", func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			pending := receipt
			accepting := hr.Named()
			open := func() *Client {
				renderer := NewClient(cachetest.New(t, directory, cache.New))
				return renderer
			}
			renderer := open()
			indexPath := filepath.Join(directory, "oci", "index.json")
			storedKey := func() string {
				t.Helper()
				reader, descriptor, _, err := renderer.cache.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
				assert.NoError(t, err)
				data, err := fs.ReadFile(reader, "content")
				assert.NoError(t, err)
				stored, err := decodeFrame(data)
				assert.NoError(t, err)
				content := descriptor.Annotations[acceptedRenderAnnotation]
				var actual acceptedRender
				assert.NoError(t, json.Unmarshal(data[12+int(binary.BigEndian.Uint32(data[8:12]))+stored.header.Size:], &actual))
				if content == "" || content != manifest.Fingerprint(actual) {
					t.Fatal("stored accepted-frame key does not identify the actual accepted frame")
				}
				return content
			}
			unchanged := func() func() {
				t.Helper()
				content, err := os.ReadFile(indexPath)
				assert.NoError(t, err)
				stamp := time.Unix(1, 0)
				assert.NoError(t, os.Chtimes(indexPath, stamp, stamp))
				before, err := os.Stat(indexPath)
				assert.NoError(t, err)
				return func() {
					t.Helper()
					after, err := os.Stat(indexPath)
					assert.NoError(t, err)
					current, err := os.ReadFile(indexPath)
					assert.NoError(t, err)
					if !bytes.Equal(current, content) || !after.ModTime().Equal(before.ModTime()) {
						t.Fatal("unchanged accepted frame republished the OCI index")
					}
				}
			}
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, maps.All(records)))
			initialKey := storedKey()
			_, pending, err = renderer.Template(t.Context(), hr, nil, RenderOptions{Sources: input.Sources})
			assert.NoError(t, err)
			resident := pending.Value().(renderFrame)
			if resident.accepted == nil || resident.acceptedKey == "" || resident.identity == "" {
				t.Fatal("warm receipt did not carry decoded accepted evidence")
			}
			check := unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, maps.All(records)))
			check()
			reader, _, _, err := renderer.cache.Blob(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, nil)
			assert.NoError(t, err)
			data, err := fs.ReadFile(reader, "content")
			assert.NoError(t, err)
			_, _, _, err = renderer.cache.Blob(t.Context(), key, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, err := w.Write(data)
				return cache.ProducerResult{}, err
			})
			assert.NoError(t, err)

			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, maps.All(records)))
			assert.Equal(t, storedKey(), initialKey)
			assert.NoError(t, renderer.cache.Close())
			renderer = open()
			assert.Equal(t, storedKey(), initialKey)
			check = unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, maps.All(records)))
			check()
			initialIndex, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, emptyRenderInventory))
			changedIndex, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			if storedKey() == initialKey {
				t.Fatal("changed accepted frame retained the previous content key")
			}
			if bytes.Equal(initialIndex, changedIndex) {
				t.Fatal("changed accepted frame was not published")
			}
			previous, complete := renderer.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete && previous != nil, true)
			for range previous {
				t.Fatal("changed complete-empty frame retained children")
			}
			_, borrowedReceipt, err := renderer.Template(t.Context(), hr, nil, RenderOptions{Sources: input.Sources})
			assert.NoError(t, err)
			foreign := accepting
			foreign.Namespace = "foreign"
			assert.NoError(t, renderer.RecordRender(t.Context(), foreign, pending, maps.All(records)))
			if _, complete := renderer.PreviousRender(t.Context(), hr, nil, input); complete {
				t.Fatal("foreign overwrite retained the previous owner's history")
			}
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, borrowedReceipt, maps.All(records)))
			assert.Equal(t, storedKey(), initialKey)
			previous, complete = renderer.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, true)
			got := maps.Collect(previous)
			assert.Diff(t, got, records)
			check = unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), accepting, pending, maps.All(records)))
			check()
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			check = unchanged()
			if err := renderer.RecordRender(canceled, accepting, pending, maps.All(records)); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled same-frame publication: %v", err)
			}
			check()
			check = unchanged()
			if err := renderer.RecordRender(t.Context(), accepting, manifest.NewRenderReceipt("invalid", nil), maps.All(records)); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failed receipt publication: %v", err)
			}
			check()
			check = unchanged()
			assert.NoError(t, renderer.cache.Close())
			if err := renderer.RecordRender(t.Context(), accepting, pending, emptyRenderInventory); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("closed owner lookup: %v", err)
			}
			check()
			renderer = open()
			previous, complete = renderer.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, true)
			got = maps.Collect(previous)
			assert.Diff(t, got, records)
		})
		described, err := fresh.describeChart(t.Context(), hr, input.Sources)
		assert.NoError(t, err)
		assert.Equal(t, described.chart == nil, true)
		assert.Equal(t, described.analysis != nil && described.record != nil, true)
		for _, tc := range []struct {
			name   string
			mutate func(*RenderOptions)
		}{
			{"dependency substitutions", func(r *RenderOptions) {
				r.DependencySubstitutions = values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"DEP": "effective-child"}}}})
			}},
			{"wipe", func(o *RenderOptions) { o.WipeSecrets = true }},
			{"skip kinds", func(o *RenderOptions) { o.SkipKinds = []string{manifest.KindKustomization} }},
			{"source binding", func(o *RenderOptions) {
				changed := *o.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
				changed.URL = "different"
				sources := store.New()
				sources.SetArtifact(hr.ChartSource(), &changed)
				o.Sources = NewSources(sources)
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				isolated := NewClient(cachetest.New(t, t.TempDir(), cache.New))
				assert.NoError(t, isolated.RecordRender(t.Context(), hr.Named(), receipt, maps.All(records)))
				changed := input
				tc.mutate(&changed)
				if _, complete := isolated.PreviousRender(t.Context(), hr, nil, changed); complete {
					t.Fatal("incompatible emission policy borrowed history")
				}
				changed.Admit = nil
				out, _, err := isolated.Template(t.Context(), hr, nil, changed)
				assert.NoError(t, err)
				assert.Diff(t, out, raw)
			})
		}
		foreign := hr.Clone()
		foreign.Namespace = "foreign"
		if _, complete := fresh.PreviousRender(t.Context(), foreign, nil, input); complete {
			t.Fatal("foreign effective owner borrowed history")
		}
		missing := input
		missing.Sources = NewSources(store.New())
		if _, complete := fresh.PreviousRender(t.Context(), hr, nil, missing); complete {
			t.Fatal("unmaterialized artifact supplied history")
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if err := client.RecordRender(canceled, hr.Named(), receipt, emptyRenderInventory); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acceptance: %v", err)
		}
		previous, complete = fresh.PreviousRender(t.Context(), hr, nil, input)
		assert.Equal(t, complete, true)
		for id := range previous {
			assert.Equal(t, id, child)
		}
		assert.NoError(t, client.RecordRender(t.Context(), hr.Named(), receipt, emptyRenderInventory))
		previous, complete = fresh.PreviousRender(t.Context(), hr, nil, input)
		assert.Equal(t, complete && previous != nil, true)
		for range previous {
			t.Fatal("empty acceptance retained previous records")
		}
	})
}

func TestTemplate_OCIResidentGenerationValidation(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	st, hr, directory := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	artifact := &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: chartFixtureFS(hr, os.DirFS(directory)), LocalPath: directory, Digest: digest, Revision: "6.15.0@" + digest}
	st.SetArtifact(hr.ChartSource(), artifact)
	owner := cachetest.New(t, t.TempDir(), cache.New)
	client := NewClient(owner)
	input := RenderOptions{Sources: NewSources(st)}
	for range 2 {
		_, receipt, err := client.Template(t.Context(), hr, nil, input)
		assert.NoError(t, err)
		assert.NoError(t, client.RecordRender(t.Context(), hr.Named(), receipt, emptyRenderInventory))
	}
	prepared, err := client.prepareTemplate(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	resident := prepared.loaded.record
	if resident == nil || resident.identity == "" {
		t.Fatal("fixture did not retain the validated resident generation")
	}
	header := resident.frame.header
	analysis := *header.Analysis
	analysis.Version = "invalid"
	header.Analysis = &analysis
	payload, err := encodeFrame(header, resident.Output)
	assert.NoError(t, err)
	_, _, _, err = owner.Blob(t.Context(), cache.BlobKey{Content: resident.key, Render: cache.RenderAccepted}, cache.Policy{}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		_, err := w.Write(payload)
		return cache.ProducerResult{}, err
	})
	assert.NoError(t, err)

	if _, _, err := client.Template(t.Context(), hr, nil, input); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("replaced OCI generation escaped version validation: %v", err)
	}
	if _, complete := client.PreviousRender(t.Context(), hr, nil, input); complete {
		t.Fatal("invalid OCI generation supplied accepted history")
	}
	changed := *artifact
	changed.Revision = "invalid"
	st.SetArtifact(hr.ChartSource(), &changed)
	if _, _, err := client.Template(t.Context(), hr, nil, input); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("changed OCI revision borrowed resident validation: %v", err)
	}
}

func TestPreviousRender_SourceBinding(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := t.TempDir()
	testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n")
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	for _, tc := range []struct {
		name   string
		mutate func(*manifest.HelmRelease, *store.SourceArtifact)
	}{
		{"source name", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Name = "foreign" }},
		{"source namespace", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Namespace = "foreign" }},
		{"source group", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Group = "foreign" }},
		{"source kind", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Kind = manifest.KindBucket }},
		{"artifact kind", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Kind = "foreign" }},
		{"url", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.URL = "foreign" }},
		{"path", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.LocalPath = "foreign" }},
		{"origin", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.LocalRoot = "foreign" }},
		{"key", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Key = "foreign" }},
		{"revision", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Revision = "foreign" }},
		{"digest", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Digest = "foreign" }},
		{"alias", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.InputAlias = !a.InputAlias }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			_, receipt, err := renderer.Template(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), hr.Named(), receipt, emptyRenderInventory))
			if _, complete := renderer.PreviousRender(t.Context(), hr, nil, input); !complete {
				t.Fatal("matching source binding missed")
			}
			changed := hr.Clone()
			source := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
			tc.mutate(changed, &source)
			objects := store.New()
			objects.SetArtifact(changed.ChartSource(), &source)
			current := input
			current.Sources = NewSources(objects)
			prepared, err := renderer.prepareTemplate(t.Context(), changed, nil, current)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.binding
			binding.Policy = prepared.binding.Policy
			header := frame.header
			header.Key, header.Binding = prepared.key, manifest.Fingerprint(binding)
			header.Analysis = (chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
			wire, err := encodeFrame(header, frame.output)
			assert.NoError(t, err)
			stored, err := decodeFrame(wire)
			assert.NoError(t, err)
			accepted, err := encodeAccepted(changed.Named(), stored, emptyRenderInventory)
			assert.NoError(t, err)
			_, _, _, err = renderer.cache.Blob(t.Context(), cache.BlobKey{Content: prepared.key, Render: cache.RenderAccepted}, cache.Policy{MaxAge: 0}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				_, err := io.Copy(w, io.MultiReader(bytes.NewReader(wire), bytes.NewReader(accepted)))
				return cache.ProducerResult{}, err
			})
			assert.NoError(t, err)

			if _, complete := renderer.PreviousRender(t.Context(), changed, nil, current); complete {
				t.Fatal("foreign source coordinate borrowed history")
			}
		})
	}
}

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := manifest.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{header: renderHeader{Key: "oversized-accepted", Analysis: &chartAnalysis{Version: "0.1.0"}}, output: payload}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {File: string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))}}
	if err := renderer.RecordRender(t.Context(), owner, receipt, maps.All(records)); !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("oversized accepted inventory was published: %v", err)
	}
}
