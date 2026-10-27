package kustomize

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
	"oras.land/oras-go/v2/errdef"
)

type historyCountingFS struct {
	fs.FS
	opens atomic.Int64
}

func (f *historyCountingFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

func TestRenderFlux_AcceptedHistory(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	spec := &manifest.Kustomization{Name: "emitter", Path: "."}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	owner := spec.Named()
	owner.Namespace = "inherited"
	var absent *TreeCache
	if previous, complete := absent.PreviousRender(t.Context(), req, owner); previous != nil || complete {
		t.Fatal("nil renderer supplied history")
	}
	t.Run("disabled preparation", func(t *testing.T) {
		t.Parallel()
		counted := &historyCountingFS{FS: os.DirFS(root)}
		artifact := *req.Source
		artifact.FS = counted
		input := req
		input.Source, input.ApplyIgnore = &artifact, true
		fresh := newTestTreeCache(t, "")
		counted.opens.Store(0)
		if previous, complete := fresh.PreviousRender(t.Context(), input, owner); previous != nil || complete {
			t.Fatal("disabled renderer supplied history")
		}
		if count := counted.opens.Load(); count != 0 {
			t.Fatalf("disabled history prepared source: opens=%d", count)
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if previous, complete := fresh.PreviousRender(canceled, input, owner); previous != nil || complete {
			t.Fatal("canceled disabled renderer supplied history")
		}
		assert.Equal(t, counted.opens.Load(), 0)
	})
	docs, receipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	if receipt == nil {
		t.Fatal("successful raw render supplied no receipt")
	}
	frame := receipt.Value().(renderFrame)
	frame.header.Binding = manifest.Fingerprint(frame.binding)
	frame.encoded, err = encodeFrame(frame.header, frame.output)
	assert.NoError(t, err)
	frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	original := bytes.Clone(frame.output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).output, original), true)
	raw, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := sourcecache.BlobKey{Content: frame.header.Key, Render: sourcecache.RenderAccepted}
	if _, _, _, err := cache.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("standalone render wrote durable output: %v", err)
	}
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: nil}))
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: nil, Records: emptyRenderInventory}))
	if records, complete := cache.PreviousRender(t.Context(), req, owner); complete || records != nil {
		t.Fatal("unaccepted output supplied history")
	}
	t.Run("durable", func(t *testing.T) {
		t.Parallel()
		if !renderPersistenceEligible(t) {
			t.Skip("requires a clean VCS-stamped build for positive persistence")
		}
		child := owner
		child.Name = "child"
		records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Refs: []manifest.NamedResource{manifest.BootstrapSourceID}, ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/red"}}}}
		assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: maps.All(records)}))
		fresh := newTestTreeCache(t, cacheDir)
		previous, complete := fresh.PreviousRender(t.Context(), req, owner)
		assert.Equal(t, complete, true)
		got := maps.Collect(previous)
		assert.Diff(t, got, records)
		t.Run("preparation", func(t *testing.T) {
			t.Parallel()
			counted := &historyCountingFS{FS: os.DirFS(root)}
			artifact := *req.Source
			artifact.FS = counted
			input := req
			input.Source, input.ApplyIgnore = &artifact, true
			directory := t.TempDir()
			publisher := newTestTreeCache(t, directory)
			_, pending, err := Render(t.Context(), publisher, input, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.NoError(t, publisher.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: pending, Records: maps.All(records)}))
			reader := newTestTreeCache(t, directory)
			counted.opens.Store(0)
			previous, complete := reader.PreviousRender(t.Context(), input, owner)
			assert.Equal(t, complete && previous != nil, true)
			assert.Diff(t, maps.Collect(previous), records)
			if counted.opens.Load() == 0 {
				t.Fatal("enabled history did not prepare source")
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			counted.opens.Store(0)
			if previous, complete := reader.PreviousRender(canceled, input, owner); previous != nil || complete {
				t.Fatal("canceled enabled renderer supplied history")
			}
			assert.Equal(t, counted.opens.Load(), 0)
			input.Source = nil
			if previous, complete := reader.PreviousRender(t.Context(), input, owner); previous != nil || complete {
				t.Fatal("unavailable source supplied history")
			}
		})

		t.Run("request key", func(t *testing.T) {
			t.Parallel()
			isolated := newTestTreeCache(t, t.TempDir())
			foreign := frame.header
			foreign.Key = "foreign"
			foreignReceipt, err := encodeFrame(foreign, frame.output)
			assert.NoError(t, err)
			foreignFrame, err := decodeFrame(foreignReceipt)
			assert.NoError(t, err)
			accepted, err := encodeAccepted(owner, foreignFrame, maps.All(records))
			assert.NoError(t, err)
			_, _, _, err = isolated.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, err := io.Copy(w, io.MultiReader(bytes.NewReader(foreignReceipt), bytes.NewReader(accepted)))
				return sourcecache.ProducerResult{}, err
			})
			assert.NoError(t, err)

			if _, complete := isolated.PreviousRender(t.Context(), req, owner); complete {
				t.Fatal("foreign request key supplied accepted history")
			}
		})
		t.Run("mutable history reads", func(t *testing.T) {
			t.Parallel()
			directory := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("before")})
			mutable := req
			mutable.Source = localSource(t, directory)
			isolated := newTestTreeCache(t, t.TempDir())
			_, current, err := testutil.RenderMaps(Render(t.Context(), isolated, mutable, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			assert.NoError(t, isolated.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: current, Records: maps.All(records)}))
			if _, complete := isolated.PreviousRender(t.Context(), mutable, owner); !complete {
				t.Fatal("unchanged mutable history missed")
			}
			before, err := isolated.prepareRender(t.Context(), mutable)
			assert.NoError(t, err)
			testutil.WriteFile(t, directory, "cm.yaml", cmYAML("after"))
			after, err := isolated.prepareRender(t.Context(), mutable)
			assert.NoError(t, err)
			assert.Equal(t, before.key, after.key)
			if _, complete := isolated.PreviousRender(t.Context(), mutable, owner); complete {
				t.Fatal("changed mutable read supplied accepted history")
			}
		})
		t.Run("frozen preparation", func(t *testing.T) {
			t.Parallel()
			renderer := newTestTreeCache(t, t.TempDir())
			input := req
			input.SkipKinds = []string{"Skipped"}
			_, pending, err := testutil.RenderMaps(Render(t.Context(), renderer, input, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			input.SkipKinds[0] = "Different"
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: pending, Records: maps.All(records)}))
			before := input
			before.SkipKinds = []string{"Skipped"}
			if _, complete := renderer.PreviousRender(t.Context(), before, owner); !complete {
				t.Fatal("caller mutation changed the prepared receipt")
			}
			if _, complete := renderer.PreviousRender(t.Context(), input, owner); complete {
				t.Fatal("mutated policy borrowed the prepared receipt")
			}
		})

		t.Run("publication", func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			pending := receipt
			accepting := owner
			open := func() *TreeCache {
				return newTestTreeCache(t, directory)
			}
			renderer := open()
			indexPath := filepath.Join(directory, "oci", "index.json")
			storedKey := func() string {
				t.Helper()
				reader, descriptor, _, err := renderer.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
				assert.NoError(t, err)
				data, err := fs.ReadFile(reader, "content")
				assert.NoError(t, err)
				stored, err := decodeFrame(data)
				assert.NoError(t, err)
				content := descriptor.Annotations[acceptedRenderAnnotation]
				if content == "" || content != manifest.Fingerprint(data[12+int(binary.BigEndian.Uint32(data[8:12]))+stored.header.Size:]) {
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
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}))
			initialKey := storedKey()
			_, pending, err = testutil.RenderMaps(Render(t.Context(), renderer, req, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			resident := pending.Value().(renderFrame)
			if resident.accepted == nil || resident.acceptedKey == "" || resident.identity == "" {
				t.Fatal("warm receipt did not carry decoded accepted evidence")
			}
			check := unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}))
			check()
			reader, _, _, err := renderer.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
			assert.NoError(t, err)
			data, err := fs.ReadFile(reader, "content")
			assert.NoError(t, err)
			_, _, _, err = renderer.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, err := w.Write(data)
				return sourcecache.ProducerResult{}, err
			})
			assert.NoError(t, err)

			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}))
			assert.Equal(t, storedKey(), initialKey)
			closeTestTreeCache(t, renderer)
			renderer = open()
			assert.Equal(t, storedKey(), initialKey)
			check = unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}))
			check()
			initialIndex, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: emptyRenderInventory}))
			changedIndex, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			if storedKey() == initialKey {
				t.Fatal("changed accepted frame retained the previous content key")
			}
			if bytes.Equal(initialIndex, changedIndex) {
				t.Fatal("changed accepted frame was not published")
			}
			previous, complete := renderer.PreviousRender(t.Context(), req, accepting)
			assert.Equal(t, complete && previous != nil, true)
			for range previous {
				t.Fatal("changed complete-empty frame retained children")
			}
			_, borrowedReceipt, err := testutil.RenderMaps(Render(t.Context(), renderer, req, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			foreign := accepting
			foreign.Namespace = "foreign"
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: foreign, Receipt: pending, Records: maps.All(records)}))
			if _, complete := renderer.PreviousRender(t.Context(), req, accepting); complete {
				t.Fatal("foreign overwrite retained the previous owner's history")
			}
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: borrowedReceipt, Records: maps.All(records)}))
			assert.Equal(t, storedKey(), initialKey)
			previous, complete = renderer.PreviousRender(t.Context(), req, accepting)
			assert.Equal(t, complete, true)
			got := maps.Collect(previous)
			assert.Diff(t, got, records)
			check = unchanged()
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}))
			check()
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			check = unchanged()
			if err := renderer.RecordRender(canceled, RenderAcceptance{Owner: accepting, Receipt: pending, Records: maps.All(records)}); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled same-frame publication: %v", err)
			}
			check()
			check = unchanged()
			if err := renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: manifest.NewRenderReceipt("invalid", nil), Records: maps.All(records)}); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failed receipt publication: %v", err)
			}
			check()
			check = unchanged()
			closeTestTreeCache(t, renderer)
			if err := renderer.RecordRender(t.Context(), RenderAcceptance{Owner: accepting, Receipt: pending, Records: emptyRenderInventory}); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("closed owner lookup: %v", err)
			}
			check()
			renderer = open()
			previous, complete = renderer.PreviousRender(t.Context(), req, accepting)
			assert.Equal(t, complete, true)
			got = maps.Collect(previous)
			assert.Diff(t, got, records)
		})
		for _, tc := range []struct {
			name   string
			mutate func(*RenderRequest)
		}{
			{"dependency substitutions", func(r *RenderRequest) {
				r.DependencySubstitutions = values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"DEP": "effective-child"}}}})
			}},
			{"wipe", func(r *RenderRequest) { r.WipeSecrets = true }},
			{"strict", func(r *RenderRequest) { r.StrictSubstitutions = true }},
			{"skip kinds", func(r *RenderRequest) { r.SkipKinds = []string{manifest.KindKustomization} }},
			{"source binding", func(r *RenderRequest) { source := *r.Source; source.URL = "different"; r.Source = &source }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				isolated := newTestTreeCache(t, t.TempDir())
				assert.NoError(t, isolated.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: maps.All(records)}))
				changed := req
				tc.mutate(&changed)
				if _, complete := isolated.PreviousRender(t.Context(), changed, owner); complete {
					t.Fatal("incompatible policy or source borrowed history")
				}
				out, _, err := testutil.RenderMaps(Render(t.Context(), isolated, changed, sourcecache.RetryConfig{}))
				assert.NoError(t, err)
				assert.Diff(t, out, raw)
			})
		}
		foreign := owner
		foreign.Namespace = "foreign"
		if _, complete := fresh.PreviousRender(t.Context(), req, foreign); complete {
			t.Fatal("foreign effective owner borrowed history")
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if err := cache.RecordRender(canceled, RenderAcceptance{Owner: owner, Receipt: receipt, Records: emptyRenderInventory}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acceptance: %v", err)
		}
		previous, complete = fresh.PreviousRender(t.Context(), req, owner)
		assert.Equal(t, complete, true)
		for id := range previous {
			assert.Equal(t, id, child)
		}
		assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: emptyRenderInventory}))
		previous, complete = fresh.PreviousRender(t.Context(), req, owner)
		assert.Equal(t, complete && previous != nil, true)
		for range previous {
			t.Fatal("empty acceptance retained previous records")
		}
		remote := frame.header
		remote.Reads.Remotes = []remoteEntry{{Input: remoteInput{URL: "https://example.invalid/remote"}}}
		remoteReceipt, err := encodeFrame(remote, frame.output)
		assert.NoError(t, err)
		assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: manifest.NewRenderReceipt(renderFrame{header: remote, binding: frame.binding, output: frame.output, encoded: remoteReceipt}, equalRenderFrame), Records: maps.All(records)}))
		fresh.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			t.Fatal("history acquired a remote")
			return nil, nil
		})
		if _, complete := fresh.PreviousRender(t.Context(), req, owner); complete {
			t.Fatal("remote observations supplied history")
		}
		reader, _, _, err := cache.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
		assert.NoError(t, err)
		_, err = fs.ReadFile(reader, "content")
		assert.NoError(t, err)
	})
}

func TestPreviousRender_SourceBinding(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")})
	req := RenderRequest{Source: localSource(t, root), Spec: &manifest.Kustomization{Name: "emitter", Path: ".", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "content", Namespace: "sources"}}, SubPath: ".", Label: root}
	owner := req.Spec.Named()
	for _, tc := range []struct {
		name   string
		mutate func(*RenderRequest)
	}{
		{"source name", func(r *RenderRequest) { r.Spec.SourceRef.Name = "foreign" }},
		{"source namespace", func(r *RenderRequest) { r.Spec.SourceRef.Namespace = "foreign" }},
		{"source kind", func(r *RenderRequest) { r.Spec.SourceRef.Kind = manifest.KindBucket }},
		{"artifact kind", func(r *RenderRequest) { r.Source.Kind = "foreign" }},
		{"url", func(r *RenderRequest) { r.Source.URL = "foreign" }},
		{"path", func(r *RenderRequest) { r.Source.LocalPath = "foreign" }},
		{"origin", func(r *RenderRequest) { r.Source.LocalRoot = "foreign" }},
		{"key", func(r *RenderRequest) { r.Source.Key = "foreign" }},
		{"revision", func(r *RenderRequest) { r.Source.Revision = "foreign" }},
		{"digest", func(r *RenderRequest) { r.Source.Digest = "foreign" }},
		{"alias", func(r *RenderRequest) { r.Source.InputAlias = !r.Source.InputAlias }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			renderer := newTestTreeCache(t, t.TempDir())
			_, receipt, err := testutil.RenderMaps(Render(t.Context(), renderer, req, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: emptyRenderInventory}))
			if _, complete := renderer.PreviousRender(t.Context(), req, owner); !complete {
				t.Fatal("matching source binding missed")
			}
			changed := req
			source, spec := *req.Source, *req.Spec
			changed.Source, changed.Spec = &source, &spec
			tc.mutate(&changed)
			prepared, err := renderer.prepareRender(t.Context(), changed)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.binding
			binding.Render = prepared.key
			header := frame.header
			header.Key, header.Binding = prepared.key, manifest.Fingerprint(binding)
			wire, err := encodeFrame(header, frame.output)
			assert.NoError(t, err)
			stored, err := decodeFrame(wire)
			assert.NoError(t, err)
			accepted, err := encodeAccepted(owner, stored, emptyRenderInventory)
			assert.NoError(t, err)
			_, _, _, err = renderer.owner.Blob(t.Context(), sourcecache.BlobKey{Content: prepared.key, Render: sourcecache.RenderAccepted}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, err := io.Copy(w, io.MultiReader(bytes.NewReader(wire), bytes.NewReader(accepted)))
				return sourcecache.ProducerResult{}, err
			})
			assert.NoError(t, err)

			if _, complete := renderer.PreviousRender(t.Context(), changed, owner); complete {
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
	renderer := newTestTreeCache(t, t.TempDir())
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{header: renderHeader{Key: "oversized-accepted"}, output: payload}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {File: string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))}}
	if err := renderer.RecordRender(t.Context(), RenderAcceptance{Owner: owner, Receipt: receipt, Records: maps.All(records)}); !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("oversized accepted inventory was published: %v", err)
	}
}

func TestRenderKey_DependsOnProjection(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")})
	spec := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "."}
	spec.DependsOn = []kustomizev1.DependencyReference{{Name: "${PREFIX:-dev-}ready", Namespace: "${TEAM:-apps}"}}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	snapshot := values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": "prod-", "TEAM": "control"}}}})
	projected := req
	projected.Spec = manifest.MapDependsOn(spec, snapshot.Resolve, snapshot.Resolve).(*manifest.Kustomization)
	cache := newTestTreeCache(t, t.TempDir())
	before, err := cache.prepareRender(t.Context(), req)
	assert.NoError(t, err)
	after, err := cache.prepareRender(t.Context(), projected)
	assert.NoError(t, err)
	assert.Equal(t, before.key, after.key)
	assert.Equal(t, spec.DependsOn[0].Name, "${PREFIX:-dev-}ready")
	for _, change := range []struct {
		name   string
		mutate func(*manifest.Kustomization)
	}{
		{"path", func(k *manifest.Kustomization) { k.Path = "other" }},
		{"name", func(k *manifest.Kustomization) { k.Name = "other" }},
		{"namespace", func(k *manifest.Kustomization) { k.Namespace = "other" }},
		{"target namespace", func(k *manifest.Kustomization) { k.TargetNamespace = "other" }},
		{"inline", func(k *manifest.Kustomization) {
			k.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "other"}}
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			t.Parallel()
			changed := req
			changed.Spec = spec.Clone()
			change.mutate(changed.Spec)
			next, err := cache.prepareRender(t.Context(), changed)
			assert.NoError(t, err)
			if next.key == before.key {
				t.Fatal("render-affecting change retained key")
			}
		})
	}
}

func TestRecordRender_FinalDependencyBinding(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")})
	cacheDir := t.TempDir()
	renderer := newTestTreeCache(t, cacheDir)
	spec := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: ".", DependsOn: []kustomizev1.DependencyReference{{Name: "${DEP}"}}}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	_, pending, err := Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{Owner: spec.Named(), Receipt: pending, Records: emptyRenderInventory}))
	renderer = newTestTreeCache(t, cacheDir)
	_, pending, err = Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	before := pending.Value().(renderFrame)
	if before.accepted == nil || before.acceptedKey == "" || before.identity == "" || before.raw == "" {
		t.Fatal("warm receipt lacks publication memos")
	}
	before.encoded, err = encodeFrame(before.header, before.output)
	assert.NoError(t, err)
	_, _, before.lookup, err = renderer.owner.Blob(t.Context(), sourcecache.BlobKey{Content: before.header.Key, Render: sourcecache.RenderAccepted}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	assert.NoError(t, err)
	pending = manifest.NewRenderReceipt(before, equalRenderFrame)
	wire, output := bytes.Clone(before.encoded), bytes.Clone(before.output)
	final := values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"DEP": "ready"}}}})
	projected := req
	projected.Spec = manifest.MapDependsOn(spec, final.Resolve, nil).(*manifest.Kustomization)
	projected.DependencySubstitutions = final
	if _, complete := renderer.PreviousRender(t.Context(), projected, spec.Named()); complete {
		t.Fatal("unaccepted snapshot supplied history")
	}
	acceptance := RenderAcceptance{Owner: spec.Named(), Receipt: pending, Records: emptyRenderInventory, DependencySubstitutions: final}
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	if records, complete := renderer.PreviousRender(t.Context(), projected, spec.Named()); !complete || records == nil {
		t.Fatal("final projected binding missed accepted history")
	}
	if _, complete := renderer.PreviousRender(t.Context(), req, spec.Named()); complete {
		t.Fatal("displaced snapshot retained history")
	}
	original := pending.Value().(renderFrame)
	assert.Equal(t, original.binding.Dependencies.Equal(before.binding.Dependencies), true)
	assert.Equal(t, original.header.Binding, before.header.Binding)
	assert.Equal(t, original.raw, before.raw)
	assert.Equal(t, original.lookup, before.lookup)
	assert.Equal(t, original.acceptedKey, before.acceptedKey)
	assert.Equal(t, original.identity, before.identity)
	assert.Equal(t, original.accepted == before.accepted, true)
	assert.Equal(t, bytes.Equal(original.encoded, wire) && bytes.Equal(original.output, output), true)
	_, rebound, err := Render(t.Context(), renderer, projected, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	after := rebound.Value().(renderFrame)
	assert.Equal(t, after.header.Key, before.header.Key)
	assert.Equal(t, bytes.Equal(after.output, output), true)
	assert.Diff(t, after.header.Reads, before.header.Reads)
	index := filepath.Join(cacheDir, "oci", "index.json")
	stamp := time.Unix(1, 0)
	assert.NoError(t, os.Chtimes(index, stamp, stamp))
	acceptance.Receipt = rebound
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	info, err := os.Stat(index)
	assert.NoError(t, err)
	assert.Equal(t, info.ModTime().Equal(stamp), true)
}
