package store_test

import (
	"archive/tar"
	"context"
	"slices"
	"testing"
	"testing/fstest"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func emissionChild(name, value string) store.Child {
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "apps"},
		"data":       map[string]any{"value": value},
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parsed.Object, parsed.Document
	if err != nil {
		panic(err)
	}
	return store.Child{Obj: obj, Doc: authoritative}
}

func TestRecordEmitted_PayloadPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		reverse, persistent bool
	}{
		{name: "memory/a-first"},
		{name: "memory/z-first", reverse: true},
		{name: "persistent/a-first", persistent: true},
		{name: "persistent/z-first", persistent: true, reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedPayloadPrecedenceCase(t, &tc)
		},
		)
	}
}

func testRecordEmittedPayloadPrecedenceCase(t *testing.T, testCaseInput *struct {
	name       string
	reverse    bool
	persistent bool
},
) {
	t.Helper()

	s := store.New()
	a, z, zz := testutil.KustomizationID(
		"apps",
		"a",
	), testutil.KustomizationID(
		"apps",
		"z",
	), testutil.KustomizationID(
		"apps",
		"zz",
	)
	for _, id := range []manifest.NamedResource{a, z, zz} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	firstChild, lastChild, declaration := emissionChild(
		"generated",
		"a",
	), emissionChild(
		"generated",
		"z",
	), emissionChild(
		"generated",
		"zz",
	)
	target := declaration.Obj.Named()
	root := ""
	if testCaseInput.persistent {
		root = t.TempDir()
	}
	cache, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cache.Close()) })
	acquire := func(value string) *store.SourceArtifact {
		t.Helper()
		input, descriptor, key, acquireErr := cache.Acquire(
			t.Context(),
			sourcecache.ArchiveKey{Content: value, Policy: "fixed"},
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			func(_ context.Context, target sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				writer := tar.NewWriter(target)
				if addFSErr := writer.AddFS(fstest.MapFS{"file": {Data: []byte(value)}}); addFSErr != nil {
					return sourcecache.ProducerResult{}, addFSErr
				}
				return sourcecache.ProducerResult{}, writer.Close()
			},
		)
		assert.NoError(t, acquireErr)
		return &store.SourceArtifact{
			Kind:      target.Kind,
			URL:       "https://" + value + ".test",
			LocalPath: "/" + value,
			LocalRoot: "/root-" + value,
			Key:       key,
			Digest:    descriptor.Annotations["flate.tree-digest"],
			Revision:  "revision-" + value,
			Metadata:  map[string]string{"origin": value},
			FS:        input,
		}
	}
	first, last := acquire("a"), acquire("z")
	first.Size, last.Size, last.Provider = 1, 2, store.ProviderAlias
	peer := store.New()
	peer.AddObject(&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace})
	peer.RecordEmitted(
		a,
		&store.Emission{Children: []store.Child{firstChild}, Complete: true},
		map[manifest.NamedResource]*store.SourceArtifact{target: first},
	)
	owners, payloads := []manifest.NamedResource{a, z}, []*store.SourceArtifact{first, last}
	children := []store.Child{firstChild, lastChild}
	if testCaseInput.reverse {
		slices.Reverse(owners)
		slices.Reverse(payloads)
		slices.Reverse(children)
	}
	testRecordEmittedPayloadPrecedenceCaseCompletion(
		t,
		&testRecordEmittedPayloadPrecedenceCaseCompletionState{
			s:           &s,
			a:           &a,
			z:           &z,
			zz:          &zz,
			declaration: &declaration,
			target:      &target,
			err:         &err,
			first:       &first,
			last:        &last,
			peer:        &peer,
			owners:      &owners,
			payloads:    &payloads,
			children:    &children,
		},
	)
}

type testRecordEmittedPayloadPrecedenceCaseCompletionState struct {
	s           **store.Store
	a           *manifest.NamedResource
	z           *manifest.NamedResource
	zz          *manifest.NamedResource
	declaration *store.Child
	target      *manifest.NamedResource
	err         *error
	first       **store.SourceArtifact
	last        **store.SourceArtifact
	peer        **store.Store
	owners      *[]manifest.NamedResource
	payloads    *[]*store.SourceArtifact
	children    *[]store.Child
}
