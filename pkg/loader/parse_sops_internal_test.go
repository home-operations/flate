package loader

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepare_UnreadableBeforeValidation(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	input := &manifest.Secret{
		Name:       "input",
		Namespace:  "apps",
		Data:       map[string]any{"SECRET": cipher},
		StringData: map[string]any{"EMPTY": ""},
	}
	doc := testutil.ObjectDocument(input)
	child, outcome, _ := Prepare(
		manifest.DocOccurrence{Document: doc},
		manifest.ParseDocOptions{RequiredIdentity: true},
	)
	assert.NoError(t, outcome.Err)
	s := store.New()
	s.AddDeclaration(
		store.Declaration{
			Obj:      child.Obj,
			Doc:      child.Doc,
			Origin:   manifest.SuppliedDeclaration,
			Warnings: child.Warnings,
		},
	)
	data := s.ReferenceFacts(input.Named(), nil, manifest.NamedResource{}).Data
	result1, err := data.Value("SECRET")
	present := result1.Found
	assert.NoError(t, err)
	assert.Equal(t, present, false)
	result2, err := data.Value("EMPTY")
	empty, present := result2.Value, result2.Found
	assert.NoError(t, err)
	assert.Equal(t, present, true)
	assert.Equal(t, empty, "")
	assert.Equal(t, input.Data["SECRET"], any(cipher))
	assert.Equal(t, doc["data"].(map[string]any)["SECRET"], any(cipher))
	warnings := s.Warnings()
	assert.Equal(t, len(warnings), 1)
	assert.Equal(t, warnings[0].Category, manifest.WarnSOPSUnreadable)
	assert.Diff(t, warnings[0].Detail, []string{"SECRET"})
}

func TestInputCache_SOPSWarningHydration(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"Secret", "ConfigMap"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			if !owner.CanPersistRenders() {
				t.Fatal("input cache witness requires a clean stamped test binary")
			}
			files := fstest.MapFS{
				"object.yaml": {Data: []byte(fmt.Sprintf(
					"apiVersion: v1\nkind: %s\nmetadata: {name: input, namespace: apps}\ndata:\n  SECRET: ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]\n  PLAIN: cGxhaW4=\n",
					kind,
				))},
			}
			var output string
			var warnings []manifest.Warning
			for _, regime := range []string{"cold", "warm"} {
				l := cachedFixtureLoader(t, root, files, owner)
				saved := l.documents.cache[filepath.Join(root, "object.yaml")].objects
				assert.Equal(t, len(saved), 1)
				assert.FatalIf(t, saved[0].payload == nil, "%s: hydration requires a saved payload", regime)
				assert.Equal(t, len(saved[0].Warnings), 1)
				assert.Equal(t, saved[0].Warnings[0].Category, manifest.WarnSOPSUnreadable)
				assert.Diff(t, saved[0].Warnings[0].Detail, []string{"SECRET"})
				occurrence, known := l.Occurrence(manifest.NamedResource{Kind: kind, Name: "input", Namespace: "apps"})
				assert.Equal(t, known, true)
				child, err := l.OccurrenceObject(t.Context(), &occurrence, nil)
				assert.NoError(t, err)
				assert.Diff(t, child.Warnings, saved[0].Warnings)
				data := child.Doc["data"].(map[string]any)
				_, present := data["SECRET"]
				assert.Equal(t, present, false)
				assert.Equal(t, data["PLAIN"], any("cGxhaW4="))
				encoded, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: child.Doc}})
				assert.NoError(t, err)
				if regime == "cold" {
					output, warnings = string(encoded), child.Warnings
				} else {
					assert.Equal(t, string(encoded), output)
					assert.Diff(t, child.Warnings, warnings)
				}
				assert.NoError(t, l.PersistInputs(t.Context()))
				l.Close()
			}
		})
	}
}
