package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
