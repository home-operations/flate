package selection

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResult_WarningsPreserveStoredEvidence(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	s.AddWarning(manifest.Warning{Resource: id, Category: manifest.WarnInputParseFailed, Detail: []string{"token"}})
	before := s.Warnings()
	result := &Result{Warnings: before}
	assert.Equal(t, len(result.Warnings), 1)
	result.Warnings = nil
	assert.Diff(t, s.Warnings(), before)
}
