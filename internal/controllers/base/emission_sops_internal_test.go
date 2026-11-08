package base

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomizestate" // Standalone controller tests MUST install the process scope owner before parsing.
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestParseChildren_SkippedDiagnostics(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, task.NewBounded(2))
	c.Configure(Options{SkipKinds: []string{manifest.KindSecret}})
	owner := &manifest.HelmRelease{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	e := newExecution(c, owner.Named())
	defer e.Close()
	e.Reads.GetObject(owner.Named())
	docs := []manifest.DocOccurrence{{Document: map[string]any{
		"apiVersion": "v1", "kind": manifest.KindSecret,
		"metadata":   map[string]any{"name": "input"},
		"stringData": map[string]any{"VALUE": "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"},
	}}}
	result1 := e.ParseChildren(owner, docs, manifest.ParseDocOptions{})
	children, visible, failures := result1.Children, result1.Visible, result1.Failures
	assert.FatalIf(t, len(children) != 0 || len(visible) != 0 || len(failures) != 0 || len(docs[0].Warnings) != 1,

		"skipped outcome: children=%v visible=%v failures=%v diagnostics=%v",
		children,
		visible,
		failures,
		docs[0].Warnings)
	assert.FatalIf(t, len(s.Warnings()) != 0, "%s", "unaccepted diagnostics escaped")

	result2 := e.ParseChildren(owner, docs, manifest.ParseDocOptions{})
	failures = result2.Failures
	assert.Equal(t, len(failures), 0)
	e.Reads.Accept(store.AssessmentReconciled)
	warnings := s.Warnings()
	assert.FatalIf(t, len(warnings) != 1,
		"accepted diagnostics=%v", warnings)

	assert.Equal(
		t,
		warnings[0].Resource,
		manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"},
	)
	assert.Diff(t, warnings[0].Detail, []string{"VALUE"})
	assert.Equal(t, len(s.Warnings()), 1)
}
