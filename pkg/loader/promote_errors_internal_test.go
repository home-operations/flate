package loader

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestPromote_AdmissionErrors(t *testing.T) {
	t.Parallel()
	for _, site := range []string{"existing", "child", "final", "cancel"} {
		t.Run(site, func(t *testing.T) {
			t.Parallel()
			testPromoteAdmissionErrorsCase(t, &site)
		},
		)
	}
}

func testPromoteAdmissionErrorsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	first := &manifest.ConfigMap{Name: "first", Namespace: "apps"}
	target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
	last := &manifest.ConfigMap{Name: "last", Namespace: "apps"}
	if *testCaseInput == "existing" {
		st.AddObject(target)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if *testCaseInput != "existing" {
		got, err := promoteForTest(ctx, t, st, first.Named(), PromoteOptions{Documents: []store.Child{{Obj: first}}})
		assert.NoError(t, err)
		assert.Equal(t, got, true)
	}
	if *testCaseInput == "final" {
		got, err := promoteForTest(ctx, t, st, target.Named(), PromoteOptions{Documents: []store.Child{{Obj: target}}})
		assert.NoError(t, err)
		assert.Equal(t, got, true)
	}
	input := &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{
		"objects.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: [\n")},
	}}
	occurrence := selection.OccurrenceFacts{
		Declaration: selection.OccurrenceID{Source: input.LocalPath, File: "objects.yaml"},
		Identity:    target.Named(), Authored: target.Named(),
	}
	if *testCaseInput == "final" {
		occurrence.Identity, occurrence.Authored = last.Named(), last.Named()
	}
	want := manifest.ErrInput
	if *testCaseInput == "cancel" {
		cancel()
		want = context.Canceled
	}
	child, err := New(st).OccurrenceObject(ctx, &occurrence, input)
	got := child.Obj != nil
	assert.FatalIf(t, got || !errors.Is(err, want), "Promote = %v, %v, want false, %v", got, err, want)
	assert.FatalIf(
		t,
		(*testCaseInput) != "existing" && st.GetObject(first.Named()) == nil,
		"%s",
		"previously admitted independent object was rolled back",
	)
	if (*testCaseInput) == "child" || (*testCaseInput) == "cancel" {
		assert.FatalIf(
			t,
			st.GetObject(target.Named()) != nil || st.GetObject(last.Named()) != nil,
			"%s",
			"failure published the rejected or later sibling",
		)
	}
	assert.FatalIf(
		t,
		(*testCaseInput) == "final" && st.GetObject(target.Named()) == nil,
		"%s",
		"final admission error rolled back accepted publication",
	)
}
