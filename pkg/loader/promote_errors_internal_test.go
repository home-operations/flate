package loader

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
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
	docs := []store.Child{{Obj: first}, {Obj: target}, {Obj: last}}
	if (*testCaseInput) == "existing" {
		st.AddObject(target)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cause := manifest.NewError(manifest.ErrInput, nil, "exact admission failed")
	calls := 0
	admit := func(obj manifest.BaseManifest) (bool, error) {
		if obj.Named() != target.Named() {
			return true, nil
		}
		calls++
		if (*testCaseInput) == "cancel" {
			cancel()
			return true, nil
		}
		if (*testCaseInput) != "final" || calls == 2 {
			return false, cause
		}
		return true, nil
	}
	got, err := Promote(ctx, st, target.Named(), PromoteOptions{Documents: docs, Admit: admit})
	var want error = cause
	if (*testCaseInput) == "cancel" {
		want = context.Canceled
	}
	{
		assert.FatalIf(t, got || !errors.Is(err, want), "Promote = %v, %v, want false, %v", got, err, want)
	}
	{
		assert.FatalIf(
			t,
			(*testCaseInput) != "existing" && st.GetObject(first.Named()) == nil,
			"%s",
			"previously admitted independent object was rolled back",
		)
	}
	if (*testCaseInput) == "child" || (*testCaseInput) == "cancel" {
		{
			assert.FatalIf(
				t,
				st.GetObject(target.Named()) != nil || st.GetObject(last.Named()) != nil,
				"%s",
				"failure published the rejected or later sibling",
			)
		}
	}
	{
		assert.FatalIf(
			t,
			(*testCaseInput) == "final" && st.GetObject(target.Named()) == nil,
			"%s",
			"final admission error rolled back accepted publication",
		)
	}
}
