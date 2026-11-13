package depwait

import (
	"errors"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/schedule"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/pkg/store"
)

func TestReadyExpr_ProjectionError(t *testing.T) {
	t.Parallel()
	docs, err := testutil.SplitDocs([]byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: dep, namespace: ns}
spec: {chartRef: {kind: OCIRepository, name: chart}, values: {version: old}}
`))
	assert.FatalIf(t, err != nil, "%v", err)
	parsed, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	broken := obj.(*manifest.HelmRelease).Clone()
	broken.HelmReleaseSpec.Values = &apix.JSON{Raw: []byte("{")}
	dep := &countedDocument{HelmRelease: broken}
	s := store.New()
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady})
	var reads store.ReadSet
	reads.Init(s, dep.Named(), nil)
	defer reads.Retire()
	captured, info := reads.Snapshot(dep.Named(), true)
	w := Waiter{
		Store: &reads, Parent: dep.Named(), Missing: NewMissingPolicy(
			nil),
	}
	for _, expr := range []string{`true`, `true || self.spec.values.version == dep.spec.values.version`} {
		{
			ok, err := w.evaluateReadyExpr(expr, dep.Named(), captured, info.Conditions)
			assert.FatalIf(t, err != nil || !ok, "lazy evaluation: ok=%v err=%v", ok, err)
		}
	}
	assert.FatalIf(t, dep.calls != 0, "%v", "short circuit converted an unused object")
	expr := `self.spec == dep.spec`
	if ok, err := w.evaluateReadyExpr(
		expr,
		dep.Named(),
		captured,
		info.Conditions,
	); ok ||
		!errors.Is(err, manifest.ErrInput) {
		t.Fatalf("projection failure: ok=%v err=%v, want input error", ok, err)
	} else {
		{
			_, terminal := errors.AsType[*celCompileError](
				err,
			)
			assert.FatalIf(t, terminal, "projection failure classified as compile error: %v", err)
		}
	}
	if dep.calls != 1 {
		t.Fatalf("same identity projected %d times, want 1", dep.calls)
	}
	ref := manifest.DependencyRef{NamedResource: dep.Named(), ReadyExpr: new(expr)}
	{
		tryReadyExprResult1 := w.tryReadyExpr(&ref, captured, info.Conditions)
		ready, failMsg := tryReadyExprResult1.ready, tryReadyExprResult1.failure
		assert.FatalIf(
			t,
			ready || failMsg != "",
			"projection failure readiness: ready=%v failMsg=%q",
			ready,
			failMsg,
		)
	}
	{
		got := w.Classify(t.Context(), ref, manifest.DrainNone)
		assert.FatalIf(t, got.Kind != schedule.Blocked, "projection failure classification: %+v", got)
	}
	assert.FatalIf(t, !reads.Valid(), "%v", "projection failure invalidated unchanged observations")
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusPending})
	assert.FatalIf(t, reads.Valid(), "%v", "projection failure lost captured condition observation")
}

func TestReadyExpr_AbsentSpec(t *testing.T) {
	t.Parallel()
	for _, statusOnly := range []bool{false, true} {
		t.Run(strconv.FormatBool(statusOnly), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			dep := testutil.KustomizationID("", "dep")
			if statusOnly {
				s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusPending})
			}
			{
				ok, err := evaluateReadyExpr(
					`!has(dep.spec) && !has(dep.metadata.labels) && dep.metadata.namespace == ''`,
					s,
					dep,
					dep,
				)
				assert.FatalIf(t, err != nil ||
					!ok, "absent desired fields: ok=%v err=%v", ok, err)
			}
			w := Waiter{
				Store: captureState(t, s), Parent: dep, Missing: NewMissingPolicy(
					nil),
			}
			ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: new(`has(dep.spec)`)}
			{
				got := w.Classify(t.Context(), ref, manifest.DrainNone)
				assert.FatalIf(t, got.Kind != schedule.Blocked, "unavailable at normal evaluation: %+v", got)
			}
			{
				got := w.Classify(t.Context(), ref, manifest.DrainCascade)
				assert.FatalIf(t, got.Kind != schedule.DependencyFailed, "unavailable at cascade: %+v", got)
			}
		})
	}
}
