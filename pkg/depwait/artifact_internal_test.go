package depwait

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/loader"

	"github.com/home-operations/flate/internal/assert"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

type artifactPromotion struct {
	promote func(manifest.NamedResource) bool
}

func (*artifactPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (p *artifactPromotion) Promote(
	_ context.Context,
	_ loader.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	return p.promote(id), nil
}

func TestClassify_ArtifactRequirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                    string
		level                                                   manifest.DrainLevel
		content, descriptor, registry, promote, skipped, absent bool
		expr                                                    string
		status                                                  store.Status
		want                                                    schedule.Kind
		message                                                 string
		wantErr                                                 error
	}{
		{name: "ready without content", status: store.StatusReady, want: schedule.Blocked},
		{name: "missing source before cascade", absent: true, want: schedule.Blocked},
		{name: "missing source at cascade", absent: true, level: manifest.DrainCascade, want: schedule.DependencyFailed, message: "dependency not found"},
		{name: "cascade without content", status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed, message: "artifact content unavailable", wantErr: manifest.ErrObjectNotFound},
		{name: "force never readmits missing content", status: store.StatusReady, level: manifest.DrainForce, want: schedule.DependencyFailed, message: "artifact content unavailable", wantErr: manifest.ErrObjectNotFound},
		{name: "content available", status: store.StatusReady, content: true, want: schedule.Terminal},
		{name: "CEL ready still requires content", status: store.StatusPending, expr: "true", want: schedule.Blocked},
		{name: "CEL error precedes content", status: store.StatusReady, expr: "!", level: manifest.DrainCascade, want: schedule.DependencyFailed, message: "readyExpr:"},
		{name: "promoted without content", promote: true, want: schedule.Blocked},
		{name: "promoted at cascade", promote: true, level: manifest.DrainCascade, want: schedule.DependencyFailed, message: "artifact content unavailable", wantErr: manifest.ErrObjectNotFound},
		{name: "descriptor", status: store.StatusReady, descriptor: true, want: schedule.Terminal},
		{name: "registry", status: store.StatusReady, registry: true, want: schedule.Terminal},
		{name: "source skipped", status: store.StatusReady, skipped: true, want: schedule.DependencyFailed, message: "source GitRepository.source.toolkit.fluxcd.io/apps/source skipped: credentials unavailable"},
		{name: "source failed", status: store.StatusFailed, want: schedule.DependencyFailed, message: "fetch failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testClassifyArtifactRequirementCase(t, &tc)
		},
		)
	}
}

func TestClassify_BootstrapArtifactRequirement(t *testing.T) {
	t.Parallel()
	for _, artifact := range []bool{false, true} {
		t.Run(fmt.Sprintf("artifact_%t", artifact), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: fstest.MapFS{}})
			w := Waiter{Store: s}
			got := w.Classify(
				t.Context(),
				manifest.DependencyRef{NamedResource: manifest.BootstrapSourceID, Artifact: artifact},
				manifest.DrainCascade,
			)
			want := schedule.DependencyFailed
			if artifact {
				want = schedule.Terminal
			}
			assert.FatalIf(t, got.Kind != want, "classification=%+v, want kind %v", got, want)
		})
	}
}

func TestClassify_SyntheticArtifactContext(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unavailable", "unreadable", "skipped"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
			repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
			st := store.New()
			st.AddObject(repo)
			status := store.StatusInfo{Status: store.StatusReady}
			if mode == "skipped" {
				status.Reason = store.ReasonSkipped
				status.Message = "material unavailable"
			}
			st.UpdateStatus(repo.Named(), status)
			waiter := Waiter{Store: st}
			result := waiter.Classify(
				t.Context(),
				manifest.DependencyRef{NamedResource: repo.Named(), Artifact: true, ArtifactOnly: mode == "unreadable"},
				manifest.DrainCascade,
			)
			sentinel := manifest.ErrObjectNotFound
			if mode == "unreadable" {
				sentinel = manifest.ErrInput
			}
			if mode == "skipped" {
				sentinel = manifest.ErrSourceSkipped
			}
			assert.FatalIf(
				t,
				!errors.Is(result.Err, sentinel) || !strings.Contains(result.Err.Error(), chart.Named().String()) ||
					strings.Contains(
						result.Err.Error(),
						repo.Named().String(),
					),
				"artifact context or sentinel lost: %+v",
				result,
			)
		})
	}
}

func testClassifyArtifactRequirementCase(t *testing.T, testCaseInput *struct {
	name string

	level      manifest.DrainLevel
	content    bool
	descriptor bool
	registry   bool
	promote    bool
	skipped    bool
	absent     bool
	expr       string
	status     store.Status
	want       schedule.Kind
	message    string
	wantErr    error
},
) {
	t.Helper()

	s := store.New()
	var source manifest.BaseManifest = &manifest.GitRepository{Name: "source", Namespace: "apps"}
	if testCaseInput.absent {
		source = &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	}
	if testCaseInput.descriptor {
		source = &manifest.HelmChartSource{
			Name:      "source",
			Namespace: "apps",
			SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindGitRepository, Name: "git"},
		}
	}
	if testCaseInput.registry {
		source = &manifest.HelmRepository{Name: "source", Namespace: "apps", URL: "https://example.test/charts"}
	}
	publish := func() {
		s.AddObject(source)
		reason, message := store.StatusReason(""), ""
		status := testCaseInput.status
		if testCaseInput.promote {
			status = store.StatusReady
		}
		if testCaseInput.skipped {
			reason, message = store.ReasonSkipped, "credentials unavailable"
		}
		if status == store.StatusFailed {
			message = "fetch failed"
		}
		s.UpdateStatus(source.Named(), store.StatusInfo{Status: status, Reason: reason, Message: message})
		artifact := &store.SourceArtifact{}
		if testCaseInput.content {
			artifact.FS = fstest.MapFS{}
		}
		s.SetArtifact(source.Named(), artifact)
	}
	w := Waiter{Store: s}
	if testCaseInput.promote {
		w.Existence = &artifactPromotion{promote: func(id manifest.NamedResource) bool {
			assert.FatalIf(t, id != source.Named(), "promotion=%s", id)
			publish()
			return true
		}}
	} else if !testCaseInput.absent {
		publish()
	}
	got := w.Classify(
		t.Context(),
		manifest.DependencyRef{
			NamedResource: source.Named(),
			ReadyExpr:     new(testCaseInput.expr),
			Artifact:      true,
		},
		testCaseInput.level,
	)
	wantErr := testCaseInput.wantErr
	if testCaseInput.skipped {
		wantErr = manifest.ErrSourceSkipped
	}
	message := got.Message
	kind := got.Kind
	if got.Err != nil {
		message, kind = got.Err.Error(), schedule.DependencyFailed
	}
	assert.FatalIf(t, kind != testCaseInput.want || !strings.Contains(message, testCaseInput.message) ||
		!errors.Is(got.Err, wantErr), "classification=%+v, want kind %v, message %q, error %v",
		got, testCaseInput.
			want, testCaseInput.
			message, wantErr,
	)
}
