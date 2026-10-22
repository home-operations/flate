package depwait

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/schedule"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type artifactPromotion struct {
	promote func(manifest.NamedResource) bool
}

func (*artifactPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p *artifactPromotion) Promote(_ context.Context, _ *store.ReadSet, id manifest.NamedResource) bool {
	return p.promote(id)
}

func TestClassify_ArtifactRequirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                         string
		level                                                        manifest.DrainLevel
		content, descriptor, registry, promote, skipped, unknownRoot bool
		expr                                                         string
		status                                                       store.Status
		want                                                         schedule.Kind
		message                                                      string
		wantErr                                                      error
	}{
		{name: "ready without content", status: store.StatusReady, want: schedule.Blocked},
		{name: "unknown root before cascade", unknownRoot: true, want: schedule.Blocked},
		{name: "unknown root at cascade", unknownRoot: true, level: manifest.DrainCascade, want: schedule.DependencyFailed, message: "unknown OCI artifact root", wantErr: manifest.ErrInput},
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
			s := store.New()
			var source manifest.BaseManifest = &manifest.GitRepository{Name: "source", Namespace: "apps"}
			if tc.unknownRoot {
				source = &manifest.OCIRepository{Name: "source", Namespace: "apps"}
			}
			if tc.descriptor {
				source = &manifest.HelmChartSource{Name: "source", Namespace: "apps", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindGitRepository, Name: "git"}}
			}
			if tc.registry {
				source = &manifest.HelmRepository{Name: "source", Namespace: "apps", URL: "https://example.test/charts"}
			}
			publish := func() {
				s.AddObject(source)
				reason, message := store.StatusReason(""), ""
				status := tc.status
				if tc.promote {
					status = store.StatusReady
				}
				if tc.skipped {
					reason, message = store.ReasonSkipped, "credentials unavailable"
				}
				if status == store.StatusFailed {
					message = "fetch failed"
				}
				s.UpdateStatus(source.Named(), store.StatusInfo{Status: status, Reason: reason, Message: message})
				artifact := &store.SourceArtifact{}
				if tc.content {
					artifact.FS = fstest.MapFS{}
				}
				s.SetArtifact(source.Named(), artifact)
			}
			w := Waiter{Store: s}
			if tc.promote {
				w.Existence = &artifactPromotion{promote: func(id manifest.NamedResource) bool {
					if id != source.Named() {
						t.Fatalf("promotion=%s", id)
					}
					publish()
					return true
				}}
			} else if !tc.unknownRoot {
				publish()
			}
			got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: source.Named(), ReadyExpr: new(tc.expr), Artifact: true, UnknownArtifactRoot: tc.unknownRoot}, tc.level)
			wantErr := tc.wantErr
			if tc.skipped {
				wantErr = manifest.ErrSourceSkipped
			}
			message := got.Message
			kind := got.Kind
			if got.Err != nil {
				message, kind = got.Err.Error(), schedule.DependencyFailed
			}
			if kind != tc.want || !strings.Contains(message, tc.message) || !errors.Is(got.Err, wantErr) {
				t.Fatalf("classification=%+v, want kind %v, message %q, error %v", got, tc.want, tc.message, wantErr)
			}
		})
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
			got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: manifest.BootstrapSourceID, Artifact: artifact}, manifest.DrainCascade)
			want := schedule.DependencyFailed
			if artifact {
				want = schedule.Terminal
			}
			if got.Kind != want {
				t.Fatalf("classification=%+v, want kind %v", got, want)
			}
		})
	}
}
