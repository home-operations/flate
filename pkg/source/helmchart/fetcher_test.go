package helmchart

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func ociRepo(url string) *manifest.HelmRepository {
	return &manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		URL: url, Type: sourcev1.HelmRepositoryTypeOCI,
	}
}

func helmChart(repoName, chart, version string) *manifest.HelmChartSource {
	return &manifest.HelmChartSource{
		Name: repoName + "-" + chart, Namespace: "flux-system",
		Chart:     chart,
		Version:   version,
		SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repoName},
	}
}

// stubOCI records the OCIRepository handed to it and returns a canned result.
type stubOCI struct {
	got    *manifest.OCIRepository
	policy cache.Policy
	reads  *store.ReadSet
	art    *store.SourceArtifact
	err    error
}

func (s *stubOCI) FetchChart(_ context.Context, r *manifest.OCIRepository, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	s.got = r
	s.reads = reads
	s.policy = policy
	return s.art, s.err
}

func TestFetch_OCIBranch(t *testing.T) {
	t.Parallel()
	repo := ociRepo("oci://oci.trueforge.org/truecharts")
	repo.Interval = metav1.Duration{Duration: time.Hour}
	stub := &stubOCI{art: &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: "/slot", Content: "acquired-generation"}}
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	f := &Fetcher{network: newTestNetwork(t), oci: stub}

	retry := cache.RetryConfig{Attempts: 3}
	hc := helmChart("truecharts", "kromgo", "3.0.0")
	reads := fInputs.readSet(hc)
	art, err := f.Fetch(t.Context(), hc, reads, cache.Policy{MaxAge: time.Minute, Retry: retry})
	if stub.reads != reads {
		t.Fatal("nested OCI fetch did not receive execution inputs")
	}
	if stub.policy.MaxAge != time.Hour || stub.policy.Retry != retry || stub.policy.Timeout != nil || stub.policy.Admit != nil {
		t.Fatalf("nested policy=%+v, want %+v", stub.policy, retry)
	}
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if stub.got == nil {
		t.Fatal("OCI fetcher was not invoked")
	}
	if stub.got.URL != "oci://oci.trueforge.org/truecharts/kromgo" {
		t.Errorf("synthesized OCIRepository URL = %q", stub.got.URL)
	}
	if stub.got.Reference == nil || stub.got.Reference.Tag != "3.0.0" {
		t.Errorf("synthesized ref = %+v, want tag 3.0.0", stub.got.Reference)
	}
	if art.Content != stub.art.Content {
		t.Fatal("OCI forwarding changed content identity")
	}
	if art.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want re-stamped HelmChart", art.Kind)
	}
}

func TestFetch_OCIAmbientContinuation(t *testing.T) {
	t.Parallel()
	for _, failure := range []error{nil, context.Canceled, context.DeadlineExceeded} {
		name := "success"
		if failure != nil {
			name = failure.Error()
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			repo := ociRepo("oci://fixture/charts")
			hc := helmChart(repo.Name, "chart", "1.0.0")
			missing, _ := errors.AsType[*source.MissingSecretError](source.MissingSecretErr(repo.Named(), "auth", "not found"))
			calls := 0
			missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
				calls++
				if failure != nil {
					return nil, failure
				}
				return &store.SourceArtifact{Kind: manifest.KindOCIRepository, Content: "chart"}, nil
			}
			f := &Fetcher{oci: &stubOCI{err: missing}}
			reads := (testInputs{repos: func(_, _ string) *manifest.HelmRepository { return repo }}).readSet(hc)
			defer reads.Close()
			_, err := f.Fetch(t.Context(), hc, reads, cache.Policy{})
			wrapped, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || wrapped.RetryAmbient == nil || calls != 0 {
				t.Fatalf("prepare = %v; calls=%d", err, calls)
			}
			artifact, err := wrapped.RetryAmbient(t.Context())
			if !errors.Is(err, failure) || calls != 1 {
				t.Fatalf("ambient continuation = %v; calls=%d", err, calls)
			}
			if failure != nil && artifact != nil || failure == nil && (artifact == nil || artifact.Kind != manifest.KindHelmChart || artifact.Content != "chart") {
				t.Fatalf("ambient chart artifact = %+v", artifact)
			}
		})
	}
}

func TestFetch_RepoNotFound(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return nil }}
	f := &Fetcher{network: newTestNetwork(t)}
	_, err := f.Fetch(context.Background(), helmChart("missing", "kromgo", "3.0.0"), fInputs.readSet(helmChart("missing", "kromgo", "3.0.0")), cache.Policy{})
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("err = %v, want ErrObjectNotFound", err)
	}
}

func TestFetch_OCIExactVersionForever(t *testing.T) {
	t.Parallel()
	repo := ociRepo("oci://example.test/charts")
	stub := &stubOCI{art: &store.SourceArtifact{Kind: manifest.KindOCIRepository}}
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	f := &Fetcher{network: newTestNetwork(t), oci: stub, disableChartDigestTracking: true}
	if _, err := f.Fetch(t.Context(), helmChart(repo.Name, "chart", "1.0.0"), fInputs.readSet(helmChart(repo.Name, "chart", "1.0.0")), cache.Policy{}); err != nil {
		t.Fatal(err)
	}
	if stub.policy.MaxAge != cache.Forever || stub.got.Interval == nil || *stub.got.Interval != repo.Interval {
		t.Fatalf("policy=%+v interval=%v, want Forever and authored interval", stub.policy, stub.got.Interval.Duration)
	}
}
