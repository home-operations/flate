package helmchart

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
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

func (s *stubOCI) Fetch(_ context.Context, r *manifest.OCIRepository, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	s.got = r
	s.reads = reads
	s.policy = policy
	return s.art, s.err
}

func TestSynthesizeOCIRepository(t *testing.T) {
	t.Parallel()
	r := &manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		URL: "oci://oci.trueforge.org/truecharts/", Type: sourcev1.HelmRepositoryTypeOCI,
	}
	syn := synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "3.0.0"))
	if syn.URL != "oci://oci.trueforge.org/truecharts/kromgo" {
		t.Errorf("URL = %q (trailing slash should be normalized)", syn.URL)
	}
	if syn.Namespace != "flux-system" {
		t.Errorf("Namespace = %q", syn.Namespace)
	}
	if syn.Reference == nil || syn.Reference.Tag != "3.0.0" || syn.Reference.Digest != "" {
		t.Errorf("Reference = %+v, want tag 3.0.0", syn.Reference)
	}
	if d := synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "sha256:abc")); d.Reference == nil || d.Reference.Digest != "sha256:abc" || d.Reference.Tag != "" {
		t.Errorf("digest Reference = %+v", d.Reference)
	}
	if c := synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "1.20.x")); c.Reference == nil || c.Reference.SemVer != "1.20.x" || c.Reference.Tag != "" || c.Reference.Digest != "" {
		t.Errorf("constraint Reference = %+v, want SemVer 1.20.x", c.Reference)
	}
	if v := synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "")); v.Reference != nil {
		t.Errorf("empty version → Reference = %+v, want nil", v.Reference)
	}
	if synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "1.0.0")).Name == synthesizeOCIRepository(r, helmChart(r.Name, "kromgo", "2.0.0")).Name {
		t.Error("distinct versions collided on synthetic name")
	}
}

func TestVersionToOCIRef(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		version    string
		wantTag    string
		wantSemVer string
		wantDigest string
	}{
		// Semver constraints route to SemVer (GC-847-R1).
		{"wildcard x", "1.20.x", "", "1.20.x", ""},
		{"wildcard X", "1.20.X", "", "1.20.X", ""},
		{"star", "*", "", "*", ""},
		{"tilde", "~1.20.0", "", "~1.20.0", ""},
		{"caret", "^1.20.0", "", "^1.20.0", ""},
		{"range", ">=1.20.0 <1.21.0", "", ">=1.20.0 <1.21.0", ""},
		// Exact pins stay literal tags (GC-847-R2).
		{"exact pin", "1.20.0", "1.20.0", "", ""},
		{"v-prefixed pin", "v1.20.0", "v1.20.0", "", ""},
		// Bare partials parse as concrete versions and must stay tags: the
		// Exact versions MUST be classified before constraints.
		{"bare minor partial", "1.20", "1.20", "", ""},
		{"bare major partial", "1", "1", "", ""},
		// Digest stays a digest (GC-847-R3).
		{"digest", "sha256:abc", "", "", "sha256:abc"},
		// Non-semver literal tags stay tags (GC-847-R4).
		{"literal tag", "latest", "latest", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ref := versionToOCIRef(helmChart("repo", "chart", tc.version))
			if ref == nil {
				t.Fatalf("versionToOCIRef(%q) = nil", tc.version)
			}
			if ref.Tag != tc.wantTag || ref.SemVer != tc.wantSemVer || ref.Digest != tc.wantDigest {
				t.Errorf("versionToOCIRef(%q) = {Tag:%q SemVer:%q Digest:%q}, want {Tag:%q SemVer:%q Digest:%q}",
					tc.version, ref.Tag, ref.SemVer, ref.Digest, tc.wantTag, tc.wantSemVer, tc.wantDigest)
			}
		})
	}
}

func TestFetch_OCIBranch(t *testing.T) {
	t.Parallel()
	repo := ociRepo("oci://oci.trueforge.org/truecharts")
	repo.Interval = metav1.Duration{Duration: time.Hour}
	stub := &stubOCI{art: &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: "/slot"}}
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	f := &Fetcher{network: newTestNetwork(t), oci: stub}

	retry := cache.RetryConfig{Attempts: 3}
	hc := helmChart("truecharts", "kromgo", "3.0.0")
	reads := fInputs.readSet(hc)
	art, err := f.Fetch(t.Context(), hc, reads, cache.Policy{MaxAge: time.Minute, Retry: retry})
	if stub.reads != reads {
		t.Fatal("nested OCI fetch did not receive execution inputs")
	}
	if stub.policy != (cache.Policy{MaxAge: time.Hour, Retry: retry}) {
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
	if art.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want re-stamped HelmChart", art.Kind)
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
	if stub.policy.MaxAge != cache.Forever || stub.got.Interval != repo.Interval {
		t.Fatalf("policy=%+v interval=%v, want Forever and authored interval", stub.policy, stub.got.Interval.Duration)
	}
}

func TestSyntheticChartName_PersistedIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, repo, chart, url, version, want string }{
		{"normal", "repo", "chart", "https://charts.example/chart.tgz", "1.2.3", "repo-chart-c09ce75"},
		{"digest", "repo", "chart", "https://charts.example/chart.tgz", "sha256:abc", "repo-chart-293cc48"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := syntheticChartName(tc.repo, tc.chart, tc.url, tc.version); got != tc.want {
				t.Fatalf("name = %q, want %q", got, tc.want)
			}
		})
	}
}
