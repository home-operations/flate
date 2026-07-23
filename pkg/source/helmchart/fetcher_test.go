package helmchart

import (
	"context"
	"errors"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func ociRepo(url string) *manifest.HelmRepository {
	return &manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: url, Type: manifest.RepoTypeOCI},
	}
}

func helmChart(repoName, chart, version string) *manifest.HelmChartSource {
	return &manifest.HelmChartSource{
		Name: repoName + "-" + chart, Namespace: "flux-system",
		HelmChartSpec: sourcev1.HelmChartSpec{
			Chart:     chart,
			Version:   version,
			SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repoName},
		},
	}
}

// stubOCI records the OCIRepository handed to it and returns a canned result.
type stubOCI struct {
	got *manifest.OCIRepository
	art *store.SourceArtifact
	err error
}

func (s *stubOCI) Fetch(_ context.Context, r *manifest.OCIRepository) (*store.SourceArtifact, error) {
	s.got = r
	return s.art, s.err
}

func TestIsOCIHelmRepo(t *testing.T) {
	cases := []struct {
		name string
		repo *manifest.HelmRepository
		want bool
	}{
		{"type oci", &manifest.HelmRepository{HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "https://x", Type: manifest.RepoTypeOCI}}, true},
		{"oci:// url no type", &manifest.HelmRepository{HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "oci://reg/x"}}, true},
		{"http default", &manifest.HelmRepository{HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "https://charts.example"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOCIHelmRepo(tc.repo); got != tc.want {
				t.Errorf("isOCIHelmRepo = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSynthesizeOCIRepository(t *testing.T) {
	r := &manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "oci://oci.trueforge.org/truecharts/", Type: manifest.RepoTypeOCI},
	}
	syn := synthesizeOCIRepository(r, "kromgo", "3.0.0")
	if syn.URL != "oci://oci.trueforge.org/truecharts/kromgo" {
		t.Errorf("URL = %q (trailing slash should be normalized)", syn.URL)
	}
	if syn.Namespace != "flux-system" {
		t.Errorf("Namespace = %q", syn.Namespace)
	}
	if syn.Reference == nil || syn.Reference.Tag != "3.0.0" || syn.Reference.Digest != "" {
		t.Errorf("Reference = %+v, want tag 3.0.0", syn.Reference)
	}
	// digest version → digest ref
	if d := synthesizeOCIRepository(r, "kromgo", "sha256:abc"); d.Reference == nil || d.Reference.Digest != "sha256:abc" || d.Reference.Tag != "" {
		t.Errorf("digest Reference = %+v", d.Reference)
	}
	// semver constraint → SemVer ref (routed through versionToOCIRef)
	if c := synthesizeOCIRepository(r, "kromgo", "1.20.x"); c.Reference == nil || c.Reference.SemVer != "1.20.x" || c.Reference.Tag != "" || c.Reference.Digest != "" {
		t.Errorf("constraint Reference = %+v, want SemVer 1.20.x", c.Reference)
	}
	// empty version → no ref
	if v := synthesizeOCIRepository(r, "kromgo", ""); v.Reference != nil {
		t.Errorf("empty version → Reference = %+v, want nil", v.Reference)
	}
	// distinct versions → distinct stable names
	if synthesizeOCIRepository(r, "kromgo", "1.0.0").Name == synthesizeOCIRepository(r, "kromgo", "2.0.0").Name {
		t.Error("distinct versions collided on synthetic name")
	}
}

func TestVersionToOCIRef(t *testing.T) {
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
		// canary for the isExactSemver-before-isSemverConstraint ordering.
		{"bare minor partial", "1.20", "1.20", "", ""},
		{"bare major partial", "1", "1", "", ""},
		// Digest stays a digest (GC-847-R3).
		{"digest", "sha256:abc", "", "", "sha256:abc"},
		// Non-semver literal tags stay tags (GC-847-R4).
		{"literal tag", "latest", "latest", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref := versionToOCIRef(tc.version)
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
	repo := ociRepo("oci://oci.trueforge.org/truecharts")
	stub := &stubOCI{art: &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: "/slot"}}
	f := &Fetcher{repos: func(_, _ string) *manifest.HelmRepository { return repo }, oci: stub}

	art, err := f.Fetch(context.Background(), helmChart("truecharts", "kromgo", "3.0.0"))
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
	f := &Fetcher{repos: func(_, _ string) *manifest.HelmRepository { return nil }}
	_, err := f.Fetch(context.Background(), helmChart("missing", "kromgo", "3.0.0"))
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("err = %v, want ErrObjectNotFound", err)
	}
}
