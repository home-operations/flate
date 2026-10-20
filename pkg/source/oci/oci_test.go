package oci

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"helm.sh/helm/v4/pkg/registry"
)

func TestPickSemverTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		tags    []string
		expr    string
		filter  string
		media   string
		want    string
		wantErr string
	}{
		{
			name: "highest in range",
			tags: []string{"1.2.3", "1.3.0", "1.4.0-rc1", "2.0.0", "garbage"},
			expr: ">=1.0.0 <2.0.0",
			want: "1.3.0",
		},
		{
			// GC-847-R1: 1.20.x resolves to the highest 1.20.* tag.
			name: "wildcard selects highest patch in minor",
			tags: []string{"1.20.0", "1.20.5", "1.21.0"},
			expr: "1.20.x",
			want: "1.20.5",
		},
		{
			// GC-847-R1: ~1.20.0 == >=1.20.0 <1.21.0.
			name: "tilde selects highest patch in minor",
			tags: []string{"1.20.0", "1.20.5", "1.21.0"},
			expr: "~1.20.0",
			want: "1.20.5",
		},
		{
			// GC-847-R1: ^1.20.0 == >=1.20.0 <2.0.0.
			name: "caret selects highest minor in major",
			tags: []string{"1.20.0", "1.20.5", "1.21.0"},
			expr: "^1.20.0",
			want: "1.21.0",
		},
		{
			name: "regex filter narrows candidate set",
			// Without filter the highest 1.x semver wins. With filter only
			// rc-prereleased entries qualify; semver treats prereleases as
			// less than non-prereleases of the same version, so within the
			// filtered set the highest prerelease wins.
			tags:   []string{"1.2.0-rc1", "1.4.0-rc1", "2.0.0", "1.5.0"},
			expr:   ">=1.0.0-0 <2.0.0",
			filter: "-rc",
			want:   "1.4.0-rc1",
		},
		{
			name: "non-semver tags ignored",
			tags: []string{"latest", "main", "1.0.0", "foo-bar"},
			expr: ">=0.0.1",
			want: "1.0.0",
		},
		{
			name:  "helm chart tags map underscore build metadata",
			tags:  []string{"1.0.0_build.1", "1.0.0_build.2", "1.0.1"},
			expr:  ">=1.0.0 <1.0.1",
			media: registry.ChartLayerMediaType,
			want:  "1.0.0_build.1",
		},
		{
			name:  "helm chart ties preserve registry order",
			tags:  []string{"1.0.0_build.2", "1.0.0_build.1"},
			expr:  ">=1.0.0",
			media: registry.ChartLayerMediaType,
			want:  "1.0.0_build.2",
		},
		{
			name: "other media retains build metadata",
			tags: []string{"1.0.0+build.1", "1.0.0_build.2"},
			expr: ">=1.0.0",
			want: "1.0.0+build.1",
		},
		{
			name:    "no match returns error",
			tags:    []string{"1.0.0", "1.1.0"},
			expr:    ">=2.0.0",
			wantErr: "no tag matched",
		},
		{
			name:    "invalid constraint returns error",
			tags:    []string{"1.0.0"},
			expr:    "not-a-constraint",
			wantErr: "semver",
		},
		{
			name:    "invalid filter regex returns error",
			tags:    []string{"1.0.0"},
			expr:    ">=0.0.1",
			filter:  "[invalid",
			wantErr: "semverFilter",
		},
		{
			name: "empty tag list returns error",
			tags: nil,
			expr: ">=0.0.1",
			// Constraint is valid but matches nothing.
			wantErr: "no tag matched",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := pickSemverTag(tc.tags, tc.expr, tc.filter, tc.media)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseOCIRef(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"strips oci scheme", "oci://ghcr.io/owner/chart", "ghcr.io/owner/chart"},
		{"strips tag", "oci://ghcr.io/owner/chart:v1.2.3", "ghcr.io/owner/chart"},
		{"strips digest", "oci://ghcr.io/owner/chart@sha256:abc123", "ghcr.io/owner/chart"},
		{"preserves port without tag", "oci://registry:5000/x", "registry:5000/x"},
		{"preserves port with tag", "oci://registry:5000/x:v1", "registry:5000/x"},
		{"preserves port with digest", "oci://registry:5000/x@sha256:abc", "registry:5000/x"},
		{"no scheme passes through", "ghcr.io/owner/chart:v1", "ghcr.io/owner/chart"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseOCIRef(tc.in)
			if err != nil {
				t.Fatalf("parseOCIRef(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseOCIRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestOCIRevision(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		ref    manifest.OCIRepositoryRef
		digest string
		want   string
	}{
		{
			name:   "tag and digest joined with @",
			ref:    manifest.OCIRepositoryRef{Tag: "v1.2.3"},
			digest: "sha256:abc",
			want:   "v1.2.3@sha256:abc",
		},
		{
			name:   "tag only when no digest",
			ref:    manifest.OCIRepositoryRef{Tag: "v1.2.3"},
			digest: "",
			want:   "v1.2.3",
		},
		{
			name:   "digest only when no tag",
			ref:    manifest.OCIRepositoryRef{Digest: "sha256:abc"},
			digest: "sha256:abc",
			want:   "sha256:abc",
		},
		{
			name:   "empty ref falls back to latest tag",
			ref:    manifest.OCIRepositoryRef{},
			digest: "sha256:abc",
			want:   "latest@sha256:abc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ociRevision(tc.ref, tc.digest)
			if got != tc.want {
				t.Errorf("ociRevision(%+v, %q) = %q, want %q", tc.ref, tc.digest, got, tc.want)
			}
		})
	}
}

func TestVersionTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ref  manifest.OCIRepositoryRef
		want string
	}{
		{"digest wins", manifest.OCIRepositoryRef{Digest: "sha256:abc", Tag: "v1"}, "sha256:abc"},
		{"semver wins over tag", manifest.OCIRepositoryRef{Tag: "v1", SemVer: ">=1.0"}, ">=1.0"},
		{"tag when no digest/semver", manifest.OCIRepositoryRef{Tag: "v1"}, "v1"},
		{"empty returns empty", manifest.OCIRepositoryRef{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := versionTag(tc.ref)
			if got != tc.want {
				t.Errorf("versionTag(%+v) = %q, want %q", tc.ref, got, tc.want)
			}
		})
	}
}

func TestResolveRef_SemverPrecedence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ref  manifest.OCIRepositoryRef
		want manifest.OCIRepositoryRef
		list bool
	}{
		{"semver only resolves", manifest.OCIRepositoryRef{SemVer: ">=1.0"}, manifest.OCIRepositoryRef{Tag: "1.2.0"}, true},
		{"semver wins over tag", manifest.OCIRepositoryRef{SemVer: ">=1.0", Tag: "1.0.0"}, manifest.OCIRepositoryRef{Tag: "1.2.0"}, true},
		{"digest suppresses semver", manifest.OCIRepositoryRef{Digest: "sha256:abc", SemVer: ">=1.0"}, manifest.OCIRepositoryRef{Digest: "sha256:abc", SemVer: ">=1.0"}, false},
		{"tag only does not resolve", manifest.OCIRepositoryRef{Tag: "1.0.0"}, manifest.OCIRepositoryRef{Tag: "1.0.0"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var listed atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !tc.list || r.URL.Path != "/v2/chart/tags/list" {
					t.Errorf("unexpected registry request: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				listed.Store(true)
				_, _ = w.Write([]byte(`{"name":"chart","tags":["1.0.0","1.2.0"]}`))
			}))
			t.Cleanup(srv.Close)
			repo := &manifest.OCIRepository{URL: "oci://" + strings.TrimPrefix(srv.URL, "http://") + "/chart"}
			client, _, err := (&Fetcher{Network: newTestNetwork(t)}).newRepoClient(t.Context(), repo, nil)
			if err != nil {
				t.Fatal(err)
			}
			client.PlainHTTP = true
			got, err := resolveRef(t.Context(), client, repo, tc.ref)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || listed.Load() != tc.list {
				t.Errorf("resolveRef = %+v, listed = %t; want %+v, %t", got, listed.Load(), tc.want, tc.list)
			}
		})
	}
}
