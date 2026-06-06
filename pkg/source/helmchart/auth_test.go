package helmchart

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// tlsPEMFiles returns the helm-tls-*.pem files the fetcher materialized
// under its tmpDir.
func tlsPEMFiles(t *testing.T, tmpDir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(tmpDir, "helm-tls-*.pem"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

// TestHelmRepoTLSOptions_MaterializesPresentKeys checks that
// helmRepoTLSOptions writes exactly the present TLS keys to temp files
// (the empty-content keys produce no file), and that cleanup removes
// them — exercising the source.TempFiles dedup through its helm caller.
func TestHelmRepoTLSOptions_MaterializesPresentKeys(t *testing.T) {
	cases := []struct {
		name      string
		data      map[string]any
		wantFiles int
	}{
		{"all-three", map[string]any{"tls.crt": "CRT", "tls.key": "KEY", "ca.crt": "CA"}, 3},
		{"cert-and-key", map[string]any{"tls.crt": "CRT", "tls.key": "KEY"}, 2},
		{"ca-only", map[string]any{"ca.crt": "CA"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httpRepo("https://charts.example")
			r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			data := tc.data
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: data}
			})

			opts, cleanup, err := f.helmRepoTLSOptions(r)
			if err != nil {
				t.Fatalf("helmRepoTLSOptions: %v", err)
			}
			if len(opts) != 1 {
				t.Fatalf("opts = %d, want 1 (WithTLSClientConfig)", len(opts))
			}
			files := tlsPEMFiles(t, f.tmpDir)
			if len(files) != tc.wantFiles {
				t.Fatalf("materialized %d PEM file(s), want %d", len(files), tc.wantFiles)
			}
			cleanup()
			if remaining := tlsPEMFiles(t, f.tmpDir); len(remaining) != 0 {
				t.Fatalf("cleanup left %d PEM file(s)", len(remaining))
			}
		})
	}
}

// TestHelmRepoTLSOptions_NoneFailsLoud pins that a certSecretRef Secret
// carrying none of tls.crt/tls.key/ca.crt is malformed config and fails
// loud (no ErrMissingSecret wrap, no files left behind).
func TestHelmRepoTLSOptions_NoneFailsLoud(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"unrelated": "x"}}
	})

	opts, cleanup, err := f.helmRepoTLSOptions(r)
	defer cleanup()
	if err == nil {
		t.Fatal("expected a loud error for a TLS secret with none of the keys")
	}
	if opts != nil {
		t.Fatalf("opts = %v, want nil on error", opts)
	}
	if files := tlsPEMFiles(t, f.tmpDir); len(files) != 0 {
		t.Fatalf("error path left %d PEM file(s) behind", len(files))
	}
}
