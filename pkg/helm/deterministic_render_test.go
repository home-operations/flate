package helm

import (
	"context"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestTemplate_DeterministicNow is the Tier 1 integration guard: a chart
// that templates `now` must render byte-identically across two UNCACHED
// renders. sprig registers `now` as time.Now, so with nanosecond precision
// two renders would differ run to run; the cfg.CustomTemplateFuncs override
// pins it to deterministic.FixedTime. Caching is disabled (zero-value
// ClientOptions) so byte-identity can only come from the override, not a
// cache hit — and the rendered value must reflect the fixed 2020 clock.
func TestTemplate_DeterministicNow(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	// Nanosecond-precision format: an un-overridden time.Now would reliably
	// differ between the two renders, so this genuinely guards the override
	// rather than coincidentally passing at second precision.
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  stamp: {{ now | date \"20060102150405.000000000\" | quote }}\n")

	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientWithOptions: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo",
			RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}

	first, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	second, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if first != second {
		t.Errorf("uncached renders differ — `now` not deterministic:\n first=%q\nsecond=%q", first, second)
	}
	if !strings.Contains(first, "20200101000000") {
		t.Errorf("rendered stamp not pinned to FixedTime (2020-01-01):\n%s", first)
	}
}

// TestTemplate_DeterministicRandom is the Tier 2 integration guard: a chart
// that templates randAlphaNum/uuidv4 — plus a sha256 over a random value, the
// shape behind checksum/secret annotations — must render byte-identically
// across two UNCACHED renders. sprig draws these from crypto/rand; the
// override seeds them from the release identity. Caching is disabled so
// byte-identity can only come from the seeded stream.
func TestTemplate_DeterministicRandom(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\n  annotations:\n    checksum/secret: {{ randAlphaNum 24 | sha256sum | quote }}\ndata:\n  token: {{ randAlphaNum 24 | quote }}\n  id: {{ uuidv4 | quote }}\n")

	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientWithOptions: %v", err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "chart-repo",
			RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}

	first, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	second, err := cli.Template(context.Background(), hr, nil, Options{})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if first != second {
		t.Errorf("uncached renders differ — random funcs not deterministic:\n first=%q\nsecond=%q", first, second)
	}
}
