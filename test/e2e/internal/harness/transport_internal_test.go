package harness

import (
	"bytes"
	"io"
	"log" //nolint:depguard // Restoring slog.SetDefault also requires restoring the standard logger writer.
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport/client"

	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
)

//nolint:paralleltest // The fixture replaces process-wide HTTP and Git transports.
func TestTransport_DeclaredHTTPS(t *testing.T) {
	previousHTTP, previousGit := http.DefaultTransport, client.Protocols["https"]
	previousResolver := net.DefaultResolver
	t.Run("fixture", func(t *testing.T) {
		f := testfixture.Materialize(t, "entry-root", "literal-app")
		isolate(t, f)
		f.Responses = []testfixture.Response{
			{
				Host:   "fixture.invalid",
				Method: "GET",
				Path:   "/manifest",
				Status: http.StatusOK,
				Body:   "expect/literal-app.stdout",
			},
			{
				Host: "fixture.invalid", Method: "GET", Path: "/redirect",
				Status: http.StatusFound, Body: "expect/literal-app.stdout",
				Headers: map[string]string{"Location": "http://fixture.invalid/manifest"},
			},
		}
		serve(t, f)
		transport := source.NewHTTPTransport(source.TransportMaterial{})
		defer transport.CloseIdleConnections()
		for _, url := range []string{
			"https://fixture.invalid/manifest", "https://undeclared.invalid/manifest",
			"http://fixture.invalid/manifest", "https://fixture.invalid/redirect",
		} {
			assertFixtureRequest(t, transport, f, url)
		}
	})
	if http.DefaultTransport != previousHTTP || client.Protocols["https"] != previousGit ||
		net.DefaultResolver != previousResolver {
		t.Fatal("fixture leaked HTTP, DNS or Git protocol registration")
	}
}

//nolint:paralleltest // CLI invocations replace process-wide logging, environment and transports.
func TestTransport_CLIRejectsPlainHTTP(t *testing.T) {
	for _, tc := range []struct {
		name      string
		url       string
		wantError string
	}{
		{"non-loopback", "http://192.0.2.1:1/manifest.yaml", "fixture forbids plain HTTP and live endpoints"},
		{"loopback", "http://127.0.0.1:1/manifest.yaml", "connect: connection refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := Open(t, "entry-root", "literal-app")
			f := fixture.fixture
			f.Restore(t)
			testutil.WriteFile(t, f.Repo, "kubernetes/apps/default/app/app/kustomization.yaml",
				"resources:\n  - "+tc.url+"\n")
			previous, writer := slog.Default(), log.Writer()
			t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer) })
			var stdout, stderr bytes.Buffer
			code := cli.Run([]string{
				"build", "ks", "app", "-p", filepath.Join(f.Repo, "kubernetes", "apps"), "-n", "default",
				"--no-progress", "--concurrency", "2",
			}, &stdout, &stderr)
			if code != 1 || !strings.Contains(stderr.String(), tc.wantError) {
				t.Fatalf(
					"unexpected plain HTTP result: code=%d stdout=%q stderr=%q",
					code,
					stdout.String(),
					stderr.String(),
				)
			}
		})
	}
}

func assertFixtureRequest(t *testing.T, transport *http.Transport, f *testfixture.Fixture, url string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if url != "https://fixture.invalid/manifest" {
		if err == nil {
			response.Body.Close()
			t.Fatalf("undeclared transport admitted %s", url)
		}
		if strings.Contains(url, "/redirect") || strings.HasPrefix(url, "http://") {
			if !strings.Contains(err.Error(), "fixture forbids plain HTTP and live endpoints") {
				t.Fatalf("plain HTTP escaped fixture denial: %v", err)
			}
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != f.Read(t, "expect/literal-app.stdout", "") || response.StatusCode != http.StatusOK {
		t.Fatalf("declared response changed: status=%d body=%q", response.StatusCode, body)
	}
}
