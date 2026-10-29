package sourceref

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_SourceRef_SelfEntryNeverAcquires(t *testing.T) {
	loader := &rejectBootstrapTransport{}
	previous := client.Protocols["git"]
	client.InstallProtocol("git", server.NewClient(loader))
	t.Cleanup(func() { client.InstallProtocol("git", previous) })
	var roots [2]string
	for i, value := range []string{"before", "after"} {
		root := t.TempDir()
		roots[i] = root
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [config/cluster.yaml, cm.yaml]\n")
		testutil.WriteFile(t, root, "entry/config/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 1m
  url: git://bootstrap.invalid/unreachable
  ref: {branch: unavailable}
---
`+testutil.KustomizationYAML("cluster", "flux-system", "  interval: 1m\n  prune: true\n  path: ./entry\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
		testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: local-bytes, namespace: apps}\ndata: {value: "+value+"}\n")
	}
	for _, verb := range []string{"build", "test", "diff"} {
		t.Run(verb, func(t *testing.T) {
			args := []string{verb, "all", "--path", filepath.Join(roots[1], "entry"), "--concurrency", "2", "--cache-dir", t.TempDir()}
			if verb == "diff" {
				args = append(args, "--path-orig", filepath.Join(roots[0], "entry"), "-o", "diff")
			}
			before := loader.calls.Load()
			out, stderr, code := harness.RunCLIBuffers(args...)
			calls := loader.calls.Load() - before
			if code != 0 || calls != 0 {
				t.Fatalf("self-entry must use working bytes: exit=%d acquisitions=%d\nstdout:\n%s\nstderr:\n%s", code, calls, out, stderr)
			}
			switch verb {
			case "build":
				if !strings.Contains(out, "name: local-bytes") || !strings.Contains(out, "value: after") {
					t.Fatalf("missing working-tree product:\n%s", out)
				}
			case "test":
				if !strings.Contains(out, "passed") {
					t.Fatalf("missing successful test result:\n%s", out)
				}
			case "diff":
				assertSourceValueDiff(t, out, "before", "after")
			}
		})
	}
}

type rejectBootstrapTransport struct {
	calls atomic.Int64
}

func (l *rejectBootstrapTransport) Load(*transport.Endpoint) (storer.Storer, error) {
	l.calls.Add(1)
	return nil, transport.ErrRepositoryNotFound
}
