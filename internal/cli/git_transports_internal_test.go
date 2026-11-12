package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"
	"github.com/go-git/go-git/v6/storage"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GitTransportSchemes(t *testing.T) {
	preservePersistenceNoticeState(t)
	for _, scheme := range []string{"ssh", "git", "http", "https", "file"} {
		t.Run(scheme, func(t *testing.T) {
			fixture := newGitTransportFixture(t, scheme)
			fixture.run(t, false)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GitBaselineTransportSchemes(t *testing.T) {
	preservePersistenceNoticeState(t)
	for _, scheme := range []string{"ssh", "git", "http", "https", "file"} {
		t.Run(scheme, func(t *testing.T) {
			fixture := newGitTransportFixture(t, scheme)
			fixture.run(t, true)
		})
	}
}

type gitTransportFixture struct {
	root, origin string
	routes       sourcegit.Transports
	calls        map[string]*atomic.Int32
}

func newGitTransportFixture(t *testing.T, scheme string) gitTransportFixture {
	t.Helper()
	public, private, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(private, "fixture")
	assert.NoError(t, err)
	key, err := ssh.NewPublicKey(public)
	assert.NoError(t, err)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: ssh, namespace: flux-system}\nstringData:\n  identity: |\n    " +
		strings.ReplaceAll(
			strings.TrimSpace(string(pem.EncodeToMemory(block))),
			"\n",
			"\n    ",
		) +
		"\n  known_hosts: " + strconv.Quote(
		knownhosts.Line([]string{"fixture.invalid"}, key),
	) + "\n"
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}
	origin := scheme + "://fixture.invalid/"
	if scheme == "file" {
		origin = "file:///"
	}
	loader := transport.MapLoader{}
	calls := map[string]*atomic.Int32{}
	var child plumbing.Hash
	childURL := ""
	for _, name := range []string{"leaf", "middle", "parent"} {
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		if child.IsZero() {
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: routed, namespace: default}\n",
			)
		} else {
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [lib]\n")
			testutil.WriteFile(t, root, ".gitmodules", "[submodule \"lib\"]\npath = lib\nurl = "+childURL+"\n")
		}
		testutil.GitCommitAll(t, repo, "files", &git.CommitOptions{Author: author})
		if !child.IsZero() {
			idx, indexErr := repo.Storer.Index()
			assert.NoError(t, indexErr)
			idx.Entries = append(idx.Entries, &index.Entry{Name: "lib", Mode: filemode.Submodule, Hash: child})
			assert.NoError(t, repo.Storer.SetIndex(idx))
			worktree, worktreeErr := repo.Worktree()
			assert.NoError(t, worktreeErr)
			_, commitErr := worktree.Commit("submodule", &git.CommitOptions{Author: author})
			assert.NoError(t, commitErr)
		}
		head, headErr := repo.Head()
		assert.NoError(t, headErr)
		child, childURL = head.Hash(), origin+name
		loader["/"+name] = struct{ storage.Storer }{repo.Storer}
		calls[childURL] = new(atomic.Int32)
	}
	local := ""
	for _, side := range []string{"before", "after"} {
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		_, remoteErr := repo.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{origin + "baseline"}},
		)
		assert.NoError(t, remoteErr)
		auth := ""
		if scheme == "ssh" {
			auth = "\n  secretRef: {name: ssh}"
			testutil.WriteFile(t, root, "flux/auth.yaml", secret)
		}
		testutil.WriteFile(
			t,
			root,
			"flux/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: flux-system}\nspec:\n  url: "+childURL+"\n  recurseSubmodules: true"+auth+"\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/app.yaml",
			testutil.KustomizationYAML(
				"app",
				"flux-system",
				"  path: ./lib\n  sourceRef: {kind: GitRepository, name: remote}\n  commonMetadata: {labels: {revision: "+side+"}}\n",
			),
		)
		pin := testutil.GitCommitAll(t, repo, side, &git.CommitOptions{Author: author})
		if side == "before" {
			assert.NoError(
				t,
				repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewTagReferenceName("baseline"), pin),
				),
			)
			loader["/baseline"] = struct{ storage.Storer }{repo.Storer}
			calls[origin+"baseline"] = new(atomic.Int32)
		} else {
			local = root
		}
	}
	server := file.NewTransport(file.Options{Loader: loader})
	routes := sourcegit.Transports{
		scheme: func(ctx context.Context, req sourcegit.TransportRequest) (sourcegit.Connection, error) {
			counter, declared := calls[req.URL.String()]
			if !declared {
				return nil, fmt.Errorf("undeclared fixture URL %q", req.URL)
			}
			counter.Add(1)
			return server.Connect(
				ctx,
				&transport.Request{
					URL:      req.URL,
					Command:  req.Command,
					Protocol: protocol.Version(req.Protocol),
				},
			)
		},
	}
	return gitTransportFixture{root: local, origin: origin, routes: routes, calls: calls}
}

func (f gitTransportFixture) run(t *testing.T, baseline bool) {
	t.Helper()
	args := []string{
		"build", "ks", "--path", filepath.Join(f.root, "flux"),
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress",
	}
	if baseline {
		args = append(args, "--base", "baseline")
	}
	const want = `---
apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: app
    kustomize.toolkit.fluxcd.io/namespace: flux-system
    revision: after
  name: routed
  namespace: default
`
	for range 2 {
		var stdout, stderr bytes.Buffer
		code := Run(args, &stdout, &stderr, RunConfig{GitTransports: f.routes})
		assert.FatalIf(t, code != 0, "exit=%d stderr=%s", code, stderr.String())
		assert.Diff(t, stdout.String(), want)
	}
	for _, name := range []string{"parent", "middle", "leaf"} {
		assert.FatalIf(t, f.calls[f.origin+name].Load() == 0, "CLI bypassed %s transport", name)
	}
	assert.Equal(t, f.calls[f.origin+"baseline"].Load() > 0, baseline)
}
