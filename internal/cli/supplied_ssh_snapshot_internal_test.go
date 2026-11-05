package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

type suppliedSSHTransport struct {
	transport.Transport

	allowed       ssh.PublicKey
	authenticated atomic.Int32
	onAuth        func()
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (s *suppliedSSHTransport) NewUploadPackSession(
	endpoint *transport.Endpoint,
	auth transport.AuthMethod,
) (transport.UploadPackSession, error) {
	keys, ok := auth.(*gitssh.PublicKeys)
	if !ok || keys.User != "git" || keys.Signer == nil {
		return nil, transport.ErrAuthenticationRequired
	}
	if !bytes.Equal(keys.Signer.PublicKey().Marshal(), s.allowed.Marshal()) {
		return nil, transport.ErrAuthorizationFailed
	}
	challenge := []byte("supplied source authentication fixture")
	signature, err := keys.Signer.Sign(rand.Reader, challenge)
	if err != nil {
		return nil, err
	}
	if err := s.allowed.Verify(challenge, signature); err != nil {
		return nil, err
	}

	s.authenticated.Add(1)
	if s.onAuth != nil {
		s.onAuth()
	}
	local := *endpoint
	local.Protocol = "file"
	local.Path = filepath.Join(local.Path, ".git")
	return s.Transport.NewUploadPackSession(&local, nil)
}

// CLI execution and Git protocol registration require serial execution.
func TestRun_SuppliedSSHSnapshotOnBothDiffSides(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	assert.FatalIf(t, err != nil, "%s", err)

	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.FatalIf(t, err != nil, "%s", err)

	identity := string(pem.EncodeToMemory(block))
	allowed, err := ssh.NewPublicKey(public)
	assert.FatalIf(t, err != nil, "%s", err)

	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	remote := t.TempDir()
	repository := testutil.GitInit(t, remote)
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}
	roots := make([]string, 0, 2)
	for _, value := range []string{"before", "after"} {
		for _, name := range []string{"one", "two"} {
			testutil.WriteFile(t, remote, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				remote,
				name+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: "+value+"}\n",
			)
		}
		commit := testutil.GitCommitAll(t, repository, value, &gitlib.CommitOptions{Author: author})
		root := t.TempDir()
		local := testutil.GitInit(t, root)
		if _, err := local.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}},
		); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
		declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: 'https://local.invalid/cluster'}\n---\n" +
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n  secretRef: {name: ursa-major-ssh}\n"
		var declarationsSb147 strings.Builder
		for _, name := range []string{"one", "two"} {
			declarationsSb147.WriteString("---\n" + testutil.KustomizationYAML(
				name,
				"flux-system",
				"  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: private}\n",
			))
		}
		declarations += declarationsSb147.String()
		declarations += "---\n" + testutil.KustomizationYAML(
			"credentials",
			"flux-system",
			"  path: ./local\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		)
		testutil.WriteFile(t, root, "local/kustomization.yaml", "resources: [auth.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"local/auth.yaml",
			"apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData: {identity: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
		)
		testutil.WriteFile(t, root, "flux.yaml", declarations)
		testutil.GitCommitAll(t, local, "cluster "+value, &gitlib.CommitOptions{Author: author})
	}
	supplied := t.TempDir()
	testutil.WriteFile(
		t,
		supplied,
		"credentials.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData:\n  identity: |\n    "+strings.ReplaceAll(
			strings.TrimSpace(identity),
			"\n",
			"\n    ",
		)+"\n  known_hosts: "+knownhosts.Line(
			[]string{"fixture"},
			allowed,
		)+"\n",
	)
	input := filepath.Join(supplied, "credentials.yaml")
	for i, root := range roots {
		result3 := runCLI(t,
			"build",
			"all",
			"--path",
			root,
			"--supply",
			input,
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--no-progress",
			"--skip-kinds",
			"CustomResourceDefinition",
		)
		stdout, stderr, code := result3.stdout, result3.stderr, result3.code
		assert.FatalIf(
			t,
			code != 0 || !strings.Contains(stdout, "stringData: {}") || !strings.Contains(stderr, "[SOPSUnreadable]") ||
				strings.Contains(stdout+stderr, "BEGIN OPENSSH PRIVATE KEY"),
			"supplied build: exit=%d stdout=%s stderr=%s",
			code,
			stdout,
			stderr,
		)
		assert.FatalIf(t, strings.Count(stdout, "value: "+[]string{"before", "after"}[i]) != 2,
			"private consumers missing: %s", stdout)
	}
	before := tr.authenticated.Load()
	var changed sync.Once
	tr.onAuth = func() {
		changed.Do(func() {
			testutil.WriteFileAt(
				t,
				input,
				"apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData: {identity: changed-after-snapshot}\n",
			)
		})
	}
	args := []string{
		"diff",
		"all",
		"--path",
		roots[1],
		"--path-orig",
		roots[0],
		"--concurrency",
		"2",
		"--no-progress",
		"-o",
		"diff",
	}
	result1 := runCLI(t, append(args, "--cache-dir", t.TempDir(), "--supply", input)...)
	stdout, stderr, code := result1.stdout, result1.stderr, result1.code
	assert.FatalIf(
		t,
		code != 0 || strings.Count(stdout, "-  value: before") != 2 || strings.Count(stdout, "+  value: after") != 2 ||
			tr.authenticated.Load()-before < 2,
		"supplied diff: exit=%d authenticated=%d stdout=%s stderr=%s",
		code,
		tr.authenticated.Load()-before,
		stdout,
		stderr,
	)

	tr.onAuth = nil
	before = tr.authenticated.Load()
	result2 := runCLI(t,
		append(args, "--cache-dir", t.TempDir(), "--allow-missing-kinds=Secret,ConfigMap")...)
	stdout, stderr, code = result2.stdout, result2.stderr, result2.code
	assert.FatalIf(t, code != 0 || !strings.Contains(stderr, "SourceCredentialsUnavailable") ||
		!strings.Contains(stderr, "ursa-major-ssh") ||
		strings.Contains(stderr, "ExternalSource") ||
		tr.authenticated.Load() != before,
		"absent-agent cause: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
}
