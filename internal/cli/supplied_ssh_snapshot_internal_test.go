package cli

import (
	"crypto/ed25519"
	"encoding/pem"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestRun_SuppliedSSHSnapshotOnBothDiffSides(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	assert.FatalIf(t, err != nil, "%s", err)

	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.FatalIf(t, err != nil, "%s", err)

	identity := string(pem.EncodeToMemory(block))
	allowed, err := ssh.NewPublicKey(public)
	assert.FatalIf(t, err != nil, "%s", err)

	host, err := ssh.NewSignerFromKey(key)
	assert.NoError(t, err)
	tr := &testutil.GitSSH{Host: host, Allowed: allowed}
	address := testutil.LocalGitSSH(t, tr)
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	remote := t.TempDir()
	tr.Roots = []string{remote}
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
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@" + address + remote + "\n  ref: {commit: " + commit.String() + "}\n  secretRef: {name: ursa-major-ssh}\n"
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
		)+"\n  known_hosts: "+strconv.Quote(
			knownhosts.Line(
				[]string{address},
				allowed,
			),
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
	before := tr.Authenticated.Load()
	var changed sync.Once
	tr.AfterAuth = func() error {
		changed.Do(func() {
			testutil.WriteFileAt(
				t,
				input,
				"apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData: {identity: changed-after-snapshot}\n",
			)
		})
		return nil
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
			tr.Authenticated.Load()-before < 2,
		"supplied diff: exit=%d authenticated=%d stdout=%s stderr=%s",
		code,
		tr.Authenticated.Load()-before,
		stdout,
		stderr,
	)

	tr.AfterAuth = nil
	before = tr.Authenticated.Load()
	result2 := runCLI(t,
		append(args, "--cache-dir", t.TempDir(), "--allow-missing-kinds=Secret,ConfigMap")...)
	stdout, stderr, code = result2.stdout, result2.stderr, result2.code
	assert.FatalIf(t, code != 0 || !strings.Contains(stderr, "SourceCredentialsUnavailable") ||
		!strings.Contains(stderr, "ursa-major-ssh") ||
		strings.Contains(stderr, "ExternalSource") ||
		tr.Authenticated.Load() != before,
		"absent-agent cause: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
}
