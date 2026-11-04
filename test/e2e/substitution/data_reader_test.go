package substitution_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

type suppliedSSHTransport struct {
	transport.Transport

	allowed       ssh.PublicKey
	host          ssh.PublicKey
	authenticated atomic.Int32
	onAuth        func()
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (s *suppliedSSHTransport) NewUploadPackSession(
	endpoint *transport.Endpoint,
	auth transport.AuthMethod,
) (transport.UploadPackSession, error) {
	var signers []ssh.Signer
	switch keys := auth.(type) {
	case *gitssh.PublicKeys:
		if keys.User != "git" || keys.Signer == nil {
			return nil, transport.ErrAuthenticationRequired
		}
		signers = []ssh.Signer{keys.Signer}
	case *gitssh.PublicKeysCallback:
		if keys.User != "git" || s.host == nil {
			return nil, transport.ErrAuthenticationRequired
		}
		if err := keys.HostKeyCallback(
			"fixture:22",
			&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22},
			s.host,
		); err != nil {
			return nil, err
		}
		var err error
		signers, err = keys.Callback()
		if err != nil {
			return nil, err
		}
	default:
		return nil, transport.ErrAuthenticationRequired
	}
	challenge := []byte("supplied source authentication fixture")
	accepted := false
	for _, signer := range signers {
		if !bytes.Equal(signer.PublicKey().Marshal(), s.allowed.Marshal()) {
			continue
		}
		signature, err := signer.Sign(rand.Reader, challenge)
		if err != nil {
			return nil, err
		}
		if err := s.allowed.Verify(challenge, signature); err != nil {
			return nil, err
		}
		accepted = true
		break
	}
	if !accepted {
		return nil, transport.ErrAuthorizationFailed
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

func TestE2E_DataReader_SuppliedSSHOnBothDiffSides(t *testing.T) {
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
		result3 := harness.RunCLIBuffers(
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
		stdout, stderr, code := result3.Stdout, result3.Stderr, result3.Code
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
	result1 := harness.RunCLIBuffers(append(args, "--cache-dir", t.TempDir(), "--supply", input)...)
	stdout, stderr, code := result1.Stdout, result1.Stderr, result1.Code
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
	result2 := harness.RunCLIBuffers(
		append(args, "--cache-dir", t.TempDir(), "--allow-missing-kinds=Secret,ConfigMap")...)
	stdout, stderr, code = result2.Stdout, result2.Stderr, result2.Code
	assert.FatalIf(t, code != 0 || !strings.Contains(stderr, "SourceCredentialsUnavailable") ||
		!strings.Contains(stderr, "ursa-major-ssh") ||
		strings.Contains(stderr, "ExternalSource") ||
		tr.authenticated.Load() != before,
		"absent-agent cause: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DataReader_GeneratorEmbeddedCiphertext(t *testing.T) {
	const value = "enabled: ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	for _, kind := range []string{"secretGenerator", "configMapGenerator"} {
		t.Run(kind, func(t *testing.T) {
			testE2EDataReaderGeneratorEmbeddedCiphertextCase1(t, &kind, value)
		})
	}
}

func testE2EDataReaderGeneratorEmbeddedCiphertextCase1(t *testing.T, kind *string, value string) {
	t.Helper()
	root := t.TempDir()
	refKind := "Secret"
	if (*kind) == "configMapGenerator" {
		refKind = "ConfigMap"
	}
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./apps\n  postBuild: {substituteFrom: [{kind: "+refKind+", name: creds}]}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [flux]\ngeneratorOptions: {disableNameSuffixHash: true}\n"+(*kind)+":\n- name: creds\n  namespace: flux-system\n  files: [VALUE=values.yaml]\n",
	)
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "values.yaml", value+"\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata:\n  value: |\n    ${VALUE}\n",
	)
	var first string
	for i := range 2 {
		result4 := harness.RunCLIBuffers(
			"build",
			"ks",
			"consumer",
			"--path",
			root,
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--no-progress",
		)
		stdout, stderr, code := result4.Stdout, result4.Stderr, result4.Code
		assert.FatalIf(t, code != 0,
			"exit=%d stdout=%s stderr=%s", code, stdout, stderr)

		docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
		assert.FatalIf(t, err != nil, "%s", err)

		found := 0
		for _, doc := range docs {
			result5 := manifest.DocMetadata(doc)
			name := result5.Name
			if manifest.DocKind(doc) != "ConfigMap" || name != "result" {
				continue
			}
			found++
			data, _ := doc["data"].(map[string]any)
			assert.FatalIf(t, data["value"] != value+"\n",
				"value=%v, want %q", data["value"], value+"\n")
		}
		assert.FatalIf(t, found != 1,
			"result ConfigMaps=%d, want 1", found)
		assert.FatalIf(t, i == 1 && stdout != first, "%s", "repeated build changed stdout")

		first = stdout
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DataReader_StringDataPrecedence(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"ks.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./apps\n  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: creds, namespace: flux-system}\ndata: {VALUE: QQ==, EMPTY: invalid-base64!}\nstringData: {VALUE: B, EMPTY: ''}\n",
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}', empty: '${EMPTY:=fallback}'}\n",
	)
	result6 := harness.RunCLIBuffers(
		"build",
		"ks",
		"consumer",
		"--path",
		root,
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--no-progress",
	)
	stdout, stderr, code := result6.Stdout, result6.Stderr, result6.Code

	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(stdout, "value: B") || !strings.Contains(stdout, "empty: fallback"),
		"exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DataReader_SuppliedSourceProducesCredential(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.NoError(t, err)
	identity := string(pem.EncodeToMemory(block))
	allowed, err := ssh.NewPublicKey(public)
	assert.NoError(t, err)
	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	remote := t.TempDir()
	repository := testutil.GitInit(t, remote)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: flux-system}\nstringData:\n  identity: |\n    " + strings.ReplaceAll(
		strings.TrimSpace(identity),
		"\n",
		"\n    ",
	) + "\n  known_hosts: " + knownhosts.Line(
		[]string{"fixture"},
		allowed,
	) + "\n"
	testutil.WriteFile(t, remote, "credentials/kustomization.yaml", "resources: [auth.yaml]\n")
	testutil.WriteFile(t, remote, "credentials/auth.yaml", secret)
	testutil.WriteFile(t, remote, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		remote,
		"workload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: source-produced, namespace: apps}\ndata: {value: complete}\n",
	)
	commit := testutil.GitCommitAll(
		t,
		repository,
		"fixture",
		&gitlib.CommitOptions{
			Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)},
		},
	)
	root := t.TempDir()
	local := testutil.GitInit(t, root)
	{
		_, err := local.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://local.invalid/cluster}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n  secretRef: {name: credentials}\n"
	var declarationsSb419 strings.Builder
	for _, name := range []string{"credentials", "workload"} {
		declarationsSb419.WriteString("---\n" + testutil.KustomizationYAML(
			name,
			"flux-system",
			"  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: private}\n",
		))
	}
	declarations += declarationsSb419.String()
	testutil.WriteFile(t, root, "flux.yaml", declarations)
	input := filepath.Join(t.TempDir(), "credentials.yaml")
	testutil.WriteFileAt(t, input, secret)
	var first string
	for i := range 2 {
		result7 := harness.RunCLIBuffers(
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
		)
		stdout, stderr, code := result7.Stdout, result7.Stderr, result7.Code

		assert.FatalIf(t, code != 0 || !strings.Contains(stdout, "name: source-produced") ||
			strings.Contains(stdout, "BEGIN OPENSSH PRIVATE KEY") ||
			strings.Contains(
				stderr,
				"dependency cycle",
			), "source-producing credential: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		assert.FatalIf(t, i == 1 && stdout != first, "%v", "source-producing output varies across runs")
		first = stdout
	}
	assert.FatalIf(t, tr.authenticated.Load() < 2,
		"authenticated acquisitions = %d", tr.authenticated.Load())
}

func TestE2E_DataReader_ClusterAdvisoriesOnBothBaseSides(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	allowed, err := ssh.NewPublicKey(public)
	assert.NoError(t, err)
	_, hostKey, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	host, err := ssh.NewSignerFromKey(hostKey)
	assert.NoError(t, err)
	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed, host: host.PublicKey()}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
	keyring := agent.NewKeyring()
	assert.NoError(t, keyring.Add(agent.AddedKey{PrivateKey: key}))
	t.Chdir(t.TempDir())
	listener, err := net.Listen("unix", "a")
	assert.NoError(t, err)
	t.Setenv("SSH_AUTH_SOCK", "a")
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				_ = agent.ServeAgent(keyring, conn)
				_ = conn.Close()
			})
		}
	})
	t.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	remote, root := t.TempDir(), t.TempDir()
	repository, local := testutil.GitInit(t, remote), testutil.GitInit(t, root)
	{
		_, err := local.CreateRemote(
			&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}},
		)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}
	var base string
	for _, value := range []string{"before", "after"} {
		testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			remote,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: "+value+", override: '${OVERRIDE}'}\n",
		)
		commit := testutil.GitCommitAll(t, repository, value, &gitlib.CommitOptions{Author: author})
		declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://local.invalid/cluster}\n---\n" +
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n---\n" +
			testutil.KustomizationYAML(
				"consumer",
				"flux-system",
				"  path: ./app\n  sourceRef: {kind: GitRepository, name: private}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
			)
		testutil.WriteFile(t, root, "flux.yaml", declarations)
		testutil.WriteFile(
			t,
			root,
			"settings.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {OVERRIDE: repository}\n",
		)
		commit = testutil.GitCommitAll(t, local, value, &gitlib.CommitOptions{Author: author})
		if value == "before" {
			base = commit.String()
		}
	}
	input := filepath.Join(t.TempDir(), "supply.yaml")
	testutil.WriteFileAt(
		t,
		input,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {OVERRIDE: supplied}\n",
	)
	var previousOut, previousErr string
	for run := range 2 {
		before := tr.authenticated.Load()
		result8 := harness.RunCLIBuffers("diff", "all", "--path", root, "--base", base, "--supply", input,
			"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "-o", "diff")
		stdout, stderr, code := result8.Stdout, result8.Stderr, result8.Code
		assert.FatalIf(
			t,
			code != 0 || tr.authenticated.Load()-before < 2 || strings.Count(stdout, "-  value: before") != 1 ||
				strings.Count(stdout, "+  value: after") != 1,
			"base diff: exit=%d authentications=%d stdout=%s stderr=%s",
			code,
			tr.authenticated.Load()-before,
			stdout,
			stderr,
		)

		assert.FatalIf(
			t,
			strings.Count(stderr, "[SuppliedPrecedence]") != 1 || strings.Contains(stderr, "authenticated with") ||
				!strings.Contains(
					stderr,
					"ConfigMap/flux-system/settings: OVERRIDE",
				),
			"cluster advisories lost or duplicated: %s",
			stderr,
		)
		assert.FatalIf(t, strings.Contains(stdout+stderr, "PRIVATE KEY") || strings.Contains(stdout, "supplied") ||
			!strings.Contains(
				stdout,
				"override: repository",
			), "private material or incorrect precedence: stdout=%s stderr=%s", stdout, stderr)
		assert.FatalIf(
			t,
			run == 1 && (stdout != previousOut || stderr != previousErr),
			"base diff changed across runs: stdout=%s stderr=%s",
			stdout,
			stderr,
		)
		previousOut, previousErr = stdout, stderr
	}
}
