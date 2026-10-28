package substitution

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

func (s *suppliedSSHTransport) NewUploadPackSession(endpoint *transport.Endpoint, auth transport.AuthMethod) (transport.UploadPackSession, error) {
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
		if err := keys.HostKeyCallback("fixture:22", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}, s.host); err != nil {
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
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	identity := string(pem.EncodeToMemory(block))
	allowed, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	remote := t.TempDir()
	repository := testutil.GitInit(t, remote)
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}
	var roots []string
	for _, value := range []string{"before", "after"} {
		for _, name := range []string{"one", "two"} {
			testutil.WriteFile(t, remote, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, remote, name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: "+value+"}\n")
		}
		commit := testutil.GitCommitAll(t, repository, value, &gitlib.CommitOptions{Author: author})
		root := t.TempDir()
		local := testutil.GitInit(t, root)
		if _, err := local.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}}); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
		declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: 'https://local.invalid/cluster'}\n---\n" +
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n  secretRef: {name: ursa-major-ssh}\n"
		for _, name := range []string{"one", "two"} {
			declarations += "---\n" + testutil.KustomizationYAML(name, "flux-system", "  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: private}\n")
		}
		declarations += "---\n" + testutil.KustomizationYAML("credentials", "flux-system", "  path: ./local\n  sourceRef: {kind: GitRepository, name: cluster}\n")
		testutil.WriteFile(t, root, "local/kustomization.yaml", "resources: [auth.yaml]\n")
		testutil.WriteFile(t, root, "local/auth.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData: {identity: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n")
		testutil.WriteFile(t, root, "flux.yaml", declarations)
		testutil.GitCommitAll(t, local, "cluster "+value, &gitlib.CommitOptions{Author: author})
	}
	supplied := t.TempDir()
	testutil.WriteFile(t, supplied, "credentials.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData:\n  identity: |\n    "+strings.ReplaceAll(strings.TrimSpace(identity), "\n", "\n    ")+"\n")
	input := filepath.Join(supplied, "credentials.yaml")
	for i, root := range roots {
		stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--supply", input, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress", "--skip-kinds", "CustomResourceDefinition")
		if code != 0 || !strings.Contains(stdout, "..PLACEHOLDER_identity..") || strings.Contains(stdout+stderr, "BEGIN OPENSSH PRIVATE KEY") {
			t.Fatalf("supplied build: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
		if strings.Count(stdout, "value: "+[]string{"before", "after"}[i]) != 2 {
			t.Fatalf("private consumers missing: %s", stdout)
		}
	}
	before := tr.authenticated.Load()
	var changed sync.Once
	tr.onAuth = func() {
		changed.Do(func() {
			testutil.WriteFileAt(t, input, "apiVersion: v1\nkind: Secret\nmetadata: {name: ursa-major-ssh, namespace: flux-system}\nstringData: {identity: changed-after-snapshot}\n")
		})
	}
	args := []string{"diff", "all", "--path", roots[1], "--path-orig", roots[0], "--concurrency", "2", "--no-progress", "-o", "diff"}
	stdout, stderr, code := harness.RunCLIBuffers(append(args, "--cache-dir", t.TempDir(), "--supply", input)...)
	if code != 0 || strings.Count(stdout, "-  value: before") != 2 || strings.Count(stdout, "+  value: after") != 2 || tr.authenticated.Load()-before < 2 {
		t.Fatalf("supplied diff: exit=%d authenticated=%d stdout=%s stderr=%s", code, tr.authenticated.Load()-before, stdout, stderr)
	}
	tr.onAuth = nil
	before = tr.authenticated.Load()
	stdout, stderr, code = harness.RunCLIBuffers(append(args, "--cache-dir", t.TempDir(), "--allow-missing-secrets")...)
	if code != 0 || !strings.Contains(stderr, "SourceCredentialsUnavailable") || !strings.Contains(stderr, "ursa-major-ssh") || strings.Contains(stderr, "ExternalSource") || strings.Contains(stderr, "LocalSSHAgent") || tr.authenticated.Load() != before {
		t.Fatalf("absent-agent cause: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestE2E_DataReader_GeneratorEmbeddedCiphertext(t *testing.T) {
	const value = "enabled: ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	for _, kind := range []string{"secretGenerator", "configMapGenerator"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			refKind := "Secret"
			if kind == "configMapGenerator" {
				refKind = "ConfigMap"
			}
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps\n  postBuild: {substituteFrom: [{kind: "+refKind+", name: creds}]}\n"))
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux]\ngeneratorOptions: {disableNameSuffixHash: true}\n"+kind+":\n- name: creds\n  namespace: flux-system\n  files: [VALUE=values.yaml]\n")
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "values.yaml", value+"\n")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata:\n  value: |\n    ${VALUE}\n")
			var first string
			for i := range 2 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
				if code != 0 || strings.Contains(stdout, "..PLACEHOLDER_VALUE..") {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
				if err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, doc := range docs {
					name, _ := manifest.DocMetadata(doc)
					if manifest.DocKind(doc) != "ConfigMap" || name != "result" {
						continue
					}
					found++
					data, _ := doc["data"].(map[string]any)
					if data["value"] != value+"\n" {
						t.Fatalf("value=%v, want %q", data["value"], value+"\n")
					}
				}
				if found != 1 {
					t.Fatalf("result ConfigMaps=%d, want 1", found)
				}
				if i == 1 && stdout != first {
					t.Fatal("repeated build changed stdout")
				}
				first = stdout
			}
		})
	}
}

func TestE2E_DataReader_StringDataPrecedence(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps\n  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}\n"))
	testutil.WriteFile(t, root, "secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: creds, namespace: flux-system}\ndata: {VALUE: QQ==, EMPTY: invalid-base64!}\nstringData: {VALUE: B, EMPTY: ''}\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}', empty: '${EMPTY:=fallback}'}\n")
	stdout, stderr, code := harness.RunCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
	if code != 0 || !strings.Contains(stdout, "value: B") || !strings.Contains(stdout, "empty: fallback") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestE2E_DataReader_SuppliedSourceProducesCredential(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	identity := string(pem.EncodeToMemory(block))
	allowed, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	remote := t.TempDir()
	repository := testutil.GitInit(t, remote)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: flux-system}\nstringData:\n  identity: |\n    " + strings.ReplaceAll(strings.TrimSpace(identity), "\n", "\n    ") + "\n"
	testutil.WriteFile(t, remote, "credentials/kustomization.yaml", "resources: [auth.yaml]\n")
	testutil.WriteFile(t, remote, "credentials/auth.yaml", secret)
	testutil.WriteFile(t, remote, "workload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, remote, "workload/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: source-produced, namespace: apps}\ndata: {value: complete}\n")
	commit := testutil.GitCommitAll(t, repository, "fixture", &gitlib.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}})
	root := t.TempDir()
	local := testutil.GitInit(t, root)
	if _, err := local.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}}); err != nil {
		t.Fatal(err)
	}
	declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://local.invalid/cluster}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n  secretRef: {name: credentials}\n"
	for _, name := range []string{"credentials", "workload"} {
		declarations += "---\n" + testutil.KustomizationYAML(name, "flux-system", "  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: private}\n")
	}
	testutil.WriteFile(t, root, "flux.yaml", declarations)
	input := filepath.Join(t.TempDir(), "credentials.yaml")
	testutil.WriteFileAt(t, input, secret)
	var first string
	for i := range 2 {
		stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--supply", input, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
		if code != 0 || !strings.Contains(stdout, "name: source-produced") || strings.Contains(stdout, "BEGIN OPENSSH PRIVATE KEY") || strings.Contains(stderr, "dependency cycle") {
			t.Fatalf("source-producing credential: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
		if i == 1 && stdout != first {
			t.Fatal("source-producing output varies across runs")
		}
		first = stdout
	}
	if tr.authenticated.Load() < 2 {
		t.Fatalf("authenticated acquisitions = %d", tr.authenticated.Load())
	}
}

func TestE2E_DataReader_ClusterAdvisoriesOnBothBaseSides(t *testing.T) {
	public, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	_, hostKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ssh.NewSignerFromKey(hostKey)
	if err != nil {
		t.Fatal(err)
	}
	tr := &suppliedSSHTransport{Transport: server.DefaultServer, allowed: allowed, host: host.PublicKey()}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
	keyring := agent.NewKeyring()
	if err := keyring.Add(agent.AddedKey{PrivateKey: key}); err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	listener, err := net.Listen("unix", "a")
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := local.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://local.invalid/cluster"}}); err != nil {
		t.Fatal(err)
	}
	author := &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(1, 0)}
	var base string
	for _, value := range []string{"before", "after"} {
		testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, remote, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: "+value+", override: '${OVERRIDE}'}\n")
		commit := testutil.GitCommitAll(t, repository, value, &gitlib.CommitOptions{Author: author})
		declarations := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://local.invalid/cluster}\n---\n" +
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: private, namespace: flux-system}\nspec:\n  url: ssh://git@fixture" + remote + "\n  ref: {commit: " + commit.String() + "}\n---\n" +
			testutil.KustomizationYAML("consumer", "flux-system", "  path: ./app\n  sourceRef: {kind: GitRepository, name: private}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n")
		testutil.WriteFile(t, root, "flux.yaml", declarations)
		testutil.WriteFile(t, root, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {OVERRIDE: repository}\n")
		commit = testutil.GitCommitAll(t, local, value, &gitlib.CommitOptions{Author: author})
		if value == "before" {
			base = commit.String()
		}
	}
	input := filepath.Join(t.TempDir(), "supply.yaml")
	testutil.WriteFileAt(t, input, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {OVERRIDE: supplied}\n")
	var previousOut, previousErr string
	for run := range 2 {
		before := tr.authenticated.Load()
		stdout, stderr, code := harness.RunCLIBuffers("diff", "all", "--path", root, "--base", base, "--supply", input,
			"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "-o", "diff")
		if code != 0 || tr.authenticated.Load()-before < 2 || strings.Count(stdout, "-  value: before") != 1 || strings.Count(stdout, "+  value: after") != 1 {
			t.Fatalf("base diff: exit=%d authentications=%d stdout=%s stderr=%s", code, tr.authenticated.Load()-before, stdout, stderr)
		}
		if strings.Count(stderr, "[SuppliedPrecedence]") != 1 || strings.Count(stderr, "[LocalSSHAgent]") != 1 || !strings.Contains(stderr, "ConfigMap/flux-system/settings: OVERRIDE") || !strings.Contains(stderr, "GitRepository.source.toolkit.fluxcd.io/flux-system/private") {
			t.Fatalf("cluster advisories lost or duplicated: %s", stderr)
		}
		if strings.Contains(stdout+stderr, "PRIVATE KEY") || strings.Contains(stdout, "supplied") || !strings.Contains(stdout, "override: repository") {
			t.Fatalf("private material or incorrect precedence: stdout=%s stderr=%s", stdout, stderr)
		}
		if run == 1 && (stdout != previousOut || stderr != previousErr) {
			t.Fatalf("base diff changed across runs: stdout=%s stderr=%s", stdout, stderr)
		}
		previousOut, previousErr = stdout, stderr
	}
}
