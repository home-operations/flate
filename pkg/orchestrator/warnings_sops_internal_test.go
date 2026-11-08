package orchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_ProducerDeclaredSecretUnavailable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cloudflared-secret
`))
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./externalsecret.yaml\n  - ./cm.yaml\n",
	)
	testutil.WriteFile(t, dir, "apps/externalsecret.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: cloudflared}
spec:
  secretStoreRef: {kind: ClusterSecretStore, name: onepassword}
  target:
    name: cloudflared-secret
    template:
      data:
        CLOUDFLARE_TUNNEL_ID: "{{ .id }}"
        identity: "{{ .identity }}"
  dataFrom:
    - extract: {key: cloudflare}
`)
	testutil.WriteFile(
		t,
		dir,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: tunnel\ndata:\n  target: \"${CLOUDFLARE_TUNNEL_ID}.cfargotunnel.com\"\n",
	)

	renderedSingleton4, err := renderTreesForTest(t.Context(), t,
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{},
	)
	assert.FatalIf(t, len(renderedSingleton4) != 1,
		"missing render: %v", err)

	o := renderedSingleton4[0].Orchestrator
	res := renderedSingleton4[0].Result
	assert.FatalIf(t, err != nil,
		"Render: %v", err)

	repo := &manifest.GitRepository{Name: "ssh", Namespace: "flux-system"}
	repo.URL = "ssh://git@example.test/cluster"
	repo.SecretRef = &manifest.LocalObjectReference{Name: "cloudflared-secret"}
	var reads store.ReadSet
	reads.Init(o.Store(), repo.Named(), nil)
	_, err = o.src.Fetchers[manifest.KindGitRepository].Fetch(t.Context(), repo, &reads, &sourcecache.Policy{})
	if missing, ok := errors.AsType[*source.MissingSecretError](
		err,
	); !ok || missing.Detail != "not found" || missing.Secret.Name != "cloudflared-secret" ||
		!reads.Valid() {
		t.Fatalf("producer-declared credential lost absent-material classification: %v", err)
	}

	want := "${CLOUDFLARE_TUNNEL_ID}.cfargotunnel.com"
	var got string
	for _, docs := range outputByParent(res.Manifests) {
		for _, d := range docs {
			md, _ := d["metadata"].(map[string]any)
			data, _ := d["data"].(map[string]any)
			if d["kind"] == "ConfigMap" && md["name"] == "tunnel" {
				got, _ = data["target"].(string)
			}
		}
	}
	assert.ErrorIf(t, got != want,
		"tunnel ConfigMap data.target = %q, want %q", got, want)

	for _, w := range res.Warnings {
		if w.Category == manifest.WarnUnresolvedSubstitution && strings.Contains(w.Message, "cloudflared-secret") {
			return
		}
	}
	t.Fatal("missing unresolved-substitution advisory")
}

func TestRender_UnreadableSSHKnownHosts(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	_, key, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(key, "fixture")
	assert.NoError(t, err)
	identity := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(block))
	var listenerConfig net.ListenConfig
	listener, err := listenerConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	var dials atomic.Int32
	var server sync.WaitGroup
	server.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			dials.Add(1)
			_ = conn.Close()
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		server.Wait()
		assert.Equal(t, dials.Load(), int32(0))
	})
	root := t.TempDir()
	testutil.WriteFile(t, root, "source.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: remote, namespace: apps}
spec:
  url: ssh://git@%s/repo
  secretRef: {name: credentials}
`, listener.Addr()))
	testutil.WriteFile(t, root, "secret.yaml", fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata: {name: credentials, namespace: apps}
data:
  identity: %s
  known_hosts: ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]
sops: {version: 3.9.0}
`, identity))
	rendered, err := renderTreesForTest(t.Context(), t,
		[]Tree{{Path: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{Concurrency: 2, CacheDir: t.TempDir(), AllowMissingKinds: []string{manifest.KindSecret}},
	)
	assert.NoError(t, err)
	assert.FatalIf(t, len(rendered) != 1, "rendered trees = %d, want one", len(rendered))
	var unreadable, unavailable int
	for _, warning := range rendered[0].Result.Warnings {
		switch warning.Category {
		case manifest.WarnSOPSUnreadable:
			unreadable++
			assert.Equal(
				t,
				warning.Resource,
				(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}),
			)
			assert.Diff(t, warning.Detail, []string{"known_hosts"})
			assert.Equal(t, warning.Count, 1)
		case manifest.WarnSourceCredentialsUnavailable:
			unavailable++
			assert.Equal(
				t,
				warning.Resource,
				(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "remote"}),
			)
		}
	}
	assert.Equal(t, unreadable, 1)
	assert.Equal(t, unavailable, 1)
}
