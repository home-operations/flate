package e2e

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_SourceRef_NonHEADTag(t *testing.T) {
	root, _, _, _ := sourceRefFixture(t)
	out, stderr := requireCLIOK(t, "build", "all", "--path", root+"/flux",
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: v2.0.0") {
		t.Fatalf("expected pinned v1.0.0 content:\n%s\nstderr:\n%s", out, stderr)
	}
}

func sourceRefFixture(t *testing.T) (string, *gogit.Repository, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	root := t.TempDir()
	repo := gitInit(t, root)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
	}); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "flux/entry.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	testutil.WriteFile(t, root, "ignored.txt", "ignored by the source artifact\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	var hashes [2]plumbing.Hash
	for i, version := range []string{"v1.0.0", "v2.0.0"} {
		testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+version+"\n")
		gitCommitAll(t, repo)
		head, err := repo.Head()
		if err != nil {
			t.Fatal(err)
		}
		hashes[i] = head.Hash()
		if err := repo.Storer.SetReference(plumbing.NewHashReference(
			plumbing.NewTagReferenceName(version), head.Hash())); err != nil {
			t.Fatal(err)
		}
	}
	return root, repo, hashes[0], hashes[1]
}

func TestE2E_SourceRef_NonHEADRendersPreserveCheckout(t *testing.T) {
	for _, kind := range []string{"tag", "commit", "semver", "head", "no-ref"} {
		t.Run(kind, func(t *testing.T) {
			root, repo, a, _ := sourceRefFixture(t)
			ref := "tag: v1.0.0"
			want := "v1.0.0"
			switch kind {
			case "commit":
				ref = "commit: " + a.String()
			case "semver":
				ref = "semver: '<2.0.0'"
			case "head":
				ref, want = "tag: v2.0.0", "dirty"
			case "no-ref":
				ref, want = "", "dirty"
			}
			setSourceRef(t, root, ref)
			mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			beforeHead, err := repo.Head()
			if err != nil {
				t.Fatal(err)
			}
			beforeStatus, err := wt.Status()
			if err != nil {
				t.Fatal(err)
			}
			before := make(map[string][]byte)
			for _, path := range []string{".git/HEAD", ".git/index", "flux/entry.yaml", "apps/cm.yaml", "apps/kustomization.yaml"} {
				before[path], err = os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
			}
			out, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			if !strings.Contains(out, "value: "+want) || kind != "head" && kind != "no-ref" && strings.Contains(out, "dirty") {
				t.Fatalf("expected %s content:\n%s", want, out)
			}
			afterHead, err := repo.Head()
			if err != nil || afterHead.Hash() != beforeHead.Hash() || afterHead.Name() != beforeHead.Name() {
				t.Fatalf("HEAD changed: %v, %v", afterHead, err)
			}
			afterStatus, err := wt.Status()
			if diff := cmp.Diff(beforeStatus, afterStatus); err != nil || diff != "" {
				t.Fatalf("checkout status changed (-want +got):\n%s\n%v", diff, err)
			}
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(root, path))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("checkout file %s changed: %v", path, err)
				}
			}
		})
	}
}

func TestE2E_PinnedDiff_BaseAndCurrentRefs(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	setSourceRef(t, root, "tag: v2.0.0")
	out, stderr := requireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
	assertSourceValueDiff(t, out, "v1.0.0", "v2.0.0")
	if loader.calls.Load() == 0 || strings.Count(stderr, "declared source ref cannot be satisfied locally") != 1 {
		t.Fatalf("exported declared-ref baseline did not warn/fetch: calls=%d\n%s", loader.calls.Load(), stderr)
	}
}

func TestE2E_PinnedDiff_StablePinIgnoresDirtyContent(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	mutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	local, _ := requireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	exported := copyTree(t, root)
	if err := os.RemoveAll(filepath.Join(exported, ".git")); err != nil {
		t.Fatal(err)
	}
	fetched, _ := requireCLIOK(t, "build", "all", "--path", exported, "--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0")
	if fetched != local || loader.calls.Load() == 0 {
		t.Fatalf("local/fetched pin bytes differ or no fetch occurred:\nlocal:\n%s\nfetched:\n%s", local, fetched)
	}
	out, stderr := requireCLIOK(t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD",
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
	if out != "" || strings.Count(stderr, "declared source ref cannot be satisfied locally") != 1 {
		t.Fatalf("stable pin changed with dirty working files:\n%s\nstderr:\n%s", out, stderr)
	}
}

func TestE2E_SourceRef_PathOrigUsesOwnObjectStore(t *testing.T) {
	current, _, _, _ := sourceRefFixture(t)
	setSourceRef(t, current, "name: refs/tags/v1.0.0")
	original, originalRepo, _, b := sourceRefFixture(t)
	if err := originalRepo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b)); err != nil {
		t.Fatal(err)
	}
	out, _ := requireCLIOK(t, "diff", "all", "--path", filepath.Join(current, "flux"), "--path-orig", filepath.Join(original, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--git-depth", "0", "--log-level", "warn", "-o", "diff")
	assertSourceValueDiff(t, out, "v2.0.0", "v1.0.0")
}

func assertSourceValueDiff(t *testing.T, out, before, after string) {
	t.Helper()
	if strings.Count(out, "-  value: "+before) != 1 || strings.Count(out, "+  value: "+after) != 1 ||
		strings.Contains(out, "+apiVersion:") || strings.Contains(out, "+kind: ConfigMap") {
		t.Fatalf("expected exact field removal/addition for %s -> %s:\n%s", before, after, out)
	}
}

func setSourceRef(t *testing.T, root, ref string) {
	t.Helper()
	path := filepath.Join(root, "flux/entry.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	line := ""
	if ref != "" {
		line = "  ref: {" + ref + "}\n"
	}
	testutil.WriteFileAt(t, path, strings.Replace(string(data), "  ref: {tag: v1.0.0}\n", line, 1))
}

type sourceRefLoader struct {
	store storer.Storer
	calls atomic.Int64
}

func (l *sourceRefLoader) Load(endpoint *transport.Endpoint) (storer.Storer, error) {
	if endpoint.Host != "fixture.invalid" || endpoint.Path != "/cluster" {
		return nil, transport.ErrRepositoryNotFound
	}
	l.calls.Add(1)
	return l.store, nil
}

func installSourceRefTransport(t *testing.T, storage storer.Storer) *sourceRefLoader {
	t.Helper()
	loader := &sourceRefLoader{store: storage}
	previous := client.Protocols["git"]
	client.InstallProtocol("git", server.NewClient(loader))
	t.Cleanup(func() { client.InstallProtocol("git", previous) })
	return loader
}
