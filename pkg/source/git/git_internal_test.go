package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_PackedContent(t *testing.T) {
	t.Parallel()
	var blocks [8]string
	for i := range blocks {
		var block strings.Builder
		for line := range 200 {
			fmt.Fprintf(&block, "%x\n", sha256.Sum256(fmt.Appendf(nil, "%d-%d", i, line)))
		}
		blocks[i] = block.String()
	}
	files := map[string]string{"base.txt": strings.Join(blocks[:], ""), "reordered.txt": ""}
	for _, i := range []int{3, 0, 5, 1, 7, 2, 6, 4} {
		files["reordered.txt"] += blocks[i]
	}
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, files)
	for _, mode := range []string{"memory", "disk"} {
		for _, attempts := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/attempts-%d", mode, attempts), func(t *testing.T) {
				t.Parallel()
				testFetcherPackedContentCase(
					t,
					testFetcherPackedContentCaseInputs{
						files:    &files,
						src:      &src,
						mode:     &mode,
						attempts: &attempts,
					},
				)
			},
			)
		}
	}
}

func TestFetcher_SourceLinksOmitted(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"hello.txt": "hi", "apps/cm.yaml": "kind: ConfigMap"})
	for name, target := range map[string]string{"alias": "hello.txt", "dirlink": "apps", ".sourceignore": "hello.txt"} {
		{
			err := os.Symlink(target, filepath.Join(src, name))
			assert.NoError(t, err)
		}
	}
	mustCommitFile(t, src, "hello.txt", "hi")
	f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, "", sourcecache.New)}
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		&manifest.GitRepository{Name: "links", URL: "file://" + src},
		(testInputs{}).readSet(&manifest.GitRepository{Name: "links", URL: "file://" + src}),
		&sourcecache.Policy{},
		nil,
	)
	assert.NoError(t, err)

	for _, name := range []string{"alias", "dirlink", "dirlink/cm.yaml", ".sourceignore"} {
		if file, openErr := art.FS.Open(name); !errors.Is(openErr, fs.ErrPermission) {
			if file != nil {
				_ = file.Close()
			}
			t.Errorf("Open(%s): %v", name, openErr)
		}
		{
			_, statErr := fs.Stat(art.FS, name)
			assert.ErrorIf(t, !errors.Is(statErr, fs.ErrPermission), "Stat(%s): %v", name, statErr)
		}
		{
			_, readDirErr := fs.ReadDir(art.FS, name)
			assert.ErrorIf(t, !errors.Is(readDirErr, fs.ErrPermission), "ReadDir(%s): %v", name, readDirErr)
		}
	}
	entries, err := fs.ReadDir(art.FS, ".")
	assert.NoError(t, err)

	for _, entry := range entries {
		if entry.Name() == "alias" || entry.Name() == "dirlink" || entry.Name() == ".sourceignore" {
			t.Errorf("listed link %s", entry.Name())
		}
	}
}

func TestFetcher_LocalFileURL(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
	}

	f := &Fetcher{Network: networktest.New(t), Cache: cache}
	art, err := fetchtest.Fetch(
		context.Background(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.FatalIf(t, err != nil, "Fetch: %v", err)
	_, pin, err := source.OperationView(t.Context(), art.FS)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
	sa := art
	assert.ErrorIf(t, sa.FS == nil || sa.Key == "" || sa.URL == "", "incomplete artifact: %+v", sa)
	{
		_, statErr2 := fs.Stat(sa.FS, path.Join("hello.txt"))
		assert.ErrorIf(t, statErr2 != nil, "expected checked-out file: %v", statErr2)
	}

	art2, err := fetchtest.Fetch(
		context.Background(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.FatalIf(t, err != nil, "Fetch second: %v", err)
	_, nextPin, err := source.OperationView(t.Context(), art2.FS)
	assert.NoError(t, err)
	assert.Equal(t, nextPin, pin)
	if art2.Revision != sa.Revision {
		t.Errorf("unchanged default ref revision changed: %s vs %s", sa.Revision, art2.Revision)
	}
}

func TestFetcher_RestrictedEndpointSchemes(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"https://github.com/o/r":    true,
		"ssh://git@github.com/o/r":  true,
		"git@github.com:o/r.git":    true,
		"user@host.xz:path/to/repo": true,
		"host.xz:path/to/repo":      false,
		"file:///srv/repo":          false,
		"git://github.com/o/r":      false,
		"http://github.com/o/r":     false,
		"/bare/local/path":          false,
		"./relative":                false,
		"./a@b":                     false,
		"ssh-but-not-scheme://x":    false,
		"":                          false,
	}
	for url, want := range cases {
		t.Run(url, func(t *testing.T) {
			t.Parallel()
			f := &Fetcher{RestrictSchemes: true}
			_, err := f.validateRepo(&manifest.GitRepository{URL: url}, false)
			assert.FatalIf(
				t,
				want && err != nil || !want && !errors.Is(err, manifest.ErrInput),
				"allowed = %t, validateRepo = %v",
				want,
				err,
			)
			if !want {
				f.Cache = new(sourcecache.Cache)
				repo := &manifest.GitRepository{URL: url}
				inputs := testInputs{}
				if strings.Contains(url, ":") && !strings.Contains(url, "://") {
					identity, signer := testSSHIdentity(t)
					repo.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
					inputs.secrets = func(string, string) store.ReferenceFacts {
						secretInput := &manifest.Secret{
							StringData: map[string]any{
								"identity":    identity,
								"known_hosts": "host.xz " + string(ssh.MarshalAuthorizedKey(signer.PublicKey())),
							},
						}

						return store.ReferenceFacts{Data: secretInput, Original: secretInput}
					}
				}
				_, err := fetchtest.Fetch(
					t.Context(),
					f.Fetch,
					repo,
					inputs.readSet(repo),
					&sourcecache.Policy{},
					nil,
				)
				assert.FatalIf(
					t,
					!errors.Is(err, manifest.ErrInput),
					"public Fetch must reject before acquisition: %v",
					err,
				)
				assert.FatalIf(
					t,
					strings.HasPrefix(url, "file://") && !strings.Contains(err.Error(), "scheme not allowed"),
					"error must name the rejected scheme: %v",
					err,
				)
			}
		})
	}
}

func TestFetcher_Freshness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		interval              time.Duration
		depth                 int
		recurse, fail, pinned bool
		commit, mixed         bool
	}{
		{name: "mutable default"},
		{name: "fresh interval", interval: time.Hour},
		{name: "commit-only pin", pinned: true},
		{name: "acquisition identity", recurse: true},
		{name: "failed refresh", fail: true},
		{name: "advanced shallow tip", depth: 1},
		{name: "commit retained", commit: true},
		{name: "mixed commit refreshes", commit: true, mixed: true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testFetcherFreshnessCase(t, &tc) },
		)
	}
}

func TestFetcher_References(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"hello.txt": "hi", "apps/a/manifest.yaml": "kind: ConfigMap", "apps/b/manifest.yaml": "kind: Secret",
	})
	first := mustTagHEAD(t, src, "v1.0.0")
	mustSetRefToHash(t, src, "refs/heads/staging", first)
	mustCommitFile(t, src, "second.txt", "two")
	mustCommitFile(t, src, "chart.yaml", "tagged")
	tip := mustCommitFile(t, src, "version.txt", "v2")
	mustTagHEAD(t, src, "v2.0.0")
	mustSetRefToHEAD(t, src, "refs/pull/1/head")
	for _, tc := range []struct {
		name              string
		ref               *sourcev1.GitRepositoryRef
		depth             int
		sparse            []string
		want              string
		files, absent     []string
		warmCommit        string
		recurse           bool
		wantErr           bool
		invalidInput      bool
		errText           string
		localAcquisitions uint64
	}{
		{name: "invalid full commit", ref: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("g", 40)}, wantErr: true, invalidInput: true},
		{name: "invalid commit branch", ref: &sourcev1.GitRepositoryRef{Commit: first, Branch: "bad branch"}, wantErr: true, invalidInput: true},
		{name: "HEAD", depth: 1, want: tip, files: []string{"hello.txt", "second.txt"}, localAcquisitions: 1},
		{
			name:              "tag",
			ref:               &sourcev1.GitRepositoryRef{Tag: "v2.0.0"},
			depth:             1,
			want:              tip,
			files:             []string{"chart.yaml"},
			localAcquisitions: 1,
		},
		{
			name:              "branch",
			ref:               &sourcev1.GitRepositoryRef{Branch: "staging"},
			depth:             1,
			want:              first,
			files:             []string{"hello.txt"},
			absent:            []string{"version.txt"},
			localAcquisitions: 1,
		},
		{
			name:              "branch recursive checkout",
			ref:               &sourcev1.GitRepositoryRef{Branch: "staging"},
			recurse:           true,
			want:              first,
			files:             []string{"hello.txt"},
			absent:            []string{"version.txt"},
			localAcquisitions: 1,
		},
		{name: "name", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}, want: first, localAcquisitions: 1},
		{name: "unresolvable name", ref: &sourcev1.GitRepositoryRef{Name: "refs/heads/does-not-exist"}, wantErr: true},
		{
			name:              "explicit name fallback",
			ref:               &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"},
			sparse:            []string{"apps/a"},
			want:              tip,
			files:             []string{"apps/a/manifest.yaml"},
			absent:            []string{"apps/b/manifest.yaml"},
			localAcquisitions: 1,
		},
		{
			name:              "semver",
			ref:               &sourcev1.GitRepositoryRef{Tag: "v1.0.0", SemVer: ">=1.1.0"},
			want:              tip,
			files:             []string{"version.txt"},
			localAcquisitions: 1,
		},
		{
			name:              "shallow semver",
			ref:               &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"},
			depth:             1,
			want:              tip,
			files:             []string{"version.txt"},
			localAcquisitions: 1,
		},
		{
			name:              "commit",
			ref:               &sourcev1.GitRepositoryRef{Commit: first},
			want:              first,
			absent:            []string{"version.txt"},
			localAcquisitions: 1,
		},
		{
			name:              "commit precedes name",
			ref:               &sourcev1.GitRepositoryRef{Commit: first, Name: "refs/tags/v2.0.0"},
			want:              first,
			absent:            []string{"version.txt"},
			localAcquisitions: 1,
		},
		{
			name:              "commit precedes unresolvable name",
			ref:               &sourcev1.GitRepositoryRef{Commit: first, Name: "refs/pull/does-not-exist/head"},
			want:              first,
			localAcquisitions: 1,
		},
		{
			name:       "commit branch reachability",
			ref:        &sourcev1.GitRepositoryRef{Commit: tip, Branch: "staging"},
			warmCommit: tip,
			wantErr:    true,
			errText:    "not reachable from branch",
		},
		{
			name:              "shallow sparse",
			depth:             1,
			sparse:            []string{"apps/a"},
			want:              tip,
			files:             []string{"apps/a/manifest.yaml"},
			absent:            []string{"apps/b/manifest.yaml"},
			localAcquisitions: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); testFetcherReferencesCase(t, &src, &tc) })
	}
}

func TestFetcher_SparseCheckout(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	files := map[string]string{
		"apps/a/manifest.yaml":     "a",
		"apps/b/manifest.yaml":     "b",
		"apps-old/manifest.yaml":   "old",
		"apps-extra/manifest.yaml": "extra",
		"infra/c/manifest.yaml":    "infra",
	}
	mustInitRepoWithFiles(t, src, files)
	f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	for _, tc := range []struct {
		prefix string
		want   []string
	}{
		{"apps/a", []string{"apps/a/manifest.yaml"}},
		{"apps", []string{"apps/a/manifest.yaml", "apps/b/manifest.yaml", "apps-old/manifest.yaml", "apps-extra/manifest.yaml"}},
		{"./apps", []string{"apps/a/manifest.yaml", "apps/b/manifest.yaml", "apps-old/manifest.yaml", "apps-extra/manifest.yaml"}},
	} {
		for _, recursive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recursive=%t", tc.prefix, recursive), func(t *testing.T) {
				t.Parallel()
				testFetcherSparseCheckoutCase(
					t,
					&testFetcherSparseCheckoutCaseInputs{
						f:         &f,
						files:     &files,
						recursive: &recursive,
						src:       &src,
						tc:        &tc,
					},
				)
			})
		}
	}
}

// TestFetcher_AppliesSpecIgnore exercises the spec.ignore wiring: a
// staged tree containing a tracked file matching the user-supplied
// gitignore pattern must have that file deleted post-checkout.
func TestFetcher_AppliesSpecIgnore(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/deployment.yaml": "kind: Deployment",
		"apps/scratch.tmp":     "noise",
		"docs/notes.md":        "drop-me",
	})

	patterns := "*.tmp\ndocs/\n"
	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:    "file://" + src,
		Ignore: &patterns,
	}
	f := &Fetcher{Network: networktest.New(t), Cache: cache}
	art, err := fetchtest.Fetch(
		context.Background(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.FatalIf(t, err != nil, "Fetch: %v", err)
	{
		_, err := fs.Stat(art.FS, path.Join("apps", "scratch.tmp"))
		assert.ErrorIf(t, !os.IsNotExist(err), "expected *.tmp to be removed by spec.ignore; stat err = %v", err)
	}
	{
		_, err := fs.Stat(art.FS, path.Join("docs"))
		assert.ErrorIf(t, !os.IsNotExist(err), "expected docs/ to be removed by spec.ignore; stat err = %v", err)
	}
	{
		_, err := fs.Stat(art.FS, path.Join("apps", "deployment.yaml"))
		assert.ErrorIf(t, err != nil, "non-ignored file should remain: %v", err)
	}
}

// `.flate-git-revision` marker survives ApplyIgnore (the default
// sourceignore patterns don't match `.flate-*`) and lets the next
// Fetch validate the cache without `.git/` — which the ignore step
// itself wipes via the VCS-excludes preset.
func TestFetcher_CommitHitSurvivesIgnore(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	patterns := "/*\n!/hello.txt\n"
	commit := mustHead(t, src)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		Ignore:    &patterns,
	}
	f := &Fetcher{Network: networktest.New(t), Cache: cache}
	var work [2]manifest.WorkRecorder
	for side := range work {
		work[side].Key = manifest.WorkKey{
			Tree:  manifest.TreeID{Cluster: "cluster", Side: manifest.TreeSide(side)},
			Owner: repo.Named(), Source: manifest.BootstrapSourceID,
		}
	}
	art, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, (testInputs{}).readSet(repo), &sourcecache.Policy{
		MaxAge: repo.SourceInterval(), Work: &manifest.AcquisitionWork{Scope: (&work[0]).Scope()},
	}, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, statErr := fs.Stat(art.FS, ".git"); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("git metadata visible: %v", statErr)
	}
	if removeErr := os.RemoveAll(src); removeErr != nil {
		t.Fatal(removeErr)
	}
	art2, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, (testInputs{}).readSet(repo), &sourcecache.Policy{
		MaxAge: repo.SourceInterval(), Work: &manifest.AcquisitionWork{Scope: (&work[1]).Scope()},
	}, nil)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != art.Revision {
		t.Errorf("warm Fetch returned a different revision: %q vs %q", art2.Revision, art.Revision)
	}
	for _, artifact := range []*store.SourceArtifact{art, art2} {
		data, err := fs.ReadFile(artifact.FS, "hello.txt")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "hi")
	}
	for side := range work {
		counts := map[string]manifest.WorkCounts{}
		{
			records := work[side].Scope().Snapshot(0).Normalize()
			for recordIndex := range records {
				record := &records[recordIndex]
				assert.Equal(t, record.Key.Tree, work[side].Key.Tree)
				assert.Equal(t, record.Key.Owner, repo.Named())
				assert.Equal(t, record.Key.Source, repo.Named())
				if record.Key.Phase == manifest.WorkAcquisition || record.Key.Phase == manifest.WorkCache {
					assert.Equal(t, record.Key.Input, art.Key)
				}
				sum := counts[string(record.Key.Phase)]
				sum.Add(&record.Counts)
				counts[string(record.Key.Phase)] = sum
			}
		}
		wantAcquisition := manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
		wantCache := manifest.WorkCounts{Attempts: 1, Completed: 1}
		if side == 1 {
			wantAcquisition = manifest.WorkCounts{}
			wantCache.CacheService = 1
		}
		assert.Equal(t, counts[string(manifest.WorkAcquisition)], wantAcquisition)
		assert.Equal(t, counts[string(manifest.WorkCache)], wantCache)
	}
}

func mustInitRepoWithFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	r := testutil.GitInit(t, dir)
	for pathValue, content := range files {
		testutil.WriteFile(t, dir, pathValue, content)
	}
	testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

// mustTagHEAD creates an annotated tag pointing at the worktree HEAD
// and returns the tagged commit SHA.
func mustTagHEAD(t *testing.T, dir, tag string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	assert.FatalIf(t, err != nil, "PlainOpen: %v", err)
	head, err := r.Head()
	assert.FatalIf(t, err != nil, "Head: %v", err)
	{
		_, err := r.CreateTag(tag, head.Hash(), nil)
		assert.FatalIf(t, err != nil, "CreateTag: %v", err)
	}
	return head.Hash().String()
}

func mustHead(t *testing.T, dir string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	assert.FatalIf(t, err != nil, "PlainOpen: %v", err)
	head, err := r.Head()
	assert.FatalIf(t, err != nil, "Head: %v", err)
	return head.Hash().String()
}

func mustSetRefToHash(t *testing.T, dir, refName, hash string) {
	t.Helper()
	r, err := git.PlainOpen(dir)
	assert.FatalIf(t, err != nil, "PlainOpen: %v", err)
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), plumbing.NewHash(hash))
	{
		err := r.Storer.SetReference(ref)
		assert.FatalIf(t, err != nil, "SetReference %s: %v", refName, err)
	}
}

func mustCommitFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	assert.FatalIf(t, err != nil, "PlainOpen: %v", err)
	full := filepath.Join(dir, name)
	{
		err := os.WriteFile(full, []byte(content), 0o600)
		assert.FatalIf(t, err != nil, "write %s: %v", name, err)
	}
	hash := testutil.GitCommitAll(t, r, "update "+name, &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(1, 0)},
	})
	return hash.String()
}

func mustInitRepo(t *testing.T, dir string) {
	t.Helper()
	r := testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "hello.txt", "hi")
	testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

func mustSetRefToHEAD(t *testing.T, dir, refName string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	assert.FatalIf(t, err != nil, "PlainOpen: %v", err)
	head, err := r.Head()
	assert.FatalIf(t, err != nil, "Head: %v", err)
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), head.Hash())
	{
		err := r.Storer.SetReference(ref)
		assert.FatalIf(t, err != nil, "SetReference %s: %v", refName, err)
	}
	return head.Hash().String()
}

func malformedViewRepo(t *testing.T) string {
	t.Helper()
	fixture := localFixture(t)

	commit, err := fixture.repo.CommitObject(fixture.head)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries[0].Name = "bad/name"
	tree.Entries = append(
		[]object.TreeEntry{{Name: "a-module", Mode: filemode.Submodule, Hash: fixture.head}},
		tree.Entries...)
	var encoded plumbing.EncodedObject = testutil.GitMalformedTree(t, tree, 1)
	commit.TreeHash, err = fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = fixture.repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	fixture.head, err = fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	head, err := fixture.repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, fixture.repo.Storer.SetReference(plumbing.NewHashReference(head.Name(), fixture.head)))
	return fixture.root
}

func TestFetcher_GenerationAdmission(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			t.Parallel()
			for _, invalid := range []string{"metadata", "ignore"} {
				t.Run(invalid, func(t *testing.T) {
					t.Parallel()
					src := t.TempDir()
					if invalid == "metadata" {
						src = malformedViewRepo(t)
					} else {
						mustInitRepoWithFiles(t, src, map[string]string{".sourceignore/bad": "not a file"})
					}
					root := ""
					if disk {
						root = t.TempDir()
					}
					f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, root, sourcecache.New)}
					repo := &manifest.GitRepository{URL: "file://" + src}
					policy := sourcecache.Policy{MaxAge: time.Hour}
					artifact, err := fetchtest.Fetch(
						t.Context(),
						f.Fetch,
						repo,
						(testInputs{}).readSet(repo),
						&policy,
						nil,
					)
					assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) ||
						invalid == "metadata" &&
							(artifact == nil || artifact.FS != nil || len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule || artifact.Warnings[0].Resource != repo.Named()) ||
						invalid == "ignore" && artifact != nil, "cold invalid view = %v, %v", artifact, err)
					assert.NoError(t, os.RemoveAll(src))
					mustInitRepo(t, src)
					artifact, err = fetchtest.Fetch(
						t.Context(),
						f.Fetch,
						repo,
						(testInputs{}).readSet(repo),
						&policy,
						nil,
					)
					assert.NoError(t, err)
					assert.Equal(t, artifact.Revision, mustHead(t, src))
					assertWorktree(t, artifact.FS, "hello.txt", "hi")
				})
			}
			t.Run("waiter and hit", func(t *testing.T) {
				t.Parallel()
				testGenerationWaiter(
					t,
					disk,
					func(ctx context.Context, f *Fetcher, url string) (*store.SourceArtifact, error) {
						return fetchtest.Fetch(ctx, f.Fetch, &manifest.GitRepository{
							URL: url, SparseCheckout: []string{"apps"}, Ignore: new("*.tmp\n"),
						}, (testInputs{}).readSet(&manifest.GitRepository{
							URL: url, SparseCheckout: []string{"apps"}, Ignore: new("*.tmp\n"),
						}), &sourcecache.Policy{MaxAge: time.Hour}, nil)
					},
					[]string{"apps/keep.yaml"},
				)
			})
		})
	}
}

type acquisitionWaitContext struct {
	context.Context //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

	waiting chan struct{}
	once    sync.Once
}

func (c *acquisitionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
