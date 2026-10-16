package helm

import (
	"context"
	"encoding/json"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/home-operations/flate/pkg/source/cache"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

func TestLoadChart_CoalescesConcurrentFirstLoad(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "mutable", true: "pinned"}[pinned], func(t *testing.T) {
			const callers = 16
			root := t.TempDir()
			testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "mychart/templates/cm.yaml", "kind: ConfigMap\n")
			testutil.WriteFile(t, root, "mychart/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			owner := newTestCache(t, "")
			input, err := source.NewLocalFS(t.Context(), root)
			assert.NoError(t, err)
			if pinned {
				input = acquireChartFS(t, owner, input)
			}
			cli, err := NewClient(owner)
			assert.NoError(t, err)
			st := store.New()
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: "mychart", RepoName: "repo", RepoNamespace: "default", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository}}
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input, LocalPath: root, Key: "untrusted"})
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			var charts [callers]*chart.Chart
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range callers {
				wg.Go(func() {
					<-start
					loaded, err := cli.loadChart(t.Context(), hr)
					if err != nil {
						t.Error(err)
						return
					}
					charts[i] = loaded.chart
				})
			}
			close(start)
			wg.Wait()
			for _, ch := range charts {
				if ch == nil || ch != charts[0] {
					t.Fatal("concurrent identical inputs did not share the canonical chart")
				}
			}
			assert.Equal(t, len(cli.chartCache), 1)
		})
	}
}

func TestLoadChart_InvalidatesOnFileMtimeChange(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"mychart/Chart.yaml":                     "apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: first\n",
		"mychart/values.yaml":                    "k: v1\n",
		"mychart/templates/cm.yaml":              "kind: ConfigMap\n",
		"mychart/charts/child/Chart.yaml":        "apiVersion: v2\nname: child\nversion: 0.1.0\ndescription: first\n",
		"mychart/charts/child/templates/cm.yaml": "kind: ConfigMap\n",
	}
	for name, body := range files {
		testutil.WriteFile(t, dir, name, body)
	}
	cli, err := NewClient(newTestCache(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	hr := newHR()
	first, err := cli.loadChart(t.Context(), hr)
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(100, 0)
	for name := range files {
		if err := os.Chtimes(filepath.Join(dir, name), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	touched, err := cli.loadChart(t.Context(), hr)
	if err != nil {
		t.Fatal(err)
	}
	if touched.chart != first.chart || touched.fingerprint != first.fingerprint {
		t.Fatal("unchanged-byte touch reloaded chart")
	}
	for _, tc := range []struct{ name, path, from, to string }{
		{"metadata", "mychart/Chart.yaml", "first", "other"},
		{"defaults", "mychart/values.yaml", "v1", "v2"},
		{"template", "mychart/templates/cm.yaml", "ConfigMap", "Namespace"},
		{"dependency metadata", "mychart/charts/child/Chart.yaml", "first", "other"},
		{"dependency template", "mychart/charts/child/templates/cm.yaml", "ConfigMap", "Namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := cli.loadChart(t.Context(), hr)
			if err != nil {
				t.Fatal(err)
			}
			body := strings.ReplaceAll(files[tc.path], tc.from, tc.to)
			testutil.WriteFile(t, dir, tc.path, body)
			if err := os.Chtimes(filepath.Join(dir, tc.path), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			after, err := cli.loadChart(t.Context(), hr)
			if err != nil {
				t.Fatal(err)
			}
			if after.chart == before.chart || computeTemplateKey(after, nil, Options{}, hr) == computeTemplateKey(before, nil, Options{}, hr) {
				t.Fatal("same-size, same-mtime edit reused chart content")
			}
		})
	}
}

// TestLoadChart_PerCallCloneIsolatesValuesAndDependencies pins that
// each LoadChart call returns a fresh outer *chart.Chart whose
// Values map and Metadata.Dependencies slice are independent of the
// cached canonical AND of every other concurrent caller. Without
// the clone, helm's ProcessDependencies (called by Install.Run)
// mutates Chart.Values and per-Dependency Enabled flags on the
// shared pointer, racing across reconciles.
func TestLoadChart_PerCallCloneIsolatesValuesAndDependencies(t *testing.T) {
	input := fstest.MapFS{
		"Chart.yaml":                     {Data: []byte("apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  condition: child.enabled\n")},
		"values.yaml":                    {Data: []byte("child:\n  enabled: true\n  value: canonical\n")},
		"charts/child/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
		"charts/child/values.yaml":       {Data: []byte("value: canonical\n")},
		"charts/child/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child\ndata:\n  value: {{ .Values.value | quote }}\n")},
	}
	cli, err := NewClient(newTestCache(t, ""))
	assert.NoError(t, err)
	st := store.New()
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: ".", RepoName: "repo", RepoNamespace: "default", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository}}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	canonical, err := cli.loadChart(t.Context(), hr)
	assert.NoError(t, err)
	snapshot := func() string {
		t.Helper()
		data, err := json.Marshal([]any{canonical.chart, canonical.chart.Raw, canonical.chart.Dependencies()[0], canonical.chart.Dependencies()[0].Raw})
		assert.NoError(t, err)
		return string(data)
	}
	before := snapshot()
	for _, enabled := range []bool{false, true, false, true} {
		out, err := cli.Template(t.Context(), hr, map[string]any{"child": map[string]any{"enabled": enabled, "value": "rendered"}}, Options{})
		assert.NoError(t, err)
		assert.Equal(t, strings.Contains(out, "rendered"), enabled)
		assert.Equal(t, snapshot(), before)
		loaded, err := cli.loadChart(t.Context(), hr)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart == canonical.chart, true)
	}
}

type chartBacking struct {
	billy.Filesystem
	opens atomic.Int32
}

func (b *chartBacking) Open(name string) (billy.File, error) {
	b.opens.Add(1)
	return b.Filesystem.Open(name)
}

func TestLoadChart_AcquiredGitWarmReuse(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"one", "two"} {
		testutil.WriteFile(t, root, dir+"/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, dir+"/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: chart\n")
	}
	repo, err := git.PlainInit(root, false)
	assert.NoError(t, err)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	_, err = wt.Add(".")
	assert.NoError(t, err)
	_, err = wt.Commit("chart", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	assert.NoError(t, err)
	owner := newTestCache(t, "")
	produce := func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		repo, err := git.CloneContext(ctx, storer, worktree, &git.CloneOptions{URL: "file://" + root, NoCheckout: true})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		head, err := repo.Head()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		return head.Hash(), nil
	}
	cli, err := NewClient(owner)
	assert.NoError(t, err)
	st := store.New()
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	hr := &manifest.HelmRelease{Name: "first", Namespace: "default", Chart: manifest.HelmChart{Name: "one", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository, RepoName: "repo", RepoNamespace: "default"}}
	var previous *chart.Chart
	for _, id := range []string{"git-one", "git-two"} {
		generation, _, err := owner.Git(t.Context(), cache.GitKey{Content: id}, cache.Policy{MaxAge: cache.Forever}, produce)
		assert.NoError(t, err)
		backing := &chartBacking{Filesystem: generation.Backing}
		input, pin, err := gittree.New(t.Context(), backing, generation.Commit, gittree.ViewOptions{Content: struct{ Source string }{id}})
		assert.NoError(t, err)
		assert.Equal(t, pin == "", false)
		art := &store.SourceArtifact{FS: input, Key: "untrusted", InputAlias: true, LocalPath: root, Revision: id}
		st.SetArtifact(hr.ChartSource(), art)
		first, err := cli.loadChart(t.Context(), hr)
		assert.NoError(t, err)
		if first.chart == previous {
			t.Fatal("different acquisition pins shared parsed entries")
		}
		previous = first.chart
		reads := backing.opens.Load()
		other := hr.Clone()
		other.Name = "second"
		warm, err := cli.loadChart(t.Context(), other)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == first.chart, true)
		assert.Equal(t, backing.opens.Load(), reads)
		other.Chart.Name = "two"
		second, err := cli.loadChart(t.Context(), other)
		assert.NoError(t, err)
		assert.Equal(t, second.chart == first.chart, false)
		assert.Equal(t, art.Key, "untrusted")
	}
}
