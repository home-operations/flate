package helm

import (
	"context"
	"encoding/json"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/home-operations/flate/pkg/source/cache"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	chartutil "helm.sh/helm/v4/pkg/chart/v2/util"
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
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: "mychart", RepoName: "repo", RepoNamespace: "default", RepoKind: manifest.KindGitRepository}}
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
	for _, route := range []string{"local", "direct", "alias", "anonymous"} {
		for _, edit := range []string{"template", "values", "values-file", "ignore", "dependency", "dependency-archive", "addition", "deletion"} {
			t.Run(route+"/"+edit, func(t *testing.T) {
				root := t.TempDir()
				testutil.WriteFile(t, root, "Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
				testutil.WriteFile(t, root, "values.yaml", "answer: old\n")
				testutil.WriteFile(t, root, "prod.yaml", "answer: old\n")
				testutil.WriteFile(t, root, "templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: main\ndata:\n  answer: {{ .Values.answer | quote }}\n")
				extra := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: extra\n"
				if edit == "ignore" || edit == "deletion" {
					testutil.WriteFile(t, root, "templates/extra.yaml", extra)
				}
				if edit == "ignore" {
					testutil.WriteFile(t, root, ".helmignore", "templates/extra.yaml\n")
				}
				if edit == "dependency" {
					testutil.WriteFile(t, root, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
					testutil.WriteFile(t, root, "charts/child/templates/cm.yaml", strings.ReplaceAll(extra, "extra", "old"))
				}
				writeArchive := func(marker string) {
					t.Helper()
					child := minimalChart()
					child.Metadata.Name = "child"
					child.Templates[0].Data = []byte(strings.ReplaceAll(extra, "extra", marker))
					name, err := chartutil.Save(child, t.TempDir())
					assert.NoError(t, err)
					data, err := os.ReadFile(name)
					assert.NoError(t, err)
					testutil.WriteFile(t, root, "charts/child.tgz", string(data))
				}
				if edit == "dependency-archive" {
					writeArchive("old")
				}
				var input fs.FS
				switch route {
				case "local":
					var err error
					input, err = source.NewLocalFS(t.Context(), root)
					assert.NoError(t, err)
				case "direct", "alias":
					art, err := gittree.AdmitDirectory(t.Context(), root)
					assert.NoError(t, err)
					input = art.FS
					if route == "alias" {
						input, _, err = gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Current: input})
						assert.NoError(t, err)
					}
				case "anonymous":
					input = os.DirFS(root)
				}
				cli, err := NewClient(newTestCache(t, ""))
				assert.NoError(t, err)
				st := store.New()
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: ".", RepoName: "repo", RepoNamespace: "default", RepoKind: manifest.KindGitRepository}}
				if edit == "values-file" {
					hr.ChartValuesFiles = []string{"prod.yaml"}
				}
				art := &store.SourceArtifact{FS: input, LocalPath: root, InputAlias: route == "alias", Key: "misleading-pin"}
				st.SetArtifact(hr.ChartSource(), art)
				cli.SetSourceResolver(NewStoreSourceResolver(st))
				stat, err := os.Stat(filepath.Join(root, "Chart.yaml"))
				assert.NoError(t, err)
				before, err := cli.Template(t.Context(), hr, nil, Options{})
				assert.NoError(t, err)
				switch edit {
				case "template":
					testutil.WriteFile(t, root, "templates/cm.yaml", strings.ReplaceAll(extra, "extra", "new"))
				case "values":
					testutil.WriteFile(t, root, "values.yaml", "answer: new\n")
				case "values-file":
					testutil.WriteFile(t, root, "prod.yaml", "answer: new\n")
				case "ignore":
					testutil.WriteFile(t, root, ".helmignore", "# include extra\n")
				case "dependency":
					testutil.WriteFile(t, root, "charts/child/templates/cm.yaml", strings.ReplaceAll(extra, "extra", "new"))
				case "dependency-archive":
					writeArchive("new")
				case "addition":
					testutil.WriteFile(t, root, "templates/arrived/cm.yaml", extra)
				case "deletion":
					assert.NoError(t, os.Remove(filepath.Join(root, "templates/extra.yaml")))
				}
				after, err := cli.Template(t.Context(), hr, nil, Options{})
				assert.NoError(t, err)
				if before == after {
					t.Fatalf("independent Template call served stale %s", edit)
				}
				want := "new"
				if edit == "addition" || edit == "ignore" {
					want = "name: extra"
				}
				if edit == "deletion" {
					if strings.Contains(after, "name: extra") {
						t.Fatal("deleted template still rendered")
					}
				} else if !strings.Contains(after, want) {
					t.Fatalf("missing edited output %q: %s", want, after)
				}
				now, err := os.Stat(filepath.Join(root, "Chart.yaml"))
				assert.NoError(t, err)
				if now.Size() != stat.Size() || !now.ModTime().Equal(stat.ModTime()) {
					t.Fatal("regression changed Chart.yaml stat")
				}
				assert.Equal(t, art.Key, "misleading-pin")
			})
		}
	}
}

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
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: ".", RepoName: "repo", RepoNamespace: "default", RepoKind: manifest.KindGitRepository}}
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
	hr := &manifest.HelmRelease{Name: "first", Namespace: "default", Chart: manifest.HelmChart{Name: "one", RepoKind: manifest.KindGitRepository, RepoName: "repo", RepoNamespace: "default"}}
	var previous *chart.Chart
	for _, id := range []string{"git-one", "git-two"} {
		generation, err := owner.Git(t.Context(), cache.GitKey{ID: id}, cache.Policy{MaxAge: cache.Forever}, produce)
		assert.NoError(t, err)
		backing := &chartBacking{Filesystem: generation.Backing}
		input, pin, err := gittree.New(t.Context(), backing, generation.Commit, gittree.ViewOptions{Identity: id + "@" + generation.Commit.String()})
		assert.NoError(t, err)
		assert.Equal(t, pin, id+"@"+generation.Commit.String())
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
