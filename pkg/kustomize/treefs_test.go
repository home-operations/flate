package kustomize

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func memoryTree(tb testing.TB, files map[string]string) *tree.Tree {
	tb.Helper()
	b := tree.NewBuilder(nil)
	for name, content := range files {
		if err := b.AddFile(name, []byte(content), 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	lower, err := b.Build()
	assert.NoError(tb, err)
	return lower
}

func treeRef(lower *tree.Tree, key string) *store.SourceArtifact {
	return &store.SourceArtifact{FS: lower, Key: key}
}

func TestTreeFS_MethodMatrix(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(memoryTree(t, map[string]string{"base/file": "data", "other/file": "other"}))
	for name, target := range map[string]string{
		"dir": "base", "parent": "base/../other", "file": "base/file",
		"dangling": "absent", "loop": "loop", "escape": "../host", "absolute": "/virtual/root/base",
	} {
		if err := b.AddSymlink(name, target); err != nil {
			t.Fatal(err)
		}
	}
	for n := 1; n <= 9; n++ {
		target := "base/file"
		if n > 1 {
			target = fmt.Sprintf("chain%d", n-1)
		}
		if err := b.AddSymlink(fmt.Sprintf("chain%d", n), target); err != nil {
			t.Fatal(err)
		}
	}
	lower, err := b.Build()
	assert.NoError(t, err)
	adapter := newTreeFS(lower, "/virtual/root", nil)
	for _, tc := range []struct {
		name, content string
		want          error
	}{
		{"base/file", "data", nil}, {"dir/file", "data", nil}, {"file", "data", nil},
		{"parent/file", "other", nil}, {"dir/../other/file", "other", nil},
		{"chain8", "data", nil}, {"chain9", "", tree.ErrLoop}, {"loop", "", tree.ErrLoop},
		{"dangling", "", fs.ErrNotExist}, {"escape", "", tree.ErrEscape}, {"absolute", "", tree.ErrEscape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			name := "/virtual/root/" + tc.name
			data, err := adapter.ReadFile(name)
			if !errors.Is(err, tc.want) || string(data) != tc.content {
				t.Fatalf("ReadFile = %q, %v", data, err)
			}
			if adapter.Exists(name) != (tc.want == nil) || adapter.IsDir(name) {
				t.Fatal("probe mismatch")
			}
			file, err := adapter.Open(name)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open = %v", err)
			}
			if err == nil {
				defer file.Close()
				data, err := io.ReadAll(file)
				if err != nil || string(data) != tc.content {
					t.Fatalf("Open read = %q, %v", data, err)
				}
				if _, err := file.Write([]byte("mutate")); !errors.Is(err, fs.ErrPermission) {
					t.Fatalf("read-only Write = %v", err)
				}
				info, err := file.Stat()
				if err != nil || info.Size() != int64(len(tc.content)) || info.Name() != filepath.Base(name) {
					t.Fatalf("Stat = %v, %v", info, err)
				}
			}
			_, _, err = adapter.CleanedAbs(name)
			if !errors.Is(err, tc.want) {
				t.Fatalf("CleanedAbs = %v", err)
			}
			if errors.Is(tc.want, tree.ErrEscape) {
				if _, ok := errors.AsType[*fluxfilesys.ConstraintError](err); !ok {
					t.Fatalf("escape type = %T", err)
				}
			}
		})
	}
	if _, err := adapter.ReadFile("/virtual/root/base"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("directory read = %v", err)
	}
	if _, err := adapter.ReadDir("/virtual/root/base/file"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("file listing = %v", err)
	}
	entries, err := adapter.ReadDir("/virtual/root/dir")
	if err != nil || !slices.Equal(entries, []string{"file"}) {
		t.Fatalf("ReadDir = %v, %v", entries, err)
	}
	entries[0] = "mutated"
	entries, err = adapter.ReadDir("/virtual/root/dir")
	if err != nil || entries[0] != "file" {
		t.Fatal("ReadDir exposed shared names")
	}
}

func TestTreeFS_PrivateWritesAndMergedWalk(t *testing.T) {
	t.Parallel()
	lower := memoryTree(t, map[string]string{"app/old": "old", "keep/file": "keep"})
	a := newTreeFS(lower, "/virtual", nil)
	if err := a.WriteFile("/virtual/app/generated", []byte("new")); err != nil {
		t.Fatal(err)
	}
	var walked []string
	if err := a.Walk("/virtual", func(name string, _ fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		walked = append(walked, name)
		if name == "/virtual/app" {
			return filepath.SkipDir
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(walked, "/virtual/app/old") || slices.Contains(walked, "/virtual/app/generated") || !slices.Contains(walked, "/virtual/keep/file") {
		t.Fatalf("walk = %v", walked)
	}
	if err := a.WriteFile("/virtual/app", nil); !errors.Is(err, tree.ErrConflict) {
		t.Fatalf("write directory = %v", err)
	}
	if data, err := lower.ReadFile("app/old"); err != nil || string(data) != "old" {
		t.Fatal("lower tree changed")
	}
	if _, err := lower.Stat("app/generated"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("write reached source")
	}
}

func TestTreeFS_MountConversion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		root, name, want string
		separator        byte
	}{
		{"/root", "/root/link/../é +@", "link/../é +@", '/'},
		{"/", "/link/../file", "link/../file", '/'},
		{"/root", `/root/a\b`, `a\b`, '/'},
		{`C:\root`, `C:\root\link\..\é +@`, "link/../é +@", '\\'},
		{`\root`, `\root\a`, "a", '\\'},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := fromKyaml(tc.root, tc.name, tc.separator)
			if err != nil || got != tc.want {
				t.Fatalf("fromKyaml = %q, %v", got, err)
			}
			if got := toKyaml(tc.root, got, tc.separator); got != tc.name {
				t.Fatalf("round trip = %q", got)
			}
		})
	}
	if _, err := fromKyaml("/root", "/rooted/file", '/'); !errors.Is(err, tree.ErrEscape) {
		t.Fatalf("prefix boundary = %v", err)
	}
}

func TestRender_NoConsumerDiskReads(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	lower := memoryTree(t, map[string]string{"cm.yaml": cmYAML("tree")})
	source := &store.SourceArtifact{LocalPath: label, FS: lower, Key: "pinned"}
	cache := newTestTreeCache(t, "")
	if err := os.RemoveAll(label); err != nil {
		t.Fatal(err)
	}
	out, err := Render(t.Context(), cache, RenderRequest{Source: source, Label: label, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out, "name", "tree") {
		t.Fatalf("virtual render = %s, %v", out, err)
	}
}

func TestRender_AutoGenerateEscape(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"../present", "../absent", "/virtual/base"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			b := tree.NewBuilder(memoryTree(t, map[string]string{"base/cm.yaml": cmYAML("safe")}))
			if err := b.AddSymlink("escape.yaml", target); err != nil {
				t.Fatal(err)
			}
			lower, err := b.Build()
			assert.NoError(t, err)
			called := false
			adapter := newTreeFS(lower, "/virtual", nil)
			err = adapter.Walk("/virtual", func(name string, _ fs.FileInfo, err error) error {
				if name == "/virtual/escape.yaml" {
					called = true
				}
				return err
			})
			if !errors.Is(err, tree.ErrEscape) || called {
				t.Fatalf("secure Walk = %v, escape callback = %v", err, called)
			}
			_, err = Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: treeRef(lower, "pin"), Label: "/virtual", SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
			if !errors.Is(err, tree.ErrEscape) {
				t.Fatalf("auto-generation = %v", err)
			}
		})
	}
}

func TestRender_ExoticNames(t *testing.T) {
	t.Parallel()
	var want []map[string]any
	for _, name := range []string{"plain.yaml", "a b+c.yaml", "@x.yaml", "é.yaml"} {
		files := map[string]string{"base/" + name: cmYAML("same"), "base/kustomization.yaml": existingKustomization("./" + name), "app/kustomization.yaml": existingKustomization("../base")}
		lower := memoryTree(t, files)
		out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: treeRef(lower, "pin"), Label: "/absent", SubPath: "app", Spec: demoSpec}, sourcecache.RetryConfig{})
		assert.NoError(t, err)
		if want == nil {
			want = out
		}
		if !reflect.DeepEqual(out, want) {
			t.Fatalf("%s changed output:\n%s", name, out)
		}
	}
}

func TestRender_SubPathResolvesLinksBeforeParents(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(memoryTree(t, map[string]string{
		"base/cm.yaml": cmYAML("wrong"), "deep/base/cm.yaml": cmYAML("correct"), "deep/inner/file": "inner",
		"escape/kustomization.yaml": existingKustomization("../../../../../../../../etc/passwd"),
	}))
	if err := b.AddSymlink("alias", "deep/inner"); err != nil {
		t.Fatal(err)
	}
	if err := b.AddSymlink("loop", "loop"); err != nil {
		t.Fatal(err)
	}
	lower, err := b.Build()
	assert.NoError(t, err)
	for _, tc := range []struct {
		subPath string
		want    error
	}{
		{"alias/../base", nil}, {"loop", tree.ErrLoop}, {"../outside", tree.ErrEscape},
		{"escape", tree.ErrEscape},
	} {
		t.Run(tc.subPath, func(t *testing.T) {
			t.Parallel()
			out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: treeRef(lower, "pin"), Label: "/virtual", SubPath: tc.subPath, Spec: demoSpec}, sourcecache.RetryConfig{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("subpath = %v, want %v", err, tc.want)
			}
			if err == nil && !hasRenderField(out, "name", "correct") {
				t.Fatalf("resolved output = %s", out)
			}
		})
	}
}

func TestRender_ConcurrentPrivateOverlays(t *testing.T) {

	scopeCases := []struct{ version, kind, namespace string }{
		{"v1", "ConfigMap", "apps"},
		{"rbac.authorization.k8s.io/v1", "ClusterRole", ""},
		{"foreign.example/v1", "ClusterRole", "apps"},
		{"rbac.authorization.k8s.io/v999", "ClusterRole", "apps"},
		{"nvidia.com/v1", "ClusterPolicy", "apps"},
	}
	for _, tc := range scopeCases {
		manifest.KindOf(&manifest.RawObject{APIVersion: tc.version, Kind: tc.kind}).Namespaced()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "remote")
	}))
	t.Cleanup(srv.Close)
	files := map[string]string{
		"cm.yaml": cmYAML("shared"),
		"kustomization.yaml": existingKustomization("cm.yaml") + "generators: [generator.yaml]\npatchesStrategicMerge:\n- |\n" +
			"  apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: shared}\n  data: {marker: patched}\n",
		"generator.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [payload=" + srv.URL + "/file.txt]\n",
	}
	for name, artifact := range map[string]*store.SourceArtifact{
		"immutable": treeRef(memoryTree(t, files), "pin"),
		"local":     localSource(t, testutil.WriteTree(t, files)),
	} {
		for _, workers := range []int{2, 4} {
			t.Run(fmt.Sprintf("%s/workers%d", name, workers), func(t *testing.T) {
				cache := newTestTreeCache(t, t.TempDir())
				start, done := make(chan struct{}), make(chan struct{})
				var renders, lookups, lookupReady sync.WaitGroup
				lookupReady.Add(workers)
				var activeRenders atomic.Int32
				iterations := make([]int, workers)
				for n := range workers {
					renders.Go(func() {
						<-start
						lookupReady.Wait()
						original := parsedKS(t, ksDoc(map[string]any{"postBuild": map[string]any{}}))
						projected := original.Clone()
						projected.NamePrefix = fmt.Sprintf("render%d-", n)
						projected.TargetNamespace = fmt.Sprintf("namespace%d", n)
						spec, err := Prepare(projected, nil, map[string]string{"SLOT": fmt.Sprint(n)})
						if err != nil {
							t.Error(err)
							return
						}
						activeRenders.Add(1)
						defer activeRenders.Add(-1)
						out, err := Render(t.Context(), cache, RenderRequest{Source: artifact, Label: "/absent", SubPath: ".", Spec: spec}, sourcecache.RetryConfig{})
						if err != nil || !hasRenderField(out, "name", fmt.Sprintf("render%d-shared", n)) ||
							!hasRenderField(out, "marker", "patched") || !hasRenderField(out, "payload", "remote") ||
							!hasRenderField(out, "namespace", fmt.Sprintf("namespace%d", n)) || spec.PostBuild.Substitute["SLOT"] != fmt.Sprint(n) ||
							original.TargetNamespace != "" || len(original.PostBuild.Substitute) != 0 {
							t.Errorf("private output = %s, %v", out, err)
						}
					})
					lookups.Go(func() {
						<-start
						for batch := 0; ; batch++ {
							select {
							case <-done:
								return
							default:
							}
							duringRender := activeRenders.Load() != 0
							for _, tc := range scopeCases {
								doc := map[string]any{"apiVersion": tc.version, "kind": tc.kind, "metadata": map[string]any{"name": "scope"}}
								obj, authoritative, changed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: "apps"})
								if err != nil {
									t.Error(err)
									return
								}
								_, ns := manifest.DocMetadata(authoritative)
								if obj.Named().Namespace != tc.namespace || ns != tc.namespace || changed != (tc.namespace != "") {
									t.Errorf("%s %s: identity=%s document=%q changed=%t", tc.version, tc.kind, obj.Named(), ns, changed)
									return
								}
								r := &manifest.ResourceSet{Namespace: "apps"}
								r.DependsOn = []fluxopv1.Dependency{{APIVersion: tc.version, Kind: tc.kind, Name: "scope"}}
								for ref := range manifest.References(r) {
									if ref.Namespace != tc.namespace {
										t.Errorf("%s %s dependency namespace=%q, want %q", tc.version, tc.kind, ref.Namespace, tc.namespace)
										return
									}
								}
							}
							if batch == 0 {
								lookupReady.Done()
							}
							if duringRender && activeRenders.Load() != 0 {
								iterations[n]++
							}
						}
					})
				}
				close(start)
				renders.Wait()
				close(done)
				lookups.Wait()
				for n, count := range iterations {
					t.Logf("lookup worker %d: %d complete scope batches during render workers", n, count)
					if count == 0 {
						t.Errorf("lookup worker %d did no scope work during render workers", n)
					}
				}
				for path, want := range files {
					got, err := fs.ReadFile(artifact.FS, path)
					if err != nil || string(got) != want {
						t.Fatalf("shared source %s mutated: %s, %v", path, got, err)
					}
				}
			})
		}
	}
}

func TestRender_InputOrigins(t *testing.T) {
	t.Parallel()
	for _, sub := range []string{"app", "generated"} {
		t.Run(sub, func(t *testing.T) {
			t.Parallel()
			files := map[string]string{
				"base/cm.yaml": cmYAML("base"), "base/kustomization.yaml": existingKustomization("./cm.yaml"),
				"app/kustomization.yaml": existingKustomization("../base", "./local.yaml"), "app/local.yaml": cmYAML("local"),
				"generated/a space+@é.yaml": cmYAML("exotic"),
			}
			root := testutil.WriteTree(t, files)
			lower := memoryTree(t, files)
			spec := ksDoc(map[string]any{"namePrefix": "merged-"})
			want, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: sub, Spec: parsedKS(t, spec)}, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			cache := newTestTreeCache(t, t.TempDir())
			source := &store.SourceArtifact{FS: lower, Key: "pin+rules+options+auth", LocalPath: root}
			for _, state := range []string{"cold", "warm", "host-absent"} {
				if state == "host-absent" {
					if err := os.RemoveAll(root); err != nil {
						t.Fatal(err)
					}
				}
				got, err := Render(t.Context(), cache, RenderRequest{Source: source, SubPath: sub, Spec: parsedKS(t, spec)}, sourcecache.RetryConfig{})
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s memory output differs: %s, %v", state, got, err)
				}
			}
			if sub == "app" {
				got, err := fs.ReadFile(lower, "app/kustomization.yaml")
				if err != nil || string(got) != files["app/kustomization.yaml"] {
					t.Fatal("generated manifest mutated source")
				}
			}
		})
	}
}

func TestRender_ArtifactIdentity(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, t.TempDir())
	for _, pin := range []struct{ key, revision, digest, name string }{
		{key: "first", name: "first"},
		{key: "second", name: "second"},
		{key: "family", revision: "first", digest: "first", name: "initial"},
		{key: "family", revision: "second", digest: "first", name: "new-revision"},
		{key: "family", revision: "second", digest: "second", name: "new-digest"},
	} {
		source := &store.SourceArtifact{Key: pin.key, Revision: pin.revision, Digest: pin.digest, LocalPath: "/missing-label", FS: memoryTree(t, map[string]string{"cm.yaml": cmYAML(pin.name)})}
		out, err := Render(t.Context(), cache, RenderRequest{Source: source, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
		if err != nil || !hasRenderField(out, "name", pin.name) {
			t.Fatalf("artifact identity reused stale render: %s %v", out, err)
		}
	}
}

func TestRender_SubPathLeadingSlash(t *testing.T) {
	t.Parallel()
	files := map[string]string{"app/cm.yaml": cmYAML("x"), "app/kustomization.yaml": existingKustomization("./cm.yaml")}
	root := testutil.WriteTree(t, files)
	want, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, SubPath: "app", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, subPath := range []string{"/app", "//app", "/./app"} {
		t.Run(subPath, func(t *testing.T) {
			t.Parallel()
			got, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, SubPath: subPath, Spec: demoSpec}, sourcecache.RetryConfig{})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("disk render = %q, %v", got, err)
			}
			source := treeRef(memoryTree(t, files), "pin")
			got, err = Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: source, Label: "/virtual", SubPath: subPath, Spec: demoSpec}, sourcecache.RetryConfig{})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("memory render = %q, %v", got, err)
			}
		})
	}
}

func TestRender_SubPathMustBeDirectory(t *testing.T) {
	t.Parallel()
	files := map[string]string{"app/cm.yaml": cmYAML("x"), "app/kustomization.yaml": existingKustomization("./cm.yaml")}
	root := testutil.WriteTree(t, files)
	for name, req := range map[string]RenderRequest{
		"disk":   {Source: localSource(t, root), Label: root, SubPath: "app/cm.yaml", Spec: demoSpec},
		"memory": {Source: treeRef(memoryTree(t, files), "pin"), Label: "/virtual", SubPath: "app/cm.yaml", Spec: demoSpec},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("file spec.path = %v", err)
			}
		})
	}
}

func TestRender_ReleasedPinRejectsHostContent(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	artifact := &store.SourceArtifact{LocalPath: label, Key: "pin"}
	out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: artifact, Label: label, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
	if !errors.Is(err, manifest.ErrInput) || hasRenderField(out, "name", "host") {
		t.Fatalf("released content used host resource: %q, %v", out, err)
	}
}

func TestRender_CacheMissesOnDirectoryMembership(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"app/a.yaml": cmYAML("a")})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: "app", Spec: demoSpec}
	first, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(first, "name", "a") {
		t.Fatalf("first = %q, %v", first, err)
	}
	testutil.WriteFile(t, root, "app/b.yaml", cmYAML("b"))
	second, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(second, "name", "b") {
		t.Fatalf("a file added to an auto-generated directory replayed a stale render: %q, %v", second, err)
	}
}

func TestTreeFS_LocalConstraints(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	testutil.WriteFile(t, root, "folder/value", "inside")
	testutil.WriteFile(t, parent, "outside", "outside")
	testutil.WriteFile(t, parent, "root-sibling/value", "sibling")
	for name, target := range map[string]string{
		"file-link": "folder/value", "directory-link": "folder", "dangling": "missing", "escaping-link": "../outside",
	} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	artifact := localSource(t, root)
	lower, _, err := source.OperationView(t.Context(), artifact.FS)
	if err != nil {
		t.Fatal(err)
	}
	adapter := newTreeFS(lower, root, nil)
	for _, tc := range []struct {
		name, path string
		constraint bool
	}{
		{name: "ordinary read", path: filepath.Join(root, "folder/value")},
		{name: "parent escape", path: root + "/../outside", constraint: true},
		{name: "outside absolute", path: filepath.Join(parent, "outside"), constraint: true},
		{name: "sibling prefix", path: filepath.Join(parent, "root-sibling/value"), constraint: true},
		{name: "file link", path: filepath.Join(root, "file-link")},
		{name: "directory link", path: filepath.Join(root, "directory-link/value")},
		{name: "dangling", path: filepath.Join(root, "dangling")},
		{name: "escaping link", path: filepath.Join(root, "escaping-link")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := adapter.ReadFile(tc.path)
			if tc.constraint {
				if _, ok := errors.AsType[*fluxfilesys.ConstraintError](err); !ok {
					t.Fatalf("lexical escape error = %v; want ConstraintError", err)
				}
			} else if tc.name == "ordinary read" {
				if err != nil || string(data) != "inside" {
					t.Fatalf("ordinary read = %q, %v", data, err)
				}
			} else if !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("denied link read = %q, %v", data, err)
			}
		})
	}
}

func TestRender_CacheInvalidatesTypeTransitions(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"app/a.yaml": cmYAML("a")})
	artifact := localSource(t, root)
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: artifact, SubPath: "app", Spec: demoSpec}
	if _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "app")); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "app", "file")
	if _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{}); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("directory-to-file transition replayed a successful frame: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "app")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(".", filepath.Join(root, "app")); err != nil {
		t.Fatal(err)
	}
	if _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{}); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("directory link replayed a successful frame: %v", err)
	}
}

func TestRender_CacheMissingComponentAppearance(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": "resources: [cm.yaml]\n",
		"cm.yaml":            cmYAML("base"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), SubPath: ".", Spec: parsedKS(t, ksDoc(map[string]any{
		"components": []any{"./missing"}, "ignoreMissingComponents": true,
	}))}
	if _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{}); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "missing/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "missing/cm.yaml", cmYAML("arrived"))
	out, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out, "name", "arrived") {
		t.Fatalf("missing component arrival replayed stale output: %s, %v", out, err)
	}
}

func TestRender_ArtifactWithoutPayloadRejected(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	for _, key := range []string{"", "pin"} {
		t.Run("key="+key, func(t *testing.T) {
			t.Parallel()
			source := &store.SourceArtifact{LocalPath: label, Key: key}
			out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: source, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
			if !errors.Is(err, manifest.ErrInput) || hasRenderField(out, "name", "host") {
				t.Fatalf("render = %q, %v", out, err)
			}
		})
	}
}
