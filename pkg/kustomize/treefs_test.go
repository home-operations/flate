package kustomize

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func memoryTree(t testing.TB, files map[string]string) *tree.Tree {
	t.Helper()
	b := tree.NewBuilder(nil)
	for name, content := range files {
		if err := b.AddFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	lower, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return lower
}

func treeRef(lower *tree.Tree, key string) *store.SourceArtifact {
	return &store.SourceArtifact{FS: lower, Key: key}
}

func TestTreeFS_MethodMatrix(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
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

func TestRenderFlux_NoConsumerDiskReads(t *testing.T) {
	label := writeTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	lower := memoryTree(t, map[string]string{"cm.yaml": cmYAML("tree")})
	source := &store.SourceArtifact{LocalPath: label, FS: lower, Key: "pinned"}
	cache := NewTreeCache()
	if err := os.RemoveAll(label); err != nil {
		t.Fatal(err)
	}
	out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: source, Label: label, SubPath: ".", Spec: demoRawSpec})
	if err != nil || !strings.Contains(string(out), "name: tree") {
		t.Fatalf("virtual render = %s, %v", out, err)
	}
}

func TestRenderFlux_AutoGenerateEscape(t *testing.T) {
	for _, target := range []string{"../present", "../absent", "/virtual/base"} {
		t.Run(target, func(t *testing.T) {
			b := tree.NewBuilder(memoryTree(t, map[string]string{"base/cm.yaml": cmYAML("safe")}))
			if err := b.AddSymlink("escape.yaml", target); err != nil {
				t.Fatal(err)
			}
			lower, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
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
			_, err = RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Source: treeRef(lower, "pin"), Label: "/virtual", SubPath: ".", Spec: demoRawSpec})
			if !errors.Is(err, tree.ErrEscape) {
				t.Fatalf("auto-generation = %v", err)
			}
		})
	}
}

func TestRenderFlux_ExoticNames(t *testing.T) {
	var want []byte
	for _, name := range []string{"plain.yaml", "a b+c.yaml", "@x.yaml", "é.yaml"} {
		files := map[string]string{"base/" + name: cmYAML("same"), "base/kustomization.yaml": existingKustomization("./" + name), "app/kustomization.yaml": existingKustomization("../base")}
		lower := memoryTree(t, files)
		out, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Source: treeRef(lower, "pin"), Label: "/absent", SubPath: "app", Spec: demoRawSpec})
		if err != nil {
			t.Fatal(err)
		}
		if want == nil {
			want = out
		}
		if !bytes.Equal(out, want) {
			t.Fatalf("%s changed output:\n%s", name, out)
		}
	}
}

func TestRenderFlux_SubPathResolvesLinksBeforeParents(t *testing.T) {
	b := tree.NewBuilder(memoryTree(t, map[string]string{"base/cm.yaml": cmYAML("wrong"), "deep/base/cm.yaml": cmYAML("correct"), "deep/inner/file": "inner"}))
	if err := b.AddSymlink("alias", "deep/inner"); err != nil {
		t.Fatal(err)
	}
	if err := b.AddSymlink("loop", "loop"); err != nil {
		t.Fatal(err)
	}
	lower, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		subPath string
		want    error
	}{
		{"alias/../base", nil}, {"loop", tree.ErrLoop}, {"../outside", tree.ErrEscape},
	} {
		t.Run(tc.subPath, func(t *testing.T) {
			out, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Source: treeRef(lower, "pin"), Label: "/virtual", SubPath: tc.subPath, Spec: demoRawSpec})
			if !errors.Is(err, tc.want) {
				t.Fatalf("subpath = %v, want %v", err, tc.want)
			}
			if err == nil && !strings.Contains(string(out), "name: correct") {
				t.Fatalf("resolved output = %s", out)
			}
		})
	}
}

func TestRenderFlux_ConcurrentPrivateOverlays(t *testing.T) {
	lower := memoryTree(t, map[string]string{"cm.yaml": cmYAML("shared")})
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			cache := NewTreeCache()
			start := make(chan struct{})
			var wg sync.WaitGroup
			for n := range concurrency {
				wg.Go(func() {
					<-start
					spec := ksDoc(map[string]any{"namePrefix": fmt.Sprintf("render%d-", n)})
					out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: treeRef(lower, "pin"), Label: "/absent", SubPath: ".", Spec: spec})
					if err != nil || !strings.Contains(string(out), fmt.Sprintf("name: render%d-shared", n)) {
						t.Errorf("private output = %s, %v", out, err)
					}
				})
			}
			close(start)
			wg.Wait()
			if _, err := lower.Stat("kustomization.yaml"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("generated file mutated shared source")
			}
		})
	}
}

func TestRenderFlux_InputOrigins(t *testing.T) {
	for _, sub := range []string{"app", "generated"} {
		t.Run(sub, func(t *testing.T) {
			files := map[string]string{
				"base/cm.yaml": cmYAML("base"), "base/kustomization.yaml": existingKustomization("./cm.yaml"),
				"app/kustomization.yaml": existingKustomization("../base", "./local.yaml"), "app/local.yaml": cmYAML("local"),
				"generated/a space+@é.yaml": cmYAML("exotic"),
			}
			root := writeTree(t, files)
			lower := memoryTree(t, files)
			spec := ksDoc(map[string]any{"namePrefix": "merged-"})
			want, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Label: root, ApplyIgnore: false, SubPath: sub, Spec: spec})
			if err != nil {
				t.Fatal(err)
			}
			cache := NewTreeCache()
			cache.SetRenderCache(t.TempDir(), 1<<20)
			source := &store.SourceArtifact{FS: lower, Key: "pin+rules+options+auth", LocalPath: root}
			for _, state := range []string{"cold", "warm", "host-absent"} {
				if state == "host-absent" {
					if err := os.RemoveAll(root); err != nil {
						t.Fatal(err)
					}
				}
				got, err := RenderFlux(t.Context(), cache, RenderRequest{Source: source, SubPath: sub, Spec: spec})
				if err != nil || !bytes.Equal(got, want) {
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

func TestRenderFlux_ArtifactIdentity(t *testing.T) {
	cache := NewTreeCache()
	cache.SetRenderCache(t.TempDir(), 1<<20)
	for _, pin := range []string{"first", "second"} {
		source := &store.SourceArtifact{Key: pin, LocalPath: "/missing-label", FS: memoryTree(t, map[string]string{"cm.yaml": cmYAML(pin)})}
		out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: source, SubPath: ".", Spec: demoRawSpec})
		if err != nil || !strings.Contains(string(out), "name: "+pin) {
			t.Fatalf("artifact key reused stale render: %s %v", out, err)
		}
	}
}

func TestRenderFlux_SubPathLeadingSlash(t *testing.T) {
	files := map[string]string{"app/cm.yaml": cmYAML("x"), "app/kustomization.yaml": existingKustomization("./cm.yaml")}
	root := writeTree(t, files)
	want, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Label: root, SubPath: "app", Spec: demoRawSpec})
	if err != nil {
		t.Fatal(err)
	}
	for _, subPath := range []string{"/app", "//app", "/./app"} {
		t.Run(subPath, func(t *testing.T) {
			got, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Label: root, SubPath: subPath, Spec: demoRawSpec})
			if err != nil || string(got) != string(want) {
				t.Fatalf("disk render = %q, %v", got, err)
			}
			source := treeRef(memoryTree(t, files), "pin")
			got, err = RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Source: source, Label: "/virtual", SubPath: subPath, Spec: demoRawSpec})
			if err != nil || string(got) != string(want) {
				t.Fatalf("memory render = %q, %v", got, err)
			}
		})
	}
}

func TestRenderFlux_SubPathMustBeDirectory(t *testing.T) {
	files := map[string]string{"app/cm.yaml": cmYAML("x"), "app/kustomization.yaml": existingKustomization("./cm.yaml")}
	root := writeTree(t, files)
	for name, req := range map[string]RenderRequest{
		"disk":   {Label: root, SubPath: "app/cm.yaml", Spec: demoRawSpec},
		"memory": {Source: treeRef(memoryTree(t, files), "pin"), Label: "/virtual", SubPath: "app/cm.yaml", Spec: demoRawSpec},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := RenderFlux(t.Context(), NewTreeCache(), req); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("file spec.path = %v", err)
			}
		})
	}
}

func TestRenderFlux_ArtifactWithoutPayloadOrIdentityRejected(t *testing.T) {
	label := writeTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	for name, source := range map[string]*store.SourceArtifact{
		"released":     {LocalPath: label, Key: "pin"},
		"unidentified": {LocalPath: label, FS: memoryTree(t, map[string]string{"cm.yaml": cmYAML("memory")})},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Source: source, SubPath: ".", Spec: demoRawSpec})
			if !errors.Is(err, manifest.ErrInput) || strings.Contains(string(out), "host") {
				t.Fatalf("render = %q, %v", out, err)
			}
		})
	}
}

func TestRenderFlux_CacheMissesOnDirectoryMembership(t *testing.T) {
	root := writeTree(t, map[string]string{"app/a.yaml": cmYAML("a")})
	cache := NewTreeCache()
	cache.SetRenderCache(t.TempDir(), 1<<20)
	req := RenderRequest{Label: root, SubPath: "app", Spec: demoRawSpec}
	first, err := RenderFlux(t.Context(), cache, req)
	if err != nil || !strings.Contains(string(first), "name: a") {
		t.Fatalf("first = %q, %v", first, err)
	}
	testutil.WriteFile(t, root, "app/b.yaml", cmYAML("b"))
	second, err := RenderFlux(t.Context(), cache, req)
	if err != nil || !strings.Contains(string(second), "name: b") {
		t.Fatalf("a file added to an auto-generated directory replayed a stale render: %q, %v", second, err)
	}
}
