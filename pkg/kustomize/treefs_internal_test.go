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
	"testing"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
	return &store.SourceArtifact{FS: lower, Key: key, Content: key}
}

func TestTreeFS_MethodMatrix(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(memoryTree(t, map[string]string{"base/file": "data", "other/file": "other"}))
	for name, target := range map[string]string{
		"dir": "base", "parent": "base/../other", "file": "base/file",
		"dangling": "absent", "loop": "loop", "escape": "../host", "absolute": "/virtual/root/base",
	} {
		{
			err := b.AddSymlink(name, target)
			assert.NoError(t, err)
		}
	}
	for n := 1; n <= 9; n++ {
		target := "base/file"
		if n > 1 {
			target = fmt.Sprintf("chain%d", n-1)
		}
		{
			err := b.AddSymlink(fmt.Sprintf("chain%d", n), target)
			assert.NoError(t, err)
		}
	}
	lower, err := b.Build()
	assert.NoError(t, err)
	adapter := newTreeFS(lower, "/virtual/root", nil)
	for _, tc := range []struct {
		name, content string
		want          error
	}{
		{"base/file", "data", nil},
		{"dir/file", "data", nil},
		{"file", "data", nil},
		{"parent/file", "other", nil},
		{"dir/../other/file", "other", nil},
		{"chain8", "data", nil},
		{"chain9", "", tree.ErrLoop},
		{"loop", "", tree.ErrLoop},
		{"dangling", "", fs.ErrNotExist},
		{"escape", "", tree.ErrEscape},
		{"absolute", "", tree.ErrEscape},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testTreeFSMethodMatrixCase(t, &testTreeFSMethodMatrixCaseInputs{adapter: &adapter, tc: &tc})
		},
		)
	}
	{
		_, readFileErr2 := adapter.ReadFile("/virtual/root/base")
		assert.FatalIf(t, !errors.Is(readFileErr2, fs.ErrInvalid), "directory read = %v", readFileErr2)
	}
	{
		_, readDirErr := adapter.ReadDir("/virtual/root/base/file")
		assert.FatalIf(t, !errors.Is(readDirErr, fs.ErrInvalid), "file listing = %v", readDirErr)
	}
	entries, err := adapter.ReadDir("/virtual/root/dir")
	{
		assert.FatalIf(t, err != nil || !slices.Equal(entries, []string{"file"}), "ReadDir = %v, %v", entries, err)
	}
	entries[0] = "mutated"
	entries, err = adapter.ReadDir("/virtual/root/dir")
	{
		assert.FatalIf(t, err != nil || entries[0] != "file", "%s", "ReadDir exposed shared names")
	}
}

func TestTreeFS_PrivateWritesAndMergedWalk(t *testing.T) {
	t.Parallel()
	lower := memoryTree(t, map[string]string{"app/old": "old", "keep/file": "keep"})
	a := newTreeFS(lower, "/virtual", nil)
	{
		err := a.WriteFile("/virtual/app/generated", []byte("new"))
		assert.NoError(t, err)
	}
	var walked []string
	{
		err := a.Walk("/virtual", func(name string, _ fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			walked = append(walked, name)
			if name == "/virtual/app" {
				return filepath.SkipDir
			}
			return nil
		})
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(
			t,
			slices.Contains(walked, "/virtual/app/old") || slices.Contains(walked, "/virtual/app/generated") ||
				!slices.Contains(walked, "/virtual/keep/file"),
			"walk = %v",
			walked,
		)
	}
	{
		err := a.WriteFile("/virtual/app", nil)
		assert.FatalIf(t, !errors.Is(err, tree.ErrConflict), "write directory = %v", err)
	}
	{
		data, err := lower.ReadFile("app/old")
		assert.FatalIf(t, err != nil || string(data) != "old", "%s", "lower tree changed")
	}
	{
		_, err := lower.Stat("app/generated")
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "%s", "write reached source")
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
			fromKyamlGot, err := fromKyaml(tc.root, tc.name, tc.separator)
			{
				assert.FatalIf(t, err != nil || fromKyamlGot != tc.want, "fromKyaml = %q, %v", fromKyamlGot, err)
			}
			{
				got := toKyaml(tc.root, fromKyamlGot, tc.separator)
				assert.FatalIf(t, got != tc.name, "round trip = %q", got)
			}
		})
	}
	{
		_, err := fromKyaml("/root", "/rooted/file", '/')
		assert.FatalIf(t, !errors.Is(err, tree.ErrEscape), "prefix boundary = %v", err)
	}
}

func TestRender_NoConsumerDiskReads(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	lower := memoryTree(t, map[string]string{"cm.yaml": cmYAML("tree")})
	source := &store.SourceArtifact{LocalPath: label, FS: lower, Key: "pinned"}
	cache := newTestTreeCache(t, "")
	{
		err := os.RemoveAll(label)
		assert.NoError(t, err)
	}
	out, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			cache,
			RenderRequest{Source: source, Label: label, SubPath: ".", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err != nil || !hasRenderField(out, "name", "tree"), "virtual render = %s, %v", out, err)
	}
}

func TestRender_AutoGenerateEscape(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"../present", "../absent", "/virtual/base"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			b := tree.NewBuilder(memoryTree(t, map[string]string{"base/cm.yaml": cmYAML("safe")}))
			{
				err := b.AddSymlink("escape.yaml", target)
				assert.NoError(t, err)
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
			{
				assert.FatalIf(
					t,
					!errors.Is(err, tree.ErrEscape) || called,
					"secure Walk = %v, escape callback = %v",
					err,
					called,
				)
			}
			_, _, err = testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{Source: treeRef(lower, "pin"), Label: "/virtual", SubPath: ".", Spec: demoSpec},
					sourcecache.RetryConfig{},
				),
			)
			{
				assert.FatalIf(t, !errors.Is(err, tree.ErrEscape), "auto-generation = %v", err)
			}
		})
	}
}

func TestRender_ExoticNames(t *testing.T) {
	t.Parallel()
	var want []map[string]any
	for _, name := range []string{"plain.yaml", "a b+c.yaml", "@x.yaml", "é.yaml"} {
		files := map[string]string{
			"base/" + name:            cmYAML("same"),
			"base/kustomization.yaml": existingKustomization("./" + name),
			"app/kustomization.yaml":  existingKustomization("../base"),
		}
		lower := memoryTree(t, files)
		out, _, err := testutil.RenderMaps(
			Render(
				t.Context(),
				newTestTreeCache(t, ""),
				RenderRequest{Source: treeRef(lower, "pin"), Label: "/absent", SubPath: "app", Spec: demoSpec},
				sourcecache.RetryConfig{},
			),
		)
		assert.NoError(t, err)
		if want == nil {
			want = out
		}
		{
			assert.FatalIf(t, !reflect.DeepEqual(out, want), "%s changed output:\n%s", name, out)
		}
	}
}

func TestRender_SubPathResolvesLinksBeforeParents(t *testing.T) {
	t.Parallel()
	b := tree.NewBuilder(memoryTree(t, map[string]string{
		"base/cm.yaml": cmYAML("wrong"), "deep/base/cm.yaml": cmYAML("correct"), "deep/inner/file": "inner",
		"escape/kustomization.yaml": existingKustomization("../../../../../../../../etc/passwd"),
	}))
	{
		err := b.AddSymlink("alias", "deep/inner")
		assert.NoError(t, err)
	}
	{
		err := b.AddSymlink("loop", "loop")
		assert.NoError(t, err)
	}
	lower, err := b.Build()
	assert.NoError(t, err)
	for _, tc := range []struct {
		subPath string
		want    error
	}{
		{"alias/../base", nil},
		{"loop", tree.ErrLoop},
		{"../outside", tree.ErrEscape},
		{"escape", tree.ErrEscape},
	} {
		t.Run(tc.subPath, func(t *testing.T) {
			t.Parallel()
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{
						Source:  treeRef(lower, "pin"),
						Label:   "/virtual",
						SubPath: tc.subPath,
						Spec:    demoSpec,
					},
					sourcecache.RetryConfig{},
				),
			)
			assert.FatalIf(t, !errors.Is(err, tc.want), "subpath = %v, want %v", err, tc.want)
			{
				assert.FatalIf(t, err == nil && !hasRenderField(out, "name", "correct"), "resolved output = %s", out)
			}
		})
	}
}

func TestRender_ConcurrentPrivateOverlays(t *testing.T) {
	t.Parallel()
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
		"kustomization.yaml": existingKustomization(
			"cm.yaml",
		) + "generators: [generator.yaml]\npatchesStrategicMerge:\n- |\n" +
			"  apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: shared}\n  data: {marker: patched}\n",
		"generator.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [payload=" + srv.URL + "/file.txt]\n",
	}
	for name, artifact := range map[string]*store.SourceArtifact{
		"immutable": treeRef(memoryTree(t, files), "pin"),
		"local":     localSource(t, testutil.WriteTree(t, files)),
	} {
		for _, workers := range []int{2, 4} {
			t.Run(fmt.Sprintf("%s/workers%d", name, workers), func(t *testing.T) {
				t.Parallel()
				testRenderConcurrentPrivateOverlaysCase(
					t,
					&testRenderConcurrentPrivateOverlaysCaseInputs{
						scopeCases: &scopeCases,
						files:      &files,
						artifact:   &artifact,
						workers:    &workers,
					},
				)
			},
			)
		}
	}
}

func TestRender_InputOrigins(t *testing.T) {
	t.Parallel()
	for _, sub := range []string{"app", "generated"} {
		t.Run(sub, func(t *testing.T) {
			t.Parallel()
			testRenderInputOriginsCase(t, &sub)
		},
		)
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
		source := &store.SourceArtifact{
			Key:       pin.key,
			Revision:  pin.revision,
			Digest:    pin.digest,
			LocalPath: "/missing-label",
			FS:        memoryTree(t, map[string]string{"cm.yaml": cmYAML(pin.name)}),
		}
		out, _, err := testutil.RenderMaps(
			Render(
				t.Context(),
				cache,
				RenderRequest{Source: source, SubPath: ".", Spec: demoSpec},
				sourcecache.RetryConfig{},
			),
		)
		{
			assert.FatalIf(
				t,
				err != nil || !hasRenderField(out, "name", pin.name),
				"artifact identity reused stale render: %s %v",
				out,
				err,
			)
		}
	}
}

func TestRender_SubPathLeadingSlash(t *testing.T) {
	t.Parallel()
	files := map[string]string{"app/cm.yaml": cmYAML("x"), "app/kustomization.yaml": existingKustomization("./cm.yaml")}
	root := testutil.WriteTree(t, files)
	want, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "app", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.NoError(t, err)
	}
	for _, subPath := range []string{"/app", "//app", "/./app"} {
		t.Run(subPath, func(t *testing.T) {
			t.Parallel()
			got, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{Source: localSource(t, root), Label: root, SubPath: subPath, Spec: demoSpec},
					sourcecache.RetryConfig{},
				),
			)
			{
				assert.FatalIf(t, err != nil || !reflect.DeepEqual(got, want), "disk render = %q, %v", got, err)
			}
			source := treeRef(memoryTree(t, files), "pin")
			got, _, err = testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{Source: source, Label: "/virtual", SubPath: subPath, Spec: demoSpec},
					sourcecache.RetryConfig{},
				),
			)
			{
				assert.FatalIf(t, err != nil || !reflect.DeepEqual(got, want), "memory render = %q, %v", got, err)
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
			{
				_, _, err := testutil.RenderMaps(
					Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}),
				)
				assert.FatalIf(t, !errors.Is(
					err,
					manifest.ErrInput,
				), "file spec.path = %v", err)
			}
		})
	}
}

func TestRender_ReleasedPinRejectsHostContent(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	artifact := &store.SourceArtifact{LocalPath: label, Key: "pin"}
	out, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: artifact, Label: label, SubPath: ".", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || hasRenderField(out, "name", "host"),
			"released content used host resource: %q, %v",
			out,
			err,
		)
	}
}

type testTreeFSMethodMatrixCaseInputs struct {
	adapter **treeFS
	tc      *struct {
		name    string
		content string
		want    error
	}
}

func testTreeFSMethodMatrixCase(t *testing.T, testCaseInput *testTreeFSMethodMatrixCaseInputs) {
	t.Helper()

	name := "/virtual/root/" + testCaseInput.tc.name
	data, readFileErr := (*testCaseInput.adapter).ReadFile(name)
	{
		assert.FatalIf(
			t,
			!errors.Is(readFileErr, testCaseInput.tc.want) || string(data) != testCaseInput.tc.content,
			"ReadFile = %q, %v",
			data,
			readFileErr,
		)
	}
	{
		assert.FatalIf(t, (*testCaseInput.adapter).Exists(name) != (testCaseInput.tc.want == nil) ||
			(*testCaseInput.adapter).IsDir(name), "%s", "probe mismatch")
	}
	file, readFileErr := (*testCaseInput.adapter).Open(name)
	{
		assert.FatalIf(t, !errors.Is(readFileErr, testCaseInput.tc.want), "Open = %v", readFileErr)
	}
	if readFileErr == nil {
		defer file.Close()
		data, readAllErr := io.ReadAll(file)
		{
			assert.FatalIf(
				t,
				readAllErr != nil || string(data) != testCaseInput.tc.content,
				"Open read = %q, %v",
				data,
				readAllErr,
			)
		}
		{
			_, writeErr := file.Write([]byte("mutate"))
			assert.FatalIf(t, !errors.Is(writeErr, fs.ErrPermission), "read-only Write = %v", writeErr)
		}
		info, readAllErr := file.Stat()
		{
			assert.FatalIf(
				t,
				readAllErr != nil || info.Size() != int64(len(testCaseInput.tc.content)) ||
					info.Name() != filepath.Base(name),
				"Stat = %v, %v",
				info,
				readAllErr,
			)
		}
	}
	_, _, readFileErr = (*testCaseInput.adapter).CleanedAbs(name)
	{
		assert.FatalIf(t, !errors.Is(readFileErr, testCaseInput.tc.want), "CleanedAbs = %v", readFileErr)
	}
	if errors.Is(testCaseInput.tc.want, tree.ErrEscape) {
		{
			_, ok := errors.AsType[*fluxfilesys.ConstraintError](readFileErr)
			assert.FatalIf(t, !ok, "escape type = %T", readFileErr)
		}
	}
}

type testRenderConcurrentPrivateOverlaysCaseInputs struct {
	scopeCases *[]struct {
		version   string
		kind      string
		namespace string
	}
	files    *map[string]string
	artifact **store.SourceArtifact
	workers  *int
}
