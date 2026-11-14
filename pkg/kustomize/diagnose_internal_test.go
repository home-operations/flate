package kustomize

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func fluxKS() *manifest.Kustomization {
	return &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
}

func cmChild(t *testing.T, root, name string, id manifest.NamedResource) {
	t.Helper()
	dir := "apps/" + name
	testutil.WriteFile(t, root, dir+"/kustomization.yaml", "namespace: "+id.Namespace+"\nresources:\n- cm.yaml\n")
	testutil.WriteFile(t, root, dir+"/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+id.Name+"}\ndata: {k: v}\n")
}

func TestRender_DuplicateID_NamesProducers(t *testing.T) {
	t.Parallel()
	cm := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup, namespace: shared}\ndata: {k: v}\n"
	alpha := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: alpha}\n"
	zeta := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: zeta}\n"
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name: "directories",
			files: map[string]string{
				"a/kustomization.yaml": "namespace: shared\nresources: [cm.yaml]\n",
				"a/cm.yaml":            cm,
				"b/kustomization.yaml": "namespace: shared\nresources: [cm.yaml]\n",
				"b/cm.yaml":            cm,
			},
			want: "    ConfigMap.v1.[noGrp]/dup.shared\n      - ./apps/a\n      - ./apps/b",
		},
		{
			name: "mixed",
			files: map[string]string{
				"one/kustomization.yaml": "namespace: default\nresources: [cm.yaml]\n",
				"one/cm.yaml":            cmYAML("shared"),
				"two/kustomization.yaml": "namespace: default\nresources: [cm.yaml]\n",
				"two/cm.yaml":            cmYAML("shared"),
				"loose.yaml":             "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\n",
			},
			want: "    ConfigMap.v1.[noGrp]/shared.default\n      - ./apps/loose.yaml\n      - ./apps/one\n      - ./apps/two",
		},
		{
			name:  "document_stream",
			files: map[string]string{"one.yaml": zeta + "---\n" + alpha, "two.yaml": alpha + "---\n" + zeta},
			want: "    ConfigMap.v1.[noGrp]/alpha.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml\n" +
				"    ConfigMap.v1.[noGrp]/zeta.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml",
		},
		{
			name: "kubernetes_list",
			files: map[string]string{
				"one.yaml": "apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: zeta}\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: alpha}\n",
				"two.yaml": alpha + "---\n" + zeta,
			},
			want: "    ConfigMap.v1.[noGrp]/alpha.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml\n" +
				"    ConfigMap.v1.[noGrp]/zeta.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for name, body := range tc.files {
				testutil.WriteFile(t, root, "apps/"+name, body)
			}
			assertDuplicateReport(t, root, tc.want)
		})
	}
}

func assertDuplicateReport(t *testing.T, root, want string) {
	t.Helper()
	var first string
	for run := range 2 {
		_, _, err := Render(
			t.Context(), newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		)
		assert.FatalIf(t, err == nil, "%s", "expected a duplicate-id build failure")
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
			"duplicate failure lost renderer attribution: %v",
			err,
		)
		assert.ErrorIf(
			t,
			!strings.Contains(err.Error(), "already registered id"),
			"expected the raw duplicate-id error:\n%s",
			err,
		)
		_, diagnostic, ok := strings.Cut(
			err.Error(),
			"\n  duplicate resource(s) produced by multiple accumulated paths:\n",
		)
		assert.FatalIf(t, !ok, "missing diagnostic attribution: %v", err)
		assert.Equal(t, diagnostic, want+"\nselect an individual overlay with -p")
		if run == 0 {
			first = err.Error()
		} else {
			assert.Equal(t, err.Error(), first)
		}
	}
}

func TestRender_DistinctIDs_NoDiagnostic(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "a", manifest.NamedResource{Namespace: "shared", Name: "a-cm"})
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "b-cm"})

	testutil.WriteFile(t, root, "apps/loose.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c-cm, namespace: shared}\ndata: {k: v}\n")
	var first []byte
	for run := range 2 {
		out, _, err := Render(
			t.Context(), newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		)
		assert.FatalIf(t, err != nil, "Render: %v", err)
		assert.Equal(t, len(out), 3)
		encoded, err := manifest.EncodeRender(out)
		assert.NoError(t, err)
		if run == 0 {
			first = encoded
		} else {
			assert.Equal(t, string(encoded), string(first))
		}
	}
}

func TestRender_NonDuplicateError_Unannotated(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")

	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "apps",
				Spec:        fluxKS(),
			},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err == nil, "%s", "expected a build failure")
	assert.ErrorIf(
		t,
		!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
		"build failure lost renderer attribution: %v",
		err,
	)
	assert.ErrorIf(
		t,
		strings.Contains(err.Error(), "duplicate resource(s) produced"),
		"a non-duplicate error must not carry the diagnostic:\n%s",
		err,
	)
}

func TestRender_NonDuplicateError_DuplicatesBehindIt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	cmChild(t, root, "c", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err == nil, "%s", "expected a build failure")
	assert.ErrorIf(
		t,
		strings.Contains(err.Error(), "duplicate resource(s) produced"),
		"non-duplicate error carries the diagnostic:\n%s",
		err,
	)
}

func TestRender_DuplicateID_FilesNameProducers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml",
		"namespace: shared\nresources:\n- one.yaml\n- two.yaml\n")
	testutil.WriteFile(t, root, "apps/one.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")
	testutil.WriteFile(t, root, "apps/two.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")

	assertDuplicateReport(t, root,
		"    ConfigMap.v1.[noGrp]/dup.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml")
}

func TestRender_DuplicateID_ManyIDsNamesProducers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var body strings.Builder
	for i := range dupDiagBuildLimit + 1 {
		body.WriteString(cmYAML(fmt.Sprintf("cm-%03d", i)) + "---\n")
	}
	testutil.WriteFile(t, root, "apps/k00/kustomization.yaml", existingKustomization("cm.yaml"))
	testutil.WriteFile(t, root, "apps/k00/cm.yaml", body.String())
	testutil.WriteFile(t, root, "apps/k01/kustomization.yaml", existingKustomization("cm.yaml"))
	testutil.WriteFile(t, root, "apps/k01/cm.yaml", cmYAML("cm-000"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", existingKustomization("k00", "k01"))
	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err == nil, "%s", "expected duplicate-id failure")
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
		"duplicate failure lost renderer attribution: %v",
		err,
	)
	_, diagnostic, ok := strings.Cut(err.Error(), "duplicate resource(s) produced by multiple accumulated paths:")
	assert.FatalIf(t, !ok, "missing diagnostic attribution: %v", err)
	for _, producer := range []string{"./apps/k00", "./apps/k01"} {
		assert.ErrorIf(
			t,
			!strings.Contains(diagnostic+"\n", "\n      - "+producer+"\n"),
			"diagnostic missing producer %s: %s",
			producer,
			diagnostic,
		)
	}
}

func TestRender_DuplicateID_EffectiveNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "directory", manifest.NamedResource{Namespace: "default", Name: "shared"})
	testutil.WriteFile(t, root, "apps/omitted.yaml", cmYAML("shared"))
	testutil.WriteFile(t, root, "apps/default.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\n")
	testutil.WriteFile(t, root, "apps/distinct.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: other}\n")
	assertDuplicateReport(
		t,
		root,
		"    ConfigMap.v1.[noGrp]/shared.[noNs]\n      - ./apps/default.yaml\n      - ./apps/directory\n      - ./apps/omitted.yaml",
	)
}

type diagnosticFS struct {
	filesys.FileSystem

	children   map[string]bool
	cleaned    map[string]int
	unreadable string
	readFile   func(string)
}

func (f diagnosticFS) ReadFile(path string) ([]byte, error) {
	if f.children != nil && filepath.Base(path) == "kustomization.yaml" {
		f.children[filepath.Dir(path)] = true
	}
	if path == f.unreadable {
		return nil, fs.ErrPermission
	}
	if f.readFile != nil {
		f.readFile(path)
	}
	return f.FileSystem.ReadFile(path)
}

func (f diagnosticFS) CleanedAbs(path string) (filesys.ConfirmedDir, string, error) {
	if f.cleaned != nil {
		f.cleaned[path]++
	}
	return f.FileSystem.CleanedAbs(path)
}

func TestDuplicateID_BoundedRebuilds(t *testing.T) {
	t.Parallel()
	fsys := filesys.MakeFsInMemory()
	assert.NoError(t, fsys.WriteFile("/apps/before.yaml", []byte(cmYAML("dup"))))
	assert.NoError(t, fsys.WriteFile("/apps/after.yaml", []byte(cmYAML("dup"))))
	var data strings.Builder
	data.WriteString("resources:\n- before.yaml\n")
	for i := range 300 {
		dir := fmt.Sprintf("/apps/k%03d", i)
		assert.NoError(t, fsys.WriteFile(dir+"/kustomization.yaml", []byte("resources: [missing.yaml]\n")))
		fmt.Fprintf(&data, "- k%03d\n", i)
	}
	data.WriteString("- after.yaml\n")
	counting := diagnosticFS{FileSystem: fsys, children: make(map[string]bool), cleaned: make(map[string]int)}
	got := duplicateProducers(counting, "/apps", "apps", []byte(data.String()))
	assert.Equal(t, len(counting.children), dupDiagBuildLimit)
	attempts := 0
	for entry, count := range counting.cleaned {
		if fsys.IsDir(entry) {
			attempts += count
		}
	}
	assert.Equal(t, attempts, dupDiagBuildLimit)
	assert.Equal(t, counting.cleaned["/apps/before.yaml"], 0)
	assert.Equal(t, counting.cleaned["/apps/after.yaml"], 0)
	assert.Equal(t, got, "  duplicate resource(s) produced by multiple accumulated paths:\n"+
		"    ConfigMap.v1.[noGrp]/dup.[noNs]\n      - ./apps/after.yaml\n      - ./apps/before.yaml")
}

func TestDuplicateID_FileLoadFailures(t *testing.T) {
	t.Parallel()
	fsys := filesys.MakeFsInMemory()
	for name, body := range map[string]string{
		"one.yaml":        cmYAML("dup"),
		"two.yaml":        cmYAML("dup"),
		"unreadable.yaml": cmYAML("dup"),
		"malformed.yaml":  cmYAML("dup") + "---\ninvalid: [\n",
		"duplicated.yaml": cmYAML("dup") + "---\n" + cmYAML("dup"),
	} {
		assert.NoError(t, fsys.WriteFile("/apps/"+name, []byte(body)))
	}
	counting := diagnosticFS{FileSystem: fsys, unreadable: "/apps/unreadable.yaml"}
	kus := []byte(existingKustomization("one.yaml", "two.yaml", "unreadable.yaml", "malformed.yaml", "duplicated.yaml"))
	assert.NoError(t, fsys.WriteFile("/apps/kustomization.yaml", kus))
	kustomizestate.BuildMutex.Lock()
	_, err := kustomizestate.Build(counting, "/apps")
	kustomizestate.BuildMutex.Unlock()
	// The parent MUST fail independently of best-effort attribution.
	assert.FatalIf(t, err == nil, "%s", "expected the original aggregate failure")
	assert.ErrorIf(t, !strings.Contains(err.Error(), "already registered id"), "lost duplicate cause: %v", err)
	got := duplicateProducers(counting, "/apps", "apps", kus)
	assert.Equal(t, got, "  duplicate resource(s) produced by multiple accumulated paths:\n"+
		"    ConfigMap.v1.[noGrp]/dup.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml")
}

//nolint:paralleltest // The witness changes the process-global OpenAPI schema.
func TestDuplicateID_SchemaIsolation(t *testing.T) {
	fsys := filesys.MakeFsInMemory()
	widget := "apiVersion: diagnostic.example/v1\nkind: Widget\nmetadata: {name: shared"
	for name, body := range map[string]string{
		"/schema/kustomization.yaml": "openapi: {path: schema.json}\nresources: [cm.yaml]\n",
		"/schema/schema.json": `{"definitions":{},"paths":{
			"/apis/diagnostic.example/v1/widgets":{"get":{
				"x-kubernetes-action":"get",
				"x-kubernetes-group-version-kind":{"group":"diagnostic.example","version":"v1","kind":"Widget"}
			}}
		}}`,
		"/schema/cm.yaml":  cmYAML("schema"),
		"/apps/one.yaml":   widget + "}\n",
		"/apps/two.yaml":   widget + ", namespace: default}\n",
		"/apps/other.yaml": widget + ", namespace: other}\n",
	} {
		assert.NoError(t, fsys.WriteFile(name, []byte(body)))
	}
	schemaEntered, releaseSchema := make(chan struct{}), make(chan struct{})
	buildDone := make(chan error, 1)
	changing := diagnosticFS{FileSystem: fsys, readFile: func(path string) {
		if path == "/schema/cm.yaml" {
			// Resource loading follows SetSchema while the build owns BuildMutex.
			scoped, found := openapi.IsNamespaceScoped(
				yaml.TypeMeta{APIVersion: "diagnostic.example/v1", Kind: "Widget"},
			)
			assert.Equal(t, scoped, false)
			assert.Equal(t, found, true)
			close(schemaEntered)
			<-releaseSchema
		}
	}}
	go func() {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		_, err := kustomizestate.Build(changing, "/schema")
		buildDone <- err
	}()
	<-schemaEntered
	const workers = 2
	read := make(chan struct{}, workers)
	reports := make(chan string, workers)
	for range workers {
		go func() {
			observed := diagnosticFS{FileSystem: fsys, readFile: func(path string) {
				if path == "/apps/one.yaml" {
					read <- struct{}{}
				}
			}}
			reports <- duplicateProducers(observed, "/apps", "apps",
				[]byte(existingKustomization("one.yaml", "two.yaml", "other.yaml")))
		}()
	}
	for range workers {
		<-read
	}
	close(releaseSchema)
	assert.NoError(t, <-buildDone)
	for range workers {
		assert.Equal(t, <-reports, "  duplicate resource(s) produced by multiple accumulated paths:\n"+
			"    Widget.v1.diagnostic.example/shared.[noNs]\n      - ./apps/one.yaml\n      - ./apps/two.yaml")
	}
}
