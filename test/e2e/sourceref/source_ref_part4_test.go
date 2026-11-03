package sourceref_test

import (
	stdcmp "cmp"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/object"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PathOrigUsesOwnObjectStore(t *testing.T) {
	for _, tt := range []struct {
		name       string
		currentRef string
	}{
		{name: "different-spec", currentRef: "name: refs/tags/v1.0.0"},
		{name: "identical-spec", currentRef: "tag: v1.0.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "identical-spec" {
				t.Skip(
					"changed-only selection ignores divergent source revisions with identical authored files; tracked in fl-5c42t",
				)
			}
			current, _, _, _ := sourceRefFixture(t)
			setSourceRef(t, current, tt.currentRef)
			original, originalRepo, _, b := sourceRefFixture(t)
			assert.NoError(t, originalRepo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b),
			))
			for _, side := range []struct {
				root  string
				value string
			}{
				{root: current, value: "v1.0.0"},
				{root: original, value: "v2.0.0"},
			} {
				out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(side.root, "flux"),
					"--concurrency", "2", "--cache-dir", t.TempDir())
				assert.FatalIf(
					t,
					!strings.Contains(out, "value: "+side.value),
					"expected %s from %s:\n%s",
					side.value,
					side.root,
					out,
				)
			}
			out, _ := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(current, "flux"),
				"--path-orig",
				filepath.Join(original, "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			assertSourceValueDiff(t, out, "v2.0.0", "v1.0.0")
		})
	}
}

func assertSourceValueDiff(t *testing.T, out, before, after string) {
	t.Helper()
	assert.FatalIf(t, strings.Count(out, "-  value: "+before) != 1 || strings.Count(out, "+  value: "+after) != 1 ||
		strings.Contains(
			out,
			"+apiVersion:",
		) || strings.Contains(out, "+kind: ConfigMap"), "expected exact field removal/addition for %s -> %s:\n%s", before, after, out)
}

func setSourceRef(t *testing.T, root, ref string) {
	t.Helper()
	path := filepath.Join(root, "flux/entry.yaml")
	data, err := os.ReadFile(path)
	assert.NoError(t, err)
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

//nolint:ireturn // The upstream interface contract requires this return type.
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

func e2ESourceRefEmptyPathMatchesRootName(
	t *testing.T,
	fixture *func(t *testing.T, field string) (string, *store.Store, *discovery.Result),
	path *struct {
		name  string
		field string
	},
	capturedInputs struct {
		want     **discovery.Result
		wantDocs *[]map[string]any
	},
) {
	t.Helper()

	root, st, res := (*fixture)(t, path.field)
	parent := testutil.KustomizationID("flux-system", "root")
	child := testutil.KustomizationID("apps", "child")
	record, _ := res.Index.Lookup(child)
	assert.FatalIf(
		t,
		st.GetObject(child) == nil || record.Parent != parent,
		"source-root child not discovered or owned by root: parent=%v",
		record.Parent,
	)
	owners, _ := res.Index.OwnersOfFile("cm.yaml")
	assert.FatalIf(
		t,
		!slices.Equal(owners, []manifest.NamedResource{parent}),
		"source-root ConfigMap owners = %v; want [%v]",
		owners,
		parent,
	)
	//nolint:gocritic // The discovery iterator yields immutable records by value.
	for id, record := range res.Index.Files() {
		wantRecord, ok := (*capturedInputs.want).Index.Lookup(id)
		assert.FatalIf(t, !ok, "unexpected discovered resource %s", id)
		assert.Equal(t, record.File, wantRecord.File)
		assert.Equal(t, record.Parent, wantRecord.Parent)
	}
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			args := []string{
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
			}
			owned := harness.RunCLIStdout(t, append([]string{"build", "ks", "root"}, args...)...)
			assert.FatalIf(
				t,
				!strings.Contains(owned, "name: root-owned") || !strings.Contains(owned, "value: source-root") ||
					!strings.Contains(
						owned,
						"namespace: apps",
					),
				"root's rendered objects missing or untransformed:\n%s",
				owned,
			)
			out := harness.RunCLIStdout(t, append([]string{"build", "all"}, args...)...)
			assert.FatalIf(
				t,
				!strings.Contains(out, "name: leaf-owned"),
				"child's rendered objects missing:\n%s",
				out,
			)
			docs, err := testutil.DecodeDocs(strings.NewReader(out))
			assert.NoError(t, err)
			for _, doc := range docs {
				name, _ := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) == manifest.KindKustomization && name == "root" {
					spec := doc["spec"].(map[string]any)
					if path.name == "explicit" {
						assert.Equal(t, spec["path"], "./")
					} else if _, present := spec["path"]; present {
						t.Fatalf("omitted path was added to the root declaration: %s", out)
					}
					delete(spec, "path")
				}
			}
			assert.Diff(t, docs, (*capturedInputs.wantDocs))
		})
	}
}

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerName(t *testing.T, tt *struct {
	name          string
	appsFile      string
	sourcesFile   string
	sourcesName   string
	sourceRefName string
	sourceURL     string
	noRef         bool
},
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	testutil.WriteFile(
		t,
		root,
		"flux/"+tt.appsFile,
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: pinned, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/"+tt.sourcesFile,
		testutil.KustomizationYAML(
			tt.sourcesName,
			"flux-system",
			"  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: "+tt.sourceRefName+", namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
	url, ref := stdcmp.Or(tt.sourceURL, "git://fixture.invalid/cluster"), "  ref: {tag: v1.0.0}\n"
	if tt.noRef {
		ref = ""
	}
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: `+url+"\n"+ref)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, deleted.yaml, extra.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/deleted.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: deleted, namespace: apps}\ndata: {value: pinned-deleted}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/extra.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\ndata: {value: pinned-extra}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash()),
	))
	assert.NoError(t, os.Remove(filepath.Join(root, "apps/deleted.yaml")))
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: pinned", "value: newer")
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
	testutil.WriteFile(t, root, "apps/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra\n")
	transport := installSourceRefTransport(t, repo.Storer)
	var wantError string
	if tt.noRef || tt.sourceURL != "" {
		l := loader.New(store.New())
		l.SourceRoot = root
		{
			_, loadErr := l.Load(t.Context(), filepath.Join(root, "apps"))
			assert.FatalIf(t, loadErr != nil, "%v", loadErr)
		}
		l.Admit(root)
		decodeError := l.EntryFailure(t.Context(), root)
		assert.FatalIf(
			t,
			!errors.Is(decodeError, manifest.ErrInput),
			"expected working-tree decode error, got %v",
			decodeError,
		)
		wantError = "flate error: " + decodeError.Error() + "\n"
	}
	for _, command := range []string{"build", "diff"} {
		t.Run(command, func(t *testing.T) {
			e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommand(t, &command, &root, struct {
				tt **struct {
					name          string
					appsFile      string
					sourcesFile   string
					sourcesName   string
					sourceRefName string
					sourceURL     string
					noRef         bool
				}
				transport **sourceRefLoader
				wantError *string
			}{tt: &tt, transport: &transport, wantError: &wantError})
		})
	}
	if wantError != "" {
		return
	}
	st := store.New()
	res, err := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: st,
	})
	assert.NoError(t, err)
	artifact, ok := st.GetArtifact(manifest.NamedResource{
		Group: "source.toolkit.fluxcd.io",
		Kind:  manifest.KindGitRepository, Namespace: "flux-system", Name: "pinned",
	}, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(
		t,
		!ok || artifact.LocalPath == root,
		"expected committed source artifact, got %+v",
		artifact,
	)
	for _, file := range []struct{ name, path string }{{"hello", "cm.yaml"}, {"deleted", "deleted.yaml"}, {"extra", "extra.yaml"}} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: file.name}
		record, indexed := res.Index.Lookup(id)
		assert.FatalIf(t, !indexed || record.Path != filepath.Join(artifact.LocalPath, "apps", file.path) ||
			filepath.Join(
				root,
				filepath.FromSlash(record.File),
			) != record.Path, "pinned discovery metadata for %s: indexed=%t path=%q source=%q", id, indexed, record.Path, record.File)
	}
}

func e2ESourceRefHeldErrorScopeName(t *testing.T, tt *struct {
	name          string
	kind          string
	sourceName    string
	url           string
	ref           string
	unreadable    bool
	bothHeld      bool
	reversedPaths bool
},
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	writeConsumer := func(name, path, kind, sourceName string) {
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				"flux-system",
				"  interval: 10m\n  path: ./"+path+"\n  sourceRef: {kind: "+kind+", name: "+sourceName+"}\n",
			),
		)
	}
	heldPath, secondPath := "held", "second"
	if tt.reversedPaths {
		heldPath, secondPath = "z-held", "a-second"
	}
	writeConsumer("a-held", heldPath, "GitRepository", "held")
	writeConsumer("b-second", secondPath, tt.kind, tt.sourceName)
	writeConsumer("z-sources", "sources", "GitRepository", "flux-system")
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [held.yaml]\n")
	testutil.WriteFile(t, root, "sources/held.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: held, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', interval: 10m}
`)
	switch {
	case tt.kind == "OCIRepository":

		testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: second, namespace: flux-system}
spec: {url: 'oci://fixture.invalid/second', interval: 10m, secretRef: {name: missing-auth}}
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: missing-auth, namespace: flux-system}
spec: {target: {name: missing-auth}}
`)

	case tt.url != "":

		testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: second, namespace: flux-system}
spec:
  interval: 10m
  url: `+tt.url+"\n"+tt.ref)

	case tt.unreadable || tt.bothHeld:

		testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [held.yaml, second.yaml]\n")
		testutil.WriteFile(t, root, "sources/second.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: second, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', interval: 10m}
`)
	}
	for _, path := range []string{heldPath, secondPath} {
		testutil.WriteFile(t, root, path+"/kustomization.yaml", "resources: [extra.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			path+"/extra.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra}\n",
		)
	}
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(t, root, heldPath+"/extra.yaml", "metadata: {name: held\n")
	if tt.unreadable {
		file := filepath.Join(root, secondPath+"/extra.yaml")
		if err := os.Chmod(file, 0); err != nil {
			t.Skipf("cannot make file unreadable: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(file, 0o600) })
	} else {
		testutil.WriteFile(t, root, secondPath+"/extra.yaml", "metadata: {name: second\n")
	}
	inputStore := store.New()
	if tt.unreadable {
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
		assert.NoError(t, err)
		obj := &manifest.GitRepository{Name: tt.sourceName, Namespace: "flux-system"}
		inputStore.AddObject(obj)
		inputStore.SetArtifact(obj.Named(), &store.SourceArtifact{LocalPath: root, FS: input.FS})
	}
	secondLoader := loader.New(inputStore)
	secondLoader.SourceRoot = root
	_, secondError := secondLoader.Load(t.Context(), filepath.Join(root, secondPath))
	if !tt.unreadable {
		assert.NoError(t, secondError)
		secondError = secondLoader.EntryFailure(t.Context(), root)
		assert.FatalIf(
			t,
			!errors.Is(secondError, manifest.ErrInput),
			"expected second consumer's decode error, got %v",
			secondError,
		)
	}
	if tt.unreadable && secondError == nil {
		t.Skip("OS or current user permits reading a file with no permissions")
	}
	assert.FatalIf(
		t,
		secondError == nil || (tt.unreadable && errors.Is(secondError, manifest.ErrInput)),
		"expected second consumer's loader error, got %v",
		secondError,
	)
	wantError, repeats := "flate error: "+secondError.Error()+"\n", 1
	if tt.bothHeld {
		heldLoader := loader.New(store.New())
		heldLoader.SourceRoot = root
		{
			_, err := heldLoader.Load(t.Context(), filepath.Join(root, heldPath))
			assert.FatalIf(t, err != nil, "%v", err)
		}
		heldLoader.Admit(root)
		heldError := heldLoader.EntryFailure(t.Context(), root)
		assert.FatalIf(
			t,
			!errors.Is(heldError, manifest.ErrInput),
			"%v",
			"expected held consumer's decode error",
		)
		repeats = 2
		if !tt.reversedPaths {
			wantError = "flate error: " + heldError.Error() + "\n"
		}
	}
	transport := installSourceRefTransport(t, repo.Storer)
	e2ESourceRefHeldErrorScopeNameLoop(t, struct {
		repeats *int
		root    *string
		tt      **struct {
			name          string
			kind          string
			sourceName    string
			url           string
			ref           string
			unreadable    bool
			bothHeld      bool
			reversedPaths bool
		}
		wantError *string
		transport **sourceRefLoader
		heldPath  *string
	}{repeats: &repeats, root: &root, tt: &tt, wantError: &wantError, transport: &transport, heldPath: &heldPath})
}

func e2ESourceRefHeldErrorScopeNameLoop(t *testing.T, loopInputs struct {
	repeats *int
	root    *string
	tt      **struct {
		name          string
		kind          string
		sourceName    string
		url           string
		ref           string
		unreadable    bool
		bothHeld      bool
		reversedPaths bool
	}
	wantError *string
	transport **sourceRefLoader
	heldPath  *string
},
) {
	t.Helper()

	for i := range *loopInputs.repeats {
		out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join((*loopInputs.root), "flux"),
			"--concurrency", "2", "--cache-dir", t.TempDir())
		switch {
		case (*loopInputs.tt).unreadable:

			assert.FatalIf(t, code != 1 || out != "" || !strings.HasSuffix(stderr, (*loopInputs.wantError)) ||
				strings.Count(stderr, "[RenderPersistenceDisabled]") > 1 ||
				(*loopInputs.transport).calls.Load() != 0, "unreadable file: exit=%d stdout=%q stderr=%s", code, out, stderr)

		case code != 1 || !strings.Contains(stderr, "kustomize build ./"+(*loopInputs.heldPath)) || !strings.Contains(stderr, "flux-system/b-second"):

			t.Fatalf("scoped failures: run=%d exit=%d stdout=%q stderr=%s", i, code, out, stderr)
		}
	}
}
