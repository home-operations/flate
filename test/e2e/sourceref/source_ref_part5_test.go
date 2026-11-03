package sourceref_test

import (
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESourceRefHeldErrorAcrossNamespacesName(t *testing.T, tt *struct {
	name        string
	betaURL     string
	workingTree bool
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
	for _, namespace := range []string{"alpha", "beta"} {
		suspend := ""
		if namespace == "beta" {
			suspend = "  suspend: true\n"
		}
		testutil.WriteFile(
			t,
			root,
			"flux/"+namespace+".yaml",
			testutil.KustomizationYAML(
				"apps",
				namespace,
				"  interval: 10m\n"+suspend+"  path: ./broken\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "flux/beta-repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: beta}
spec: {interval: 10m, url: '`+tt.betaURL+`'}
`)
	testutil.WriteFile(
		t,
		root,
		"flux/sources.yaml",
		testutil.KustomizationYAML(
			"z-sources",
			"alpha",
			"  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: alpha}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
	testutil.WriteFile(t, root, "broken/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"broken/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n",
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
	harness.MutateFile(t, filepath.Join(root, "broken/cm.yaml"), "value: pinned", "value: newer")
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	testutil.WriteFile(t, root, "broken/cm.yaml", "metadata: {name: broken\n")
	l := loader.New(store.New())
	l.SourceRoot = root
	{
		_, loadErr := l.Load(t.Context(), filepath.Join(root, "broken"))
		assert.FatalIf(t, loadErr != nil, "%v", loadErr)
	}
	l.Admit(root)
	heldError := l.EntryFailure(t.Context(), root)
	assert.FatalIf(
		t,
		!errors.Is(heldError, manifest.ErrInput),
		"expected working-tree decode error, got %v",
		heldError,
	)
	transport := installSourceRefTransport(t, repo.Storer)
	var previous string
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			e2ESourceRefHeldErrorAcrossNamespacesNameCase(t, &root, &concurrency, struct {
				heldError *interface{ Error() string }
				transport **sourceRefLoader
				previous  *string
			}{heldError: (*interface{ Error() string })(&heldError), transport: &transport, previous: &previous})
		})
	}
	st := store.New()
	_, discoveryError := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: st,
	})
	for _, namespace := range []string{"alpha", "beta"} {
		ks, ok := st.Get[*manifest.Kustomization](testutil.KustomizationID(namespace, "apps"))
		assert.FatalIf(
			t,
			!ok || ks.SourceRef.Namespace != "" || ks.Path != "./broken",
			"expected implicit source in consumer namespace, got %+v",
			ks,
		)
		for ref := range manifest.References(ks, manifest.ReferenceRelations) {
			if ref.Use == "source" {
				assert.Equal(t, ref.Namespace, namespace)
			}
		}
	}
	art, ok := st.GetArtifact(manifest.NamedResource{
		Group: "source.toolkit.fluxcd.io",
		Kind:  manifest.KindGitRepository, Namespace: "alpha", Name: "cluster",
	}, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(
		t,
		!ok || art.LocalRoot != root || art.LocalPath == root || art.Key == "" || art.FS == nil,
		"expected alpha's committed source artifact, got %+v",
		art,
	)
	pinned, err := fs.ReadFile(art.FS, "broken/cm.yaml")
	assert.FatalIf(
		t,
		err != nil || !strings.Contains(string(pinned), "value: pinned"),
		"alpha lost committed bytes: %q, %v",
		pinned,
		err,
	)
	switch {
	case tt.workingTree:

		betaArtifact, ok := st.GetArtifact(manifest.NamedResource{
			Group: "source.toolkit.fluxcd.io",
			Kind:  manifest.KindGitRepository, Namespace: "beta", Name: "cluster",
		}, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok || betaArtifact.LocalRoot != root || betaArtifact.LocalPath == root ||
			betaArtifact.LocalPath == art.LocalPath ||
			betaArtifact.Key != "" ||
			betaArtifact.FS == nil, "expected beta's working-tree artifact, got %+v", betaArtifact)
		current, err := fs.ReadFile(betaArtifact.FS, "broken/cm.yaml")
		assert.FatalIf(
			t,
			err != nil || string(current) != "metadata: {name: broken\n",
			"beta lost malformed working-tree bytes: %q, %v",
			current,
			err,
		)
		assert.FatalIf(
			t,
			discoveryError != nil,
			"incidental working-tree error failed discovery: %v",
			discoveryError,
		)

	case discoveryError != nil:

		t.Fatalf("expected alpha's error to be discarded, got %v", discoveryError)
	}
}

func e2ESourceRefHeldErrorSurvivesNamespaceInheritanceName(t *testing.T, tt *struct {
	name                string
	namespace           string
	wantNamespace       string
	pinned              bool
	external            bool
	namespaceFreeSource bool
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
	for _, ks := range []struct{ name, path string }{
		{"probe", "ok"}, {"entry", "entries"}, {"stage", "stage"},
	} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+ks.name+".yaml",
			testutil.KustomizationYAML(
				ks.name,
				"flux-system",
				"  interval: 10m\n  path: ./"+ks.path+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "entries/kustomization.yaml", "resources: [nested/child.yaml]\n")
	testutil.WriteFile(t, root, "entries/nested/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entries/nested/child.yaml",
		testutil.KustomizationYAML(
			"a-held",
			"",
			"  interval: 10m\n  suspend: true\n  path: ./held\n  sourceRef: {kind: GitRepository, name: unknown"+tt.namespace+"}\n",
		),
	)
	testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [sources.yaml]\n")
	sourceTargetNamespace := ""
	if tt.namespaceFreeSource {
		sourceTargetNamespace = "  targetNamespace: beta\n"
		if tt.pinned {
			sourceTargetNamespace = "  targetNamespace: shifted\n"
		}
	}
	testutil.WriteFile(
		t,
		root,
		"stage/sources.yaml",
		testutil.KustomizationYAML(
			"z-sources",
			"flux-system",
			"  interval: 10m\n  path: ./sources\n",
		)+sourceTargetNamespace+`  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`,
	)
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [claim.yaml, repo.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"sources/claim.yaml",
		testutil.KustomizationYAML(
			"claim",
			"flux-system",
			"  interval: 10m\n  path: ./entries/nested\n  targetNamespace: shifted\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	sourceName, sourceMetadataNamespace := "pulse", ", namespace: flux-system"
	if tt.pinned || tt.external {
		sourceName, sourceMetadataNamespace = "unknown", ", namespace: shifted"
	}
	if tt.namespaceFreeSource {
		sourceMetadataNamespace = ""
	}
	url, ref := "git://fixture.invalid/cluster", "  ref: {branch: master}\n"
	if tt.external {
		url, ref = "git://fixture.invalid/external", ""
	}
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: `+sourceName+sourceMetadataNamespace+`}
spec:
  interval: 10m
  url: `+url+"\n"+ref)
	testutil.WriteFile(t, root, "ok/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"ok/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ok, namespace: apps}\n",
	)
	testutil.WriteFile(t, root, "held/kustomization.yaml", "resources: [broken.yaml]\n")
	testutil.WriteFile(t, root, "held/broken.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: held}\n")
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	head, err := repo.Head()
	assert.NoError(t, err)
	if tt.pinned {
		testutil.WriteFile(t, root, "extra.txt", "second commit\n")
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{
				Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			},
		)
	}
	if !tt.external {
		harness.MutateFile(
			t,
			filepath.Join(root, "sources/repo.yaml"),
			"{branch: master}",
			"{commit: "+head.Hash().String()+"}",
		)
	}
	testutil.WriteFile(t, root, "held/broken.yaml", "metadata: {name: held\n")
	l := loader.New(store.New())
	l.SourceRoot = root
	{
		_, err := l.Load(t.Context(), filepath.Join(root, "held"))
		assert.FatalIf(t, err != nil, "%v", err)
	}
	l.Admit(root)
	heldError := l.EntryFailure(t.Context(), root)
	assert.FatalIf(
		t,
		!errors.Is(heldError, manifest.ErrInput),
		"expected held consumer's decode error, got %v",
		heldError,
	)
	transport := installSourceRefTransport(t, repo.Storer)
	out, stderr, code := harness.RunCLIBuffers("build", "ks", "probe", "-n", "flux-system",
		"--path", filepath.Join(root, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir())
	aliases := 1
	if tt.pinned || tt.external {
		aliases = 0
	}
	persistence := strings.Count(stderr, "[RenderPersistenceDisabled]")
	if code != 0 || persistence > 1 ||
		strings.Count(
			stderr,
			"[BootstrapAliases]",
		) != aliases || strings.Count(stderr, "warnings (") != aliases+persistence ||
		strings.Contains(
			stderr,
			"held/broken.yaml",
		) || transport.calls.Load() != 0 || !strings.Contains(out, "name: ok") {
		t.Fatalf("re-keyed held consumer: exit=%d transport=%d stdout=%q\nstderr (-want +got):\n%s",
			code, transport.calls.Load(), out, cmp.Diff("", stderr))
	}
	st := store.New()
	_, discoveryError := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: st,
	})
	id := testutil.KustomizationID("shifted", "a-held")
	ks, ok := st.Get[*manifest.Kustomization](id)
	assert.FatalIf(
		t,
		!ok || ks.Path != "./held" || ks.SourceRef.Name != "unknown",
		"expected surviving consumer with inherited namespace and unchanged path/source, got %+v",
		ks,
	)
	var sourceRef manifest.NamedResource
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if ref.Use == "source" {
			sourceRef = ref.NamedResource
			break
		}
	}
	assert.Equal(t, sourceRef.Namespace, tt.wantNamespace)
	e2ESourceRefHeldErrorSurvivesNamespaceInheritanceNameLoop(t, struct {
		tt **struct {
			name                string
			namespace           string
			wantNamespace       string
			pinned              bool
			external            bool
			namespaceFreeSource bool
		}
		st             **store.Store
		sourceRef      *manifest.NamedResource
		discoveryError *error
		root           *string
	}{tt: &tt, st: &st, sourceRef: &sourceRef, discoveryError: &discoveryError, root: &root})
}

func e2ESourceRefPinnedPathIgnoresAddedDiscoveryObjectsScope(
	t *testing.T,
	current *string,
	scope *string,
	original *string,
) {
	t.Helper()

	path := filepath.Join((*current), (*scope))
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", path,
		"--concurrency", "2", "--cache-dir", t.TempDir())
	docs, err := testutil.DecodeDocs(strings.NewReader(out))
	assert.NoError(t, err)
	ids := map[string]int{}
	pinned := 0
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		ids[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
		if manifest.DocKind(doc) == manifest.KindConfigMap && name == "hello" {
			metadata := doc["metadata"].(map[string]any)
			labels := metadata["labels"].(map[string]any)
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/name"], "apps")
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			assert.Equal(t, doc["data"].(map[string]any)["value"], "v1.0.0")
			pinned++
		}
	}
	assert.Equal(t, pinned, 1)
	if (*scope) == "flux" {
		assert.FatalIf(
			t,
			strings.Contains(out, "phantom") || strings.Contains(out, "added-"),
			"working-tree discovery objects escaped the pin:\n%s",
			out,
		)
	} else {
		assert.Diff(t, ids, map[string]int{
			"ConfigMap/apps/hello":               1,
			"ConfigMap/apps/added-ks-output":     1,
			"ConfigMap/apps/added-rs-output":     1,
			"GitRepository/flux-system/cluster":  1,
			"GitRepository/flux-system/working":  1,
			"Kustomization/flux-system/apps":     1,
			"Kustomization/flux-system/added-ks": 1,
			"ResourceSet/flux-system/added-rs":   1,
		})
	}
	for direction, paths := range [][2]string{{(*current), (*original)}, {(*original), (*current)}} {
		args := []string{
			"diff", "all", "--path", filepath.Join(paths[0], (*scope)),
			"--path-orig", filepath.Join(paths[1], (*scope)), "--concurrency", "2", "--cache-dir", t.TempDir(),
		}
		if (*scope) == "flux" {
			out, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
			assert.FatalIf(t, out != "", "working-tree discovery objects changed the pinned diff:\n%s", out)
			continue
		}
		product, _ := harness.RequireCLIOK(t, append(args, "-o", "json")...)
		var result struct {
			Changes []struct {
				Kind      string `json:"Kind"`
				Namespace string `json:"Namespace"`
				Name      string `json:"Name"`
				Status    string `json:"Status"`
				Parent    struct {
					Kind      string `json:"Kind"`
					Namespace string `json:"Namespace"`
					Name      string `json:"Name"`
					Path      string `json:"Path"`
				} `json:"Parent"`
			} `json:"Changes"`
			Suppressed []any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(product), &result))
		assert.Equal(t, len(result.Suppressed), 0)
		changed := map[string]int{}
		for changeIndex := range result.Changes {
			change := result.Changes[changeIndex]
			id := change.Kind + "/" + change.Namespace + "/" + change.Name
			changed[id]++
			assert.Equal(t, change.Status, []string{"added", "removed"}[direction])
			wantKind, wantNamespace, wantName, wantPath := "", "", "", "."
			if change.Kind == "ConfigMap" && change.Name == "added-ks-output" {
				wantKind, wantNamespace, wantName, wantPath = "Kustomization", "flux-system", "added-ks", ""
			}
			assert.Equal(t, change.Parent.Kind, wantKind)
			assert.Equal(t, change.Parent.Namespace, wantNamespace)
			assert.Equal(t, change.Parent.Name, wantName)
			if change.Kind != "ConfigMap" {
				assert.Equal(t, change.Parent.Path, wantPath)
			}
		}
		assert.Diff(t, changed, map[string]int{
			"ConfigMap/apps/added-ks-output":     1,
			"ConfigMap/apps/added-rs-output":     1,
			"GitRepository/flux-system/working":  1,
			"Kustomization/flux-system/added-ks": 1,
			"ResourceSet/flux-system/added-rs":   1,
		})
	}
}

func e2EPinnedDiffRootPinStage(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	if (*stage) == "tag-change" {
		harness.MutateFile(t, filepath.Join((*current), "flux/repo.yaml"), "tag: v1.0.0", "tag: v2.0.0")
	}
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			e2EPinnedDiffRootPinStageScope(t, &current, &capturedInputs.original, struct {
				scope     *string
				stage     **string
				transport ***sourceRefLoader
			}{scope: &scope, stage: &stage, transport: &capturedInputs.transport})
		})
	}
}

func e2EPinnedDiffRootPinStage2(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	e2EPinnedDiffRootPinStage(t, stage, current, struct {
		original  *string
		transport **sourceRefLoader
	}{original: capturedInputs.original, transport: capturedInputs.transport})
}

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommand(
	t *testing.T,
	command *string,
	root *string,
	capturedInputs struct {
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
	},
) {
	t.Helper()

	var previous string
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommandCase(t, &command, &root, struct {
				concurrency    *string
				capturedInputs *struct {
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
				}
				previous *string
			}{concurrency: &concurrency, capturedInputs: &capturedInputs, previous: &previous})
		})
	}
}

func e2EPinnedDiffRootPinStageScope(t *testing.T, current **string, original **string, capturedInputs struct {
	scope     *string
	stage     **string
	transport ***sourceRefLoader
},
) {
	t.Helper()

	for _, side := range []struct{ name, current, original, before, after string }{
		{"forward", (*(*current)), (*(*original)), "v1.0.0", "v2.0.0"},
		{"reverse", (*(*original)), (*(*current)), "v2.0.0", "v1.0.0"},
	} {
		t.Run(side.name, func(t *testing.T) { e2EPinnedDiffRootPinStageScopeName(t, &side, &capturedInputs) })
	}
}

func e2EPinnedDiffRootPinStage3(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	e2EPinnedDiffRootPinStage2(t, stage, current, struct {
		original  *string
		transport **sourceRefLoader
	}{original: capturedInputs.original, transport: capturedInputs.transport})
}

func e2ESourceRefHeldErrorAcrossNamespacesNameCase(
	t *testing.T,
	root *string,
	concurrency *string,
	capturedInputs struct {
		heldError *interface{ Error() string }
		transport **sourceRefLoader
		previous  *string
	},
) {
	t.Helper()

	out, stderr, code := harness.RunCLIBuffers("build", "ks", "apps", "-n", "alpha",
		"--path", filepath.Join((*root), "flux"), "--concurrency", (*concurrency), "--cache-dir", t.TempDir())
	if code != 0 || !strings.Contains(out, "value: pinned") || strings.Contains(out, "value: newer") ||
		strings.Contains(stderr, (*capturedInputs.heldError).Error()) || (*capturedInputs.transport).calls.Load() != 0 {
		t.Fatalf("suspended consumer: exit=%d transport=%d stdout=%q\nstderr:\n%s",
			code, (*capturedInputs.transport).calls.Load(), out, stderr)
	}
	assert.FatalIf(
		t,
		(*concurrency) == "4" && out != (*capturedInputs.previous),
		"output differs across concurrency levels (-want +got):\n%s",
		cmp.Diff((*capturedInputs.previous), out),
	)
	(*capturedInputs.previous) = out
}

func e2ESourceRefHeldErrorSurvivesNamespaceInheritanceNameLoop(t *testing.T, loopInputs struct {
	tt **struct {
		name                string
		namespace           string
		wantNamespace       string
		pinned              bool
		external            bool
		namespaceFreeSource bool
	}
	st             **store.Store
	sourceRef      *manifest.NamedResource
	discoveryError *error
	root           *string
},
) {
	t.Helper()

	switch {
	case (*loopInputs.tt).pinned:

		art, ok := (*loopInputs.st).GetArtifact((*loopInputs.sourceRef), store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(
			t,
			(*loopInputs.discoveryError) != nil || !ok || art.LocalRoot != (*loopInputs.root) ||
				art.LocalPath == (*loopInputs.root),
			"expected genuine inherited source pin without a held error: error=%v artifact=%+v",
			(*loopInputs.discoveryError),
			art,
		)

	case (*loopInputs.discoveryError) != nil:

		t.Fatalf("incidental decode error failed discovery: %v", (*loopInputs.discoveryError))
	}
}
