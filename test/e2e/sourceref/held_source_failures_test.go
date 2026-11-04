package sourceref_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

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
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
		assert.NoError(t, err)
		obj := &manifest.GitRepository{Name: tt.sourceName, Namespace: "flux-system"}
		inputStore.AddObject(obj)
		inputStore.SetArtifact(obj.Named(), &store.SourceArtifact{LocalPath: root, FS: input.FS})
	}
	secondLoader := loader.New(inputStore)
	secondLoader.SourceRoot = root
	_, secondError := secondLoader.Load(t.Context(), filepath.Join(root, secondPath), nil)
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
			_, err := heldLoader.Load(t.Context(), filepath.Join(root, heldPath), nil)
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
		runCLIBuffersResult1 := harness.RunCLIBuffers(
			"build",
			"all",
			"--path",
			filepath.Join((*loopInputs.root), "flux"),
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
		)
		out, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
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
	harness.MutateFile(t, filepath.Join(root, "broken", "cm.yaml"), "value: pinned", "value: newer")
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
		_, loadErr := l.Load(t.Context(), filepath.Join(root, "broken"), nil)
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
			filepath.Join(root, "sources", "repo.yaml"),
			"{branch: master}",
			"{commit: "+head.Hash().String()+"}",
		)
	}
	testutil.WriteFile(t, root, "held/broken.yaml", "metadata: {name: held\n")
	l := loader.New(store.New())
	l.SourceRoot = root
	{
		_, err := l.Load(t.Context(), filepath.Join(root, "held"), nil)
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
	runCLIBuffersResult2 := harness.RunCLIBuffers("build", "ks", "probe", "-n", "flux-system",
		"--path", filepath.Join(root, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir())
	out, stderr, code := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
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

	runCLIBuffersResult3 := harness.RunCLIBuffers("build", "ks", "apps", "-n", "alpha",
		"--path", filepath.Join((*root), "flux"), "--concurrency", (*concurrency), "--cache-dir", t.TempDir())
	out, stderr, code := runCLIBuffersResult3.Stdout, runCLIBuffersResult3.Stderr, runCLIBuffersResult3.Code
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
