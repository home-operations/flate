package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestRun_PinnedArtifactDiscovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		revealed, broad bool
	}{{}, {revealed: true}, {broad: true}, {revealed: true, broad: true}} {
		t.Run(fmt.Sprintf("followed_source_%t_broad_%t", tc.revealed, tc.broad), func(t *testing.T) {
			t.Parallel()
			testRunPinnedArtifactDiscoveryCase(t, &tc)
		},
		)
	}
}

func TestPlan_PinnedRootKeepsEntrySource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml, flux/entry.yaml, flux/repo.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/entry.yaml",
		testutil.KustomizationYAML(
			"root",
			"control",
			"  path: ./\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	source := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: control}\nspec: {url: 'git://fixture.invalid/cluster', ref: {tag: v1}}\n"
	testutil.WriteFile(t, root, "flux/repo.yaml", source)
	for i, tag := range []string{"v1", "v2"} {
		testutil.WriteFile(
			t,
			root,
			"cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {value: "+tag+"}\n",
		)
		hash := testutil.GitCommitAll(
			t,
			repo,
			tag,
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(int64(i), 0)}},
		)
		assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(tag), hash)))
	}
	testutil.WriteFile(t, root, "flux/repo.yaml", strings.Replace(source, "tag: v1", "tag: v2", 1))
	entries, err := Plan(t.Context(), Config{Path: root, SelfURLs: []string{"git://fixture.invalid/cluster"}})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(entries) != 1 || entries[0].Path != root,
		"root pin replaced the entry's source view: %+v",
		entries,
	)
}

func TestPlan_PinnedPathExcludesWorkingTreePackages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/entry.yaml",
		testutil.KustomizationYAML(
			"root",
			"control",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/repo.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: control}\nspec: {url: 'git://fixture.invalid/cluster', ref: {tag: pinned}}\n",
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {value: pinned}\n",
	)
	hash := testutil.GitCommitAll(
		t,
		repo,
		"pinned",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), hash)))
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {value: dirty}\n",
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, added.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/added.yaml",
		testutil.KustomizationYAML(
			"phantom",
			"control",
			"  path: ./phantom\n  sourceRef: {kind: GitRepository, name: working}\n",
		)+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: working, namespace: control}\nspec: {url: 'git://fixture.invalid/cluster'}\n",
	)
	testutil.WriteFile(t, root, "phantom/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "phantom/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: phantom}\n")
	testutil.GitCommitAll(
		t,
		repo,
		"head",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}},
	)
	entries, err := Plan(t.Context(), Config{Path: root, SelfURLs: []string{"git://fixture.invalid/cluster"}})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(entries) != 1 || entries[0].Path != root,
		"suppressed working-tree package became a cluster: %+v",
		entries,
	)
}

func TestAliasBootstrapSources_DeclaredRefs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, base                                        string
		commit                                            bool
		ref                                               *sourcev1.GitRepositoryRef
		head, fallback, fail, noStore, sparse, submodules bool
		lookupErr                                         bool
	}{
		{name: "no-ref", head: true},
		{name: "empty-ref", ref: &sourcev1.GitRepositoryRef{}, head: true},
		{name: "head", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-tag", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}},
		{name: "older-base-head-commit", base: "v1.0.0", commit: true},
		{name: "matching-base-head", base: "v2.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, head: true},
		{name: "older-base-head-sparse", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true},
		{name: "older-base-head-submodules", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, submodules: true, fallback: true},
		{name: "non-head", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "conflicting", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}},
		{name: "missing-tag", ref: &sourcev1.GitRepositoryRef{Tag: "missing"}, fallback: true},
		{name: "missing-name", ref: &sourcev1.GitRepositoryRef{Name: "refs/pull/9/head"}, fallback: true},
		{name: "branch", ref: &sourcev1.GitRepositoryRef{Branch: "master"}, head: true},
		{name: "missing-branch", ref: &sourcev1.GitRepositoryRef{Branch: "missing"}, head: true},
		{name: "older-base-head-branch-submodules", base: "v1.0.0", ref: &sourcev1.GitRepositoryRef{Branch: "master"}, submodules: true, fallback: true},
		{name: "recursive-missing-branch", ref: &sourcev1.GitRepositoryRef{Branch: "missing"}, submodules: true, fallback: true},
		{name: "invalid-branch", ref: &sourcev1.GitRepositoryRef{Branch: "bad branch"}, fail: true},
		{name: "branch-lookup-error", ref: &sourcev1.GitRepositoryRef{Branch: "main"}, lookupErr: true, head: true},
		{name: "no-store-branch", ref: &sourcev1.GitRepositoryRef{Branch: "main"}, noStore: true, head: true},
		{name: "missing-semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">3.0.0"}, fallback: true},
		{name: "no-store", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, noStore: true, fallback: true},
		{name: "no-store-invalid", ref: &sourcev1.GitRepositoryRef{Commit: "abc"}, noStore: true, fail: true},
		{name: "no-store-no-ref", noStore: true, head: true},
		{name: "sparse", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, sparse: true},
		{name: "submodules", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, submodules: true, fallback: true},
		{name: "head-sparse", ref: &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, sparse: true, head: true},
		{name: "missing-commit", ref: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, sparse: true, fallback: true},
		{name: "missing-commit-branch", ref: &sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, fallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAliasBootstrapSourcesDeclaredRefsCase(t, &tc)
		},
		)
	}
}

func testRunPinnedArtifactDiscoveryCase(t *testing.T, testCaseInput *struct {
	revealed bool
	broad    bool
},
) {
	t.Helper()

	root := discoveryRefFixture(t)
	repo, err := git.PlainOpen(root)
	assert.FatalIf(t, err != nil, "%v", err)
	entry := "flux"
	if testCaseInput.revealed {
		entry = "sources"
		testutil.WriteFile(
			t,
			root,
			"flux/entry.yaml",
			testutil.KustomizationYAML(
				"entry",
				"flux-system",
				"  {path: ./sources, sourceRef: {kind: GitRepository, name: flux-system}}\n",
			),
		)
	}
	testutil.WriteFile(t, root, entry+"/pinned.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: pinned}}
`+"---\n"+
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}\n",
		))
	child := testutil.KustomizationYAML(
		"child",
		"flux-system",
		"  {path: ./leaf, sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}}\n",
	)
	testutil.WriteFile(t, root, "apps/child.yaml", child)
	testutil.WriteFile(
		t,
		root,
		"apps/kustomization.yaml",
		"resources: [child.yaml, cm.yaml, generator.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=pinned]\n- name: file-generated\n  files: [value=input.txt]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/generator.yaml",
		"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps, uid: pinned}\nspec:\n  sources: [{alias: repo, kind: GitRepository, name: cluster, namespace: flux-system}]\n  artifacts: [{name: pinned-output}]\n",
	)
	testutil.WriteFile(t, root, "apps/input.txt", "pinned")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {value: pinned}\n",
	)
	hash := testutil.GitCommitAll(
		t,
		repo,
		"pinned",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(1, 0)}},
	)
	{
		localErr := repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName("pinned"), hash),
		)
		assert.FatalIf(t, localErr != nil, "%v", localErr)
	}
	{
		innerLocalErr := os.Remove(filepath.Join(root, "apps", "child.yaml"))
		assert.FatalIf(t, innerLocalErr != nil, "%v", innerLocalErr)
	}
	{
		innerInnerLocalErr := os.Remove(filepath.Join(root, "apps", "cm.yaml"))
		assert.FatalIf(t, innerInnerLocalErr != nil, "%v", innerInnerLocalErr)
	}
	{
		innerInnerInnerLocalErr := os.Remove(filepath.Join(root, "apps", "generator.yaml"))
		assert.FatalIf(t, innerInnerInnerLocalErr != nil, "%v", innerInnerInnerLocalErr)
	}
	testutil.WriteFile(
		t,
		root,
		"apps/child.yaml",
		strings.Replace(child, manifest.FluxKustomizeDomain, "foreign.example", 1),
	)
	testutil.WriteFile(t, root, "apps/phantom.yaml", strings.Replace(child, "name: child", "name: phantom", 1))
	testutil.WriteFile(t, root, "apps/input.txt", "dirty")
	testutil.WriteFile(
		t,
		root,
		"apps/kustomization.yaml",
		"resources: [phantom.yaml, child.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [value=dirty]\n- name: phantom-generated\n  literals: [value=dirty]\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"head",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(2, 0)}},
	)
	st := store.New()
	foreignSource := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       manifest.KindGitRepository,
		Namespace:  "flux-system",
		Name:       "cluster",
	}
	st.AddObject(foreignSource)
	foreignArtifact := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	st.SetArtifact(foreignSource.Named(), foreignArtifact)
	scan := filepath.Join(root, "flux")
	if testCaseInput.broad {
		scan = root
	}
	entries, err := Plan(
		t.Context(),
		Config{Path: scan, RepoRoot: root, SelfURLs: []string{"git://fixture.invalid/cluster"}},
	)
	assert.FatalIf(
		t,
		err != nil || len(entries) != 1 || entries[0].Path != scan,
		"committed ownership partitioned working-tree declarations: %+v, %v",
		entries,
		err,
	)
	result, err := Run(t.Context(), Config{
		Path: scan, RepoRoot: root, Store: st, SelfURLs: []string{"git://fixture.invalid/cluster"},
	})
	assert.FatalIf(t, err != nil, "%v", err)
	assert.FatalIf(
		t,
		st.GetArtifact(foreignSource.Named(), store.ArtifactContent) != foreignArtifact,
		"%v",
		"pinned source replaced a foreign artifact",
	)
	parent := testutil.KustomizationID("flux-system", "parent")
	childID := testutil.KustomizationID("flux-system", "child")
	phantom := testutil.KustomizationID("flux-system", "phantom")
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "settings"}
	artifact, available := st.GetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !available,

		"pinned artifact missing: records=%+v objects=%v",
		result.Index.Records,
		st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))

	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	generatorObj := st.GetObject(generator)
	_, generatorDoc, attempt := st.GetEmission(generator)
	attempt.Finish()
	assert.FatalIf(t, generatorObj == nil || generatorDoc == nil ||
		generatorDoc["metadata"].(map[string]any)["uid"] != "pinned", "pinned generator document = %#v", generatorDoc)
	generatorRecord, _ := result.Index.Lookup(generator)
	input := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	assert.FatalIf(
		t,
		!slices.Equal(generatorRecord.Refs, []manifest.NamedResource{input}),
		"pinned generator inputs = %v",
		generatorRecord.Refs,
	)
	output := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "pinned-output",
	}
	productions := st.Productions(output, result.Index)
	assert.FatalIf(
		t,
		len(productions) != 1 || productions[0].Owner != parent || productions[0].Declaration != generator ||
			productions[0].Source != input ||
			productions[0].File != generatorRecord.File,
		"pinned generator productions = %+v",
		productions,
	)
	assert.FatalIf(t, st.GetObject(phantom) != nil || st.GetObject(childID) == nil ||
		parentsForTest(result.Index)[childID] != parent,

		"pinned children: phantom=%v child=%v parent=%v",
		st.GetObject(phantom),
		st.GetObject(childID),
		parentsForTest(result.Index)[childID])

	generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated"}
	generatedCM, ok := values.NewStoreProvider(st).Data(generated).(*manifest.ConfigMap)
	assert.FatalIf(
		t,
		st.GetObject(generated) != nil,
		"%v",
		"pinned generator lookup entered canonical inventory",
	)
	assert.FatalIf(t, !ok || generatedCM.Data["value"] != "pinned" ||
		values.NewStoreProvider(st).
			Data(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "phantom-generated"}) !=
			nil, "pinned generators = %v", generatedCM)
	fileGenerated, ok := values.NewStoreProvider(st).Data(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "file-generated"}).(*manifest.ConfigMap)
	assert.FatalIf(
		t,
		!ok || fileGenerated.Data["value"] != "pinned",
		"committed generator file = %v",
		fileGenerated,
	)
	for _, id := range []manifest.NamedResource{childID, cm, generated, generator} {
		file := filepath.Join(root, filepath.FromSlash(filesForTest(result.Index)[id]))
		assert.FatalIf(t, !safepath.Contains(artifact.LocalPath, file),
			"source file for %s = %s, want under %s", id, file, artifact.LocalPath)
	}
	{
		got := renderersForTest(result.Index, cm)
		assert.FatalIf(
			t,
			!slices.Equal(got, []manifest.NamedResource{parent}),
			"self-produce = %v, want [%s]",
			got,
			parent,
		)
	}
	{
		got := testutil.Owners(
			st.Productions(cm, result.Index),
		)
		assert.FatalIf(t, !slices.Equal(
			got,
			[]manifest.NamedResource{parent},
		), "producers = %v, want [%s]", got, parent)
	}
}

func testAliasBootstrapSourcesDeclaredRefsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		base       string
		commit     bool
		ref        *sourcev1.GitRepositoryRef
		head       bool
		fallback   bool
		fail       bool
		noStore    bool
		sparse     bool
		submodules bool
		lookupErr  bool
	},
) {
	t.Helper()

	root := discoveryRefFixture(t)
	if testCaseInput.noStore {
		{
			err := os.RemoveAll(filepath.Join(root, ".git"))
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	st := store.New()
	repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	repository.URL, repository.Reference = "git://fixture.invalid/cluster", testCaseInput.ref
	var revision string
	if testCaseInput.base != "" {
		repo, err := git.PlainOpen(root)
		assert.FatalIf(t, err != nil, "%v", err)
		head, err := repo.Head()
		assert.FatalIf(t, err != nil, "%v", err)
		revision = head.Hash().String()
		if testCaseInput.commit {
			repository.Reference = &sourcev1.GitRepositoryRef{Commit: revision}
		}
		ignore := "apps/excluded.txt\n"
		repository.Ignore = &ignore
		owner := cachetest.New(t, "", sourcecache.New)
		result, err := baseline.AutoResolve(t.Context(), root, testCaseInput.base, owner)
		assert.FatalIf(t, err != nil, "%v", err)
		st.SetArtifact(manifest.BootstrapSourceID, result.BaseInput)
	}
	repository.RecurseSubmodules = testCaseInput.submodules
	if testCaseInput.sparse {
		repository.SparseCheckout = []string{"./apps"}
	}
	d := Session{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}
	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	if testCaseInput.lookupErr {
		input, err := gittree.AdmitDirectory(t.Context(), &gitrepo.Location{
			ContentRoot: root, LookupErr: errors.New("repository lookup failed"),
		})
		assert.NoError(t, err)
		st.SetArtifact(manifest.BootstrapSourceID, input)
	}
	st.AddObject(repository)
	bootstrap := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	side := bootstrap.LocalPath
	d.cfg.Path = side
	aliased, err := d.resolveEntrySources(t.Context(), false)
	assert.FatalIf(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != bootstrap ||
		st.GetObject(
			repository.Named(),
		) != repository, "%v", "discovery replaced the bootstrap or authored source")
	if testCaseInput.base != "" {
		want := "A"
		if testCaseInput.head {
			want = "B"
		}
		content, innerInnerInnerInnerLocalErr := fs.ReadFile(bootstrap.FS, "value.txt")
		assert.FatalIf(
			t,
			innerInnerInnerInnerLocalErr != nil || string(content) != want || bootstrap.Key == "" ||
				bootstrap.LocalRoot != root,
			"baseline content = %q, %v; want %s with immutable checkout identity: %+v",
			content,
			innerInnerInnerInnerLocalErr,
			want,
			bootstrap,
		)
	}
	if testCaseInput.fail {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux) ||
			st.GetArtifact(repository.Named(), store.ArtifactContent) != nil,
			"invalid source = %v, %v", err, st.GetArtifact(repository.Named(), store.ArtifactContent))

		return
	}
	assert.FatalIf(t, err != nil, "%v", err)
	if testCaseInput.fallback {
		assert.FatalIf(
			t,
			st.GetArtifact(repository.Named(), store.ArtifactContent) != nil,
			"%v",
			"fallback installed a HEAD artifact",
		)
		{
			status, ok := st.GetStatus(repository.Named())
			assert.FatalIf(
				t,
				ok && status.Status == store.StatusReady,
				"%v",
				"fallback marked source ready",
			)
		}
		warnings := st.Warnings()
		detail := []string{
			(*manifest.GitRepositoryRef)(repository.Reference).String(),
			"ref unavailable in this tree's repository or checkout options require fetching",
		}
		assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnSourceRefUnavailable ||
			warnings[0].Resource != repository.Named() ||
			warnings[0].Count != 1 ||
			!slices.Equal(warnings[0].Detail, detail), "fallback warning = %+v", warnings)
		return
	}
	shouldReturn := testAliasBootstrapSourcesDeclaredRefsCaseTail(
		t,
		&testAliasBootstrapSourcesDeclaredRefsCaseTailInputs{
			st:            &st,
			repository:    &repository,
			testCaseInput: &testCaseInput,
			revision:      &revision,
			side:          &side,
			d:             &d,
			aliased:       &aliased,
			bootstrap:     &bootstrap,
			root:          &root,
		},
	)
	if shouldReturn {
		return
	}
}
