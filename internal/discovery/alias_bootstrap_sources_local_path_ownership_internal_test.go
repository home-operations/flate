package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/helm"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/source"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAliasBootstrapSources_LocalPathOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, url string
		local, late     bool
	}{
		{name: "pinned-local", kind: manifest.KindGitRepository, url: "git://fixture.invalid/cluster", local: true},
		{name: "external-git", kind: manifest.KindGitRepository, url: "git://fixture.invalid/other"},
		{name: "external-oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/cluster"},
		{name: "external-bucket", kind: manifest.KindBucket, url: "https://fixture.invalid/cluster"},
		{name: "late-pinned-local", kind: manifest.KindGitRepository, url: "git://fixture.invalid/cluster", local: true, late: true},
		{name: "late-external-git", kind: manifest.KindGitRepository, url: "git://fixture.invalid/other", late: true},
		{name: "late-external-oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/cluster", late: true},
		{name: "late-external-bucket", kind: manifest.KindBucket, url: "https://fixture.invalid/cluster", late: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAliasBootstrapSourcesLocalPathOwnershipCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestAliasBootstrapSources_PreservesCanonicalSource(t *testing.T) {
	t.Parallel()
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_view_pinned_%t", pinned), func(t *testing.T) {
			t.Parallel()
			testAliasBootstrapSourcesPreservesCanonicalSourceCase(
				t,
				&pinned,
			)
		},
		)
	}

	root := t.TempDir()
	st := store.New()
	repository := &manifest.GitRepository{Name: "flux-system", Namespace: "flux-system"}
	repository.URL = "git://fixture.invalid/cluster"
	repository.Reference = &sourcev1.GitRepositoryRef{Commit: "invalid"}
	st.AddObject(repository)
	d := Session{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	artifact := st.GetArtifact(repository.Named(), store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t,
		artifact.LocalPath != root || st.GetObject(repository.Named()) != repository,
		"canonical bootstrap or authored object changed: %+v",
		artifact,
	)

	t.Run("mutable alias refresh", func(t *testing.T) {
		t.Parallel()
		testAliasBootstrapSourcesPreservesCanonicalSourceMutableAliasRefresh(
			t,
			testAliasBootstrapSourcesPreservesCanonicalSourceMutableAliasRefreshInputs{
				root:       &root,
				st:         &st,
				repository: &repository,
				d:          &d,
			},
		)
	},
	)
}

func discoveryRefFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(t, root, "value.txt", value)
		testutil.WriteFile(t, root, "apps/value.txt", value)
		testutil.WriteFile(t, root, "apps/ignored.txt", value)
		testutil.WriteFile(t, root, "apps/excluded.txt", value)
		testutil.WriteFile(t, root, ".sourceignore", "apps/ignored.txt\n")
		hash := testutil.GitCommitAll(
			t,
			repo,
			value,
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
		)

		{
			err := repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName([]string{"v1.0.0", "v2.0.0"}[i]), hash),
			)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	testutil.WriteFile(t, root, "value.txt", "dirty")
	testutil.WriteFile(t, root, "apps/value.txt", "dirty")
	return root
}

func TestAliasBootstrapSources_NilCacheIsLazy(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("isolated default cache uses XDG_CACHE_HOME on Linux")
	}
	fallback := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", fallback)
	root := discoveryRefFixture(t)
	defaultRoot := filepath.Join(fallback, "flate")
	for _, tag := range []string{"", "v2.0.0", "missing", "v1.0.0"} {
		repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
		repository.URL = "git://fixture.invalid/cluster"
		if tag != "" {
			repository.Reference = &sourcev1.GitRepositoryRef{Tag: tag}
		}
		st := store.New()
		st.AddObject(repository)
		d := Session{cfg: Config{Path: root, RepoRoot: root, Store: st, SelfURLs: []string{repository.URL}}}

		{
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		{
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		_, err := os.Stat(defaultRoot)

		assert.FatalIf(t, !os.IsNotExist(err), "default cache created for %q: %v", tag, err)

		if tag == "v1.0.0" {
			artifact := st.GetArtifact(repository.Named(), store.ArtifactContent).(*store.SourceArtifact)

			assert.FatalIf(t, artifact.FS == nil || artifact.Key == "", "committed source = %+v", artifact)
		}
	}
}

type warningFailureFS struct{}

func (warningFailureFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
}

func TestEntrySourceView_WarningEvidence(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(strconv.FormatBool(failed), func(t *testing.T) {
			t.Parallel()
			testEntrySourceViewWarningEvidenceCase(t, &failed)
		},
		)
	}
}

func testAliasBootstrapSourcesLocalPathOwnershipCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		kind  string
		url   string
		local bool
		late  bool
	},
) {
	t.Helper()

	root := discoveryRefFixture(t)
	st := store.New()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      testCaseInput.kind,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	switch testCaseInput.kind {
	case manifest.KindGitRepository:
		st.AddObject(&manifest.GitRepository{
			Name: id.Name, Namespace: id.Namespace, URL: testCaseInput.url,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		})
	case manifest.KindOCIRepository:
		st.AddObject(&manifest.OCIRepository{Name: id.Name, Namespace: id.Namespace, URL: testCaseInput.url})
	default:
		bucket := &manifest.Bucket{Name: id.Name, Namespace: id.Namespace}
		bucket.Endpoint, bucket.BucketName = testCaseInput.url, id.Name
		st.AddObject(bucket)
	}
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: id.Namespace,
		Path:      "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      id.Kind,
			Name:      id.Name,
			Namespace: id.Namespace,
		},
	}
	st.AddObject(ks)
	d := Session{cfg: Config{
		Path: root, RepoRoot: root, Store: st, SelfURLs: []string{"git://fixture.invalid/cluster"},
	}}
	if testCaseInput.late {
		{
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		st.DeleteObject(id)

		{
			_, err := d.resolveEntrySources(t.Context(), true)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		{
			got := st.GetArtifact(
				id,
				store.ArtifactContent,
			) != nil
			assert.FatalIf(
				t,
				got != (testCaseInput.kind == manifest.KindGitRepository),
				"missing-source alias = %t for %s",
				got,
				testCaseInput.kind,
			)
		}
		ref := ""
		if testCaseInput.kind == manifest.KindGitRepository {
			ref = "\n  ref: {tag: v1.0.0}"
		}
		field := "url"
		if testCaseInput.kind == manifest.KindBucket {
			field, ref = "endpoint", "\n  bucketName: cluster"
		}
		testutil.WriteFile(
			t,
			root,
			"authored/source.yaml",
			fmt.Sprintf(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: cluster, namespace: flux-system}\nspec:\n  %s: %q%s\n",
				testCaseInput.kind,
				field,
				testCaseInput.url,
				ref,
			),
		)
		l := loader.New(st)
		l.SourceRoot, l.PreferExisting = root, true

		{
			_, err := l.Load(t.Context(), filepath.Join(root, "authored"), manifest.NamedResource{}, nil)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		l.Admit(root)
	}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	authored := st.GetObject(id)

	{
		aliases, err := d.resolveEntrySources(
			t.Context(),
			true,
		)
		assert.FatalIf(t, err != nil || aliases != 0 ||
			st.GetObject(id) != authored, "authored source replaced: %v, %v", aliases, err)
	}

	assert.FatalIf(
		t,
		!testCaseInput.local && st.GetArtifact(id, store.ArtifactContent) != nil,
		"%v",
		"external source acquired by aliasing",
	)

	if testCaseInput.local {
		artifact := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		_, identity, viewErr := source.OperationView(t.Context(), artifact.FS)
		if viewErr != nil || identity == "" || artifact.Content != identity {
			t.Fatalf("pinned view identity = %q, want %q: %v", artifact.Content, identity, viewErr)
		}

		assert.FatalIf(t, artifact.LocalPath == root, "%v", "pinned source must render its committed tree")
	} else {
		st.SetArtifact(id, &store.SourceArtifact{Kind: id.Kind, URL: testCaseInput.url, LocalPath: t.TempDir()})
	}
	l := loader.New(st)

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	child := testutil.HelmReleaseID("apps", "release")
	ownersOfFileResult := l.Index.OwnersOfFile("apps/release.yaml")
	owners := ownersOfFileResult.Owners
	parent := manifest.NamedResource{}
	for _, owner := range owners {
		if owner != child {
			parent = owner
			break
		}
	}
	owned := parent != (manifest.NamedResource{})

	assert.FatalIf(
		t,
		owned != testCaseInput.local || owned && parent != ks.Named(),
		"local path ownership = %v, %v; want local=%v",
		parent,
		owned,
		testCaseInput.local,
	)
}

func testAliasBootstrapSourcesPreservesCanonicalSourceCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	bootstrap := &store.SourceArtifact{
		Kind:      manifest.KindGitRepository,
		LocalPath: root,
		LocalRoot: root,
		Revision:  "one",
		Digest:    "aaa",
		FS:        fstest.MapFS{"file": {Data: []byte("first")}},
	}
	if *testCaseInput {
		bootstrap.Key, bootstrap.Content = "family", "generation"
	}
	st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
	d := Session{cfg: Config{Path: root, Store: st, SelfURLs: []string{"https://fixture.invalid/repo"}}}
	ids := []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "one", Name: "source"},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "two", Name: "alias"},
	}
	for _, id := range ids {
		if id == ids[1] {
			repository := &manifest.GitRepository{
				Name:      id.Name,
				Namespace: id.Namespace,
				URL:       "https://fixture.invalid/repo",
			}
			repository.SparseCheckout = []string{}
			st.AddObject(repository)
		}
		st.AddObject(
			&manifest.Kustomization{
				Name:      id.Name,
				Namespace: id.Namespace,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name},
			},
		)

		{
			count, err := d.resolveEntrySources(t.Context(), true)
			assert.FatalIf(t, err != nil || count != 1, "publish alias = %d, %v", count, err)
		}
	}
	first := st.GetArtifact(ids[0], store.ArtifactContent).(*store.SourceArtifact)
	second := st.GetArtifact(ids[1], store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t,
		first.Key != "" || second.Key != "" || first.LocalPath == second.LocalPath || first.LocalRoot != root ||
			second.LocalRoot != root ||
			!first.InputAlias ||
			!second.InputAlias,
		"alias view/ownership = %+v, %+v",
		first,
		second,
	)

	generations := []struct{ revision, digest string }{{"two", "aaa"}, {"two", "bbb"}}
	if !(*testCaseInput) {
		generations = append(generations, generations[1])
	}
	for i, generation := range generations {
		next := *bootstrap
		next.Revision, next.Digest = generation.revision, generation.digest
		if *testCaseInput {
			next.Content = fmt.Sprintf("generation-%d", i)
		}
		next.FS = fstest.MapFS{"file": {Data: []byte("other" + first.Revision + first.Digest)}}
		st.SetArtifact(manifest.BootstrapSourceID, &next)

		{
			_, err := d.resolveEntrySources(t.Context(), true)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		got := st.GetArtifact(ids[0], store.ArtifactContent).(*store.SourceArtifact)

		assert.FatalIf(t, got.FS == first.FS || got.Key != "" || got.Revision != generation.revision ||
			got.Digest != generation.digest, "generation reused alias identity: %+v", got)

		data, err := fs.ReadFile(got.FS, "file")

		assert.FatalIf(
			t,
			err != nil || string(data) != "other"+first.Revision+first.Digest,
			"alias generation bytes = %q, %v",
			data,
			err,
		)

		first, bootstrap = got, &next
	}
}

type testAliasBootstrapSourcesPreservesCanonicalSourceMutableAliasRefreshInputs struct {
	root       *string
	st         **store.Store
	repository **manifest.GitRepository
	d          *Session
}

func testAliasBootstrapSourcesPreservesCanonicalSourceMutableAliasRefresh(
	t *testing.T,
	testCaseInput testAliasBootstrapSourcesPreservesCanonicalSourceMutableAliasRefreshInputs,
) {
	t.Helper()

	testutil.WriteFile(t, (*testCaseInput.root), "chart/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 1.0.0\n")
	testutil.WriteFile(
		t, (*testCaseInput.root), "chart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: main\n",
	)
	testutil.WriteFile(t, (*testCaseInput.root), "excluded/file", "hidden")
	alias := &manifest.GitRepository{Name: "alias", Namespace: "flux-system", URL: (*testCaseInput.repository).URL}
	alias.Ignore = new("excluded/\n")
	(*testCaseInput.st).AddObject(alias)
	count, err := testCaseInput.d.resolveEntrySources(t.Context(), true)

	assert.FatalIf(t, err != nil || count != 1, "alias: %d, %v", count, err)

	art := (*testCaseInput.st).GetArtifact(alias.Named(), store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t,
		art.Key != "" || !art.InputAlias || art.LocalRoot != (*testCaseInput.root) ||
			art.LocalPath == (*testCaseInput.root),
		"alias coordinates: %+v",
		art,
	)

	owner := cachetest.New(t, "", sourcecache.New)
	cli := helm.NewClient(owner)

	hr := &manifest.HelmRelease{
		Name:      "release",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Name: "chart",
			Source: manifest.NamedResource{
				Group:     alias.Named().Group,
				Kind:      alias.Named().Kind,
				Namespace: alias.Namespace,
				Name:      alias.Name,
			},
		},
	}
	for _, exists := range []bool{false, true, false} {
		if exists {
			testutil.WriteFile(
				t, (*testCaseInput.root), "chart/templates/arrived/deep/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: arrived\n",
			)
		} else {
			{
				err := os.RemoveAll(filepath.Join((*testCaseInput.root), "chart", "templates", "arrived"))
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
		out, _, err := testutil.RenderMaps(
			cli.Template(
				t.Context(),
				hr,
				nil,
				helm.RenderOptions{Options: helm.Options{}, Sources: helm.NewSources((*testCaseInput.st))},
			),
		)

		assert.FatalIf(t, err != nil, "%v", err)

		assert.FatalIf(t, slices.ContainsFunc(
			out,
			func(doc map[string]any) bool {
				docMetadataResult := manifest.DocMetadata(doc)
				name := docMetadataResult.Name
				return name == "arrived"
			},
		) != exists, "alias membership stale: %s", out)

		assert.FatalIf(
			t, (*testCaseInput.st).GetArtifact(alias.Named(), store.ArtifactContent) != art, "%v",
			"Template republished alias",
		)

		view, pin, err := source.OperationView(t.Context(), art.FS)

		assert.FatalIf(t, err != nil || pin != "", "alias admission: %q, %v", pin, err)

		{
			_, err := fs.Stat(view, "excluded/file")
			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "alias lost shaping: %v", err)
		}
	}

	{
		_, err := testCaseInput.d.resolveEntrySources(t.Context(), true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	assert.FatalIf(
		t,
		len(testCaseInput.d.aliasViews) != 1 || (*testCaseInput.st).GetObject(alias.Named()) != alias,
		"%v",
		"alias refresh changed registry or authored object",
	)
}

func testEntrySourceViewWarningEvidenceCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "app/file.yaml", "content")
	current := os.DirFS(root)
	if *testCaseInput {
		current = warningFailureFS{}
	}
	input, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: current},
	)
	assert.NoError(t, err)
	raw := manifest.Warning{
		Category: manifest.WarnSkippedSubmodule,
		Message:  "skipping submodule",
		Detail:   []string{"app/module"},
	}
	input.Warnings = []manifest.Warning{raw}
	invocation := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "current only"}
	bootstrap := &store.SourceArtifact{LocalPath: root, FS: input, Warnings: []manifest.Warning{invocation}}
	obj := &manifest.GitRepository{Name: "alias", Namespace: "apps"}
	d := &Admission{aliasViews: map[string]*store.SourceArtifact{}}
	for range 2 {
		artifact, _, err := d.entrySourceView(
			t.Context(),
			&entryResolution{object: obj, input: bootstrap, git: obj},
		)

		assert.FatalIf(
			t, (*testCaseInput) &&
				!errors.Is(
					err,
					fs.ErrPermission,
				) || !(*testCaseInput) && err != nil, "source view error=%v, failed=%t",
			err, (*testCaseInput),
		)

		if *testCaseInput {
			assert.FatalIf(t, artifact != nil, "failed source view = %+v", artifact)

			continue
		}

		assert.FatalIf(
			t,
			artifact == nil || len(artifact.Warnings) != 0,
			"alias warning evidence = %+v",
			artifact,
		)

		if input := artifact.FS.(*gittree.FS); len(input.Warnings) != 0 {
			t.Fatalf("derived filesystem warnings = %+v", input.Warnings)
		}
	}
	assert.Diff(t, input.Warnings, []manifest.Warning{raw})
	assert.Diff(t, bootstrap.Warnings, []manifest.Warning{invocation})
}
