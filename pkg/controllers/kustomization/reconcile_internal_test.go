package kustomization

import (
	"archive/tar"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/artifactgenerator"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func newControllerWithFixture(t *testing.T) (*Controller, *store.Store, string) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "kustomization.yaml"), "resources:\n- cm.yaml\n")
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default}
data: {greeting: hi}
`)

	s := store.New()
	bootstrap := &manifest.GitRepository{
		Name: "flux-system", Namespace: "flux-system",
		URL: "file://" + root,
	}
	s.AddObject(bootstrap)
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	s.SetArtifact(bootstrap.Named(), input)
	s.UpdateStatus(bootstrap.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "ready"})

	owner := cachetest.New(t, "", sourcecache.New)
	tasks := task.NewBounded(2)
	network := new(source.Network)
	t.Cleanup(network.Close)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	t.Cleanup(inputs.Close)
	cache := kustomize.NewTreeCache(owner, &inputs, tasks)
	t.Cleanup(cache.Close)

	c := New(s, tasks, cache)
	c.Configure(Options{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	t.Cleanup(func() {
		cache.Close()
		tasks.BlockTillDone()
	})
	return c, s, root
}

func freezeSourceForTest(t *testing.T, st *store.Store) {
	t.Helper()
	original := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	owner := cachetest.New(t, "", sourcecache.New)
	input, _, key, err := owner.Acquire(
		t.Context(),
		sourcecache.ArchiveKey{Policy: "fixed", Content: t.Name()},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		func(_ context.Context, target sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			writer := tar.NewWriter(target)
			if err := writer.AddFS(original.FS); err != nil {
				return sourcecache.ProducerResult{}, err
			}
			return sourcecache.ProducerResult{}, writer.Close()
		},
	)
	assert.NoError(t, err)
	pinned := *original
	pinned.FS, pinned.Key, pinned.Content = input, key, sourcecache.AcquisitionIdentity(input)
	st.SetArtifact(manifest.BootstrapSourceID, &pinned)
}

const namespaceIdentityDocs = `apiVersion: v1
kind: ConfigMap
metadata: {name: hello}
data: {greeting: hi}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: chart}
spec: {url: oci://example.test/chart}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: app}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: role}
---
apiVersion: foreign.example/v1
kind: ClusterRole
metadata: {name: foreign}
`

func TestReconcile_HappyPath(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	list := "apiVersion: v1\nkind: List\nitems:\n"
	var listSb134 strings.Builder
	for doc := range strings.SplitSeq(namespaceIdentityDocs, "---\n") {
		listSb134.WriteString("- " + strings.ReplaceAll(strings.TrimSpace(doc), "\n", "\n  ") + "\n")
	}
	list += listSb134.String()
	testutil.WriteFile(t, root, "apps/cm.yaml", list)
	freezeSourceForTest(t, s)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path:            "./apps",
		TargetNamespace: "rendered",
		CommonMetadata:  &kustomizev1.CommonMetadata{Labels: map[string]string{"team": "flate"}},
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	assert.ErrorIf(t, info.Message != "", "unexpected Ready message: %q", info.Message)
	art, _ := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, art == nil, "%v", "Emission not stored")
	assert.ErrorIf(t, art.Fingerprint == "", "%v", "artifact missing fingerprint")
	assert.ErrorIf(t, len(art.Manifests) == 0, "%v", "artifact has no rendered manifests")
	assert.Equal(t, len(art.Manifests), 5)
	for _, doc := range art.Manifests {
		metadata2 := manifest.DocMetadata(doc.Document)
		name, ns := metadata2.Name, metadata2.Namespace
		labels := doc.Document["metadata"].(map[string]any)["labels"].(map[string]any)
		assert.Equal(t, labels["team"], any("flate"))
		assert.Equal(t, labels["kustomize.toolkit.fluxcd.io/name"], any(ks.Name))
		want := ks.TargetNamespace
		if manifest.DocKind(doc.Document) == "ClusterRole" &&
			manifest.DocAPIVersion(doc.Document) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		assert.Equal(t, ns, want)
		id := manifest.NamedResource{
			Group: schema.FromAPIVersionAndKind(
				manifest.DocAPIVersion(doc.Document),
				manifest.DocKind(doc.Document),
			).Group,
			Kind:      manifest.DocKind(doc.Document),
			Namespace: want,
			Name:      name,
		}
		assert.ErrorIf(
			t,
			!slices.Contains(testutil.Owners(s.Productions(id, nil)), ks.Named()),
			"child %s lost parent edge",
			id,
		)
		assert.ErrorIf(t, s.GetObject(id) == nil, "missing effective child %s/%s", want, name)
	}
}

func TestReconcile_ArtifactGeneratorPersistedDocument(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skipf("render persistence disabled: %s", reason)
	}
	root := testutil.WriteTree(t, map[string]string{
		"apps/kustomization.yaml": "resources: [generator.yaml]\n",
		"apps/generator.yaml": `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata:
  name: generated
  namespace: apps
  uid: constructed-document
  labels: {purpose: retained}
  annotations: {example.test/note: rendered}
spec:
  sources: [{alias: repo, kind: GitRepository, name: input}]
  commonMetadata: {labels: {purpose: generated}}
  artifacts:
    - name: output
      revision: "@repo"
      originRevision: "@repo"
      copy: [{from: "@repo/file", to: "@artifact/file"}]
`,
	})
	storage := t.TempDir()
	var fresh map[string]any
	for _, persisted := range []bool{false, true} {
		owner, err := sourcecache.New(t.Context(), storage)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, owner.Close()) })
		s := store.New()
		tasks := task.NewBounded(2)
		t.Cleanup(tasks.BlockTillDone)
		network := new(source.Network)
		t.Cleanup(network.Close)
		inputs := source.NewHTTPInputs(owner, network, tasks)
		t.Cleanup(inputs.Close)
		trees := kustomize.NewTreeCache(owner, &inputs, tasks)
		t.Cleanup(trees.Close)
		c := New(s, tasks, trees)

		c.Configure(Options{}, sourcecache.RetryConfig{})
		c.StartLifecycle()
		g := artifactgenerator.New(s, tasks, owner)

		guard := &guardedSourceFS{FS: os.DirFS(root)}
		guard.deny.Store(persisted)
		renderSource := &manifest.GitRepository{Name: "render-source", Namespace: "apps"}
		s.AddObject(renderSource)
		s.SetArtifact(
			renderSource.Named(),
			&store.SourceArtifact{
				FS:        guard,
				LocalPath: root,
				Key:       "render-pin",
				Content:   "render-content",
				Digest:    "sha256:render",
				Revision:  "render-revision",
			},
		)
		s.UpdateStatus(renderSource.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
		ks := &manifest.Kustomization{
			Name:      "apps",
			Namespace: "apps",
			Path:      "./apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository,
				Name: renderSource.Name,
			},
		}
		s.AddObject(ks)
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "persisted=%t render status=%+v", persisted, info)
		id := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: "apps",
			Name:      "generated",
		}
		var reads store.ReadSet
		reads.Init(s, id, nil)
		obj, doc, attempt := reads.GetEmission(id)
		assert.FatalIf(
			t,
			obj == nil || doc == nil || !reads.Valid() || s.EmissionOwner(id) != ks.Named(),
			"persisted=%t missing authoritative generator: %v, %v",
			persisted,
			obj,
			doc,
		)
		if persisted {
			assert.Diff(t, doc, fresh)
			assert.Equal(t, guard.opened.Load(), int64(0))
			t.Log("persistence enabled; reopened disk-hit witness: rendered document with source reads denied, opens=0")
		} else {
			fresh = manifest.DeepCopyMap(doc)
			metadata := doc["metadata"].(map[string]any)
			assert.Equal(t, metadata["uid"], any("constructed-document"))
			assert.Equal(t, metadata["annotations"].(map[string]any)["example.test/note"], any("rendered"))
			assert.FatalIf(t, guard.opened.Load() == 0, "%v", "fresh rendering did not consume source files")
		}
		attempt.Finish()
		reads.Close()
		repo := &manifest.GitRepository{Name: "input", Namespace: "apps"}
		s.AddObject(repo)
		s.SetArtifact(
			repo.Named(),
			&store.SourceArtifact{
				FS:       fstest.MapFS{"file": {Data: []byte("generated bytes")}},
				Key:      "input-pin",
				Digest:   "sha256:input",
				Revision: "input-revision",
				Metadata: map[string]string{"org.opencontainers.image.revision": "input-origin"},
			},
		)
		s.UpdateStatus(repo.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
		info = controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: tasks, Store: s, ID: id, ReconcileNode: g.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "persisted=%t generator status=%+v", persisted, info)
		target := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      "output",
		}
		payload, ok := s.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok, "%v", "generated payload missing")
		data, err := fs.ReadFile(payload.FS, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "generated bytes")
		assert.Equal(t, payload.Revision, "input-revision")
		assert.Equal(t, payload.Metadata["org.opencontainers.image.revision"], "input-origin")
		_, generated, output := s.GetEmission(target)
		assert.Equal(
			t,
			generated["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"],
			"constructed-document",
		)
		output.Finish()
		tasks.BlockTillDone()
		trees.Close()
		inputs.Close()
		network.Close()
		assert.NoError(t, owner.Close())
	}
}

func TestReconcile_EmptyPostBuildRespectsSubstituteStrategy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		strategy  kustomizev1.SubstituteStrategy
		postBuild map[string]any
		disabled  string
		input     string
		want      string
		reference string
	}{
		{name: "no postBuild", want: "${VAR:=default}"},
		{name: "unset", postBuild: map[string]any{}, want: "${VAR:=default}"},
		{
			name:      "WithVariables",
			strategy:  kustomizev1.SubstituteStrategyWithVariables,
			postBuild: map[string]any{},
			want:      "${VAR:=default}",
		},
		{name: "Always", strategy: kustomizev1.SubstituteStrategyAlways, postBuild: map[string]any{}, want: "default"},
		{name: "empty substitute", postBuild: map[string]any{"substitute": map[string]any{}}, want: "${VAR:=default}"},
		{
			name: "missing optional ConfigMap",
			postBuild: map[string]any{
				"substituteFrom": []any{map[string]any{"kind": "ConfigMap", "name": "missing", "optional": true}},
			},
			want: "${VAR:=default}",
		},
		{
			name: "missing optional Secret",
			postBuild: map[string]any{
				"substituteFrom": []any{map[string]any{"kind": "Secret", "name": "missing", "optional": true}},
			},
			want: "${VAR:=default}",
		},
		{
			name:      "inline substitute",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			want:      "configured",
		},
		{
			name:      "supplied default",
			input:     "${ENV:=prod}",
			postBuild: map[string]any{"substitute": map[string]any{"ENV": "stage"}},
			want:      "stage",
		},
		{
			name:      "nested default",
			input:     "${ENV:=${OTHER:=prod}}",
			postBuild: map[string]any{"substitute": map[string]any{"OTHER": "stage"}},
			want:      "stage",
		},
		{
			name:      "escaped default",
			input:     "$${ENV:=prod}",
			postBuild: map[string]any{"substitute": map[string]any{"ENV": "stage"}},
			want:      "${ENV:=prod}",
		},
		{
			name:      "ConfigMap producer",
			reference: "ConfigMap",
			postBuild: map[string]any{"substituteFrom": []any{map[string]any{"kind": "ConfigMap", "name": "settings"}}},
			want:      "referenced",
		},
		{
			name:      "Secret producer",
			reference: "Secret",
			postBuild: map[string]any{"substituteFrom": []any{map[string]any{"kind": "Secret", "name": "settings"}}},
			want:      "referenced",
		},
		{
			name:      "inline over ConfigMap",
			reference: "ConfigMap",
			postBuild: map[string]any{
				"substituteFrom": []any{map[string]any{"kind": "ConfigMap", "name": "settings"}},
				"substitute":     map[string]any{"VAR": "configured"},
			},
			want: "configured",
		},
		{
			name:      "inline over Secret",
			reference: "Secret",
			postBuild: map[string]any{
				"substituteFrom": []any{map[string]any{"kind": "Secret", "name": "settings"}},
				"substitute":     map[string]any{"VAR": "configured"},
			},
			want: "configured",
		},
		{
			name:      "disabled label",
			strategy:  kustomizev1.SubstituteStrategyAlways,
			postBuild: map[string]any{},
			disabled:  "labels",
			want:      "${VAR:=default}",
		},
		{
			name:      "disabled annotation",
			strategy:  kustomizev1.SubstituteStrategyAlways,
			postBuild: map[string]any{},
			disabled:  "annotations",
			want:      "${VAR:=default}",
		},
		{name: "double dollar without variables", input: "echo $$PID", want: "echo $$PID"},
		{
			name:      "double dollar Always",
			input:     "echo $$PID",
			strategy:  kustomizev1.SubstituteStrategyAlways,
			postBuild: map[string]any{},
			want:      "echo $PID",
		},
		{
			name:      "double dollar with variables",
			input:     "echo $$PID",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			want:      "echo $PID",
		},
		{
			name:      "double dollar disabled label",
			input:     "echo $$PID",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			disabled:  "labels",
			want:      "echo $$PID",
		},
		{
			name:      "double dollar disabled annotation",
			input:     "echo $$PID",
			postBuild: map[string]any{"substitute": map[string]any{"VAR": "configured"}},
			disabled:  "annotations",
			want:      "echo $$PID",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReconcileEmptyPostBuildRespectsSubstituteStrategyCase(
				t,
				&tc,
			)
		},
		)
	}
}

func testReconcileEmptyPostBuildRespectsSubstituteStrategyCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		strategy kustomizev1.
				SubstituteStrategy
		postBuild map[string]any
		disabled  string
		input     string

		want      string
		reference string
	},
) {
	t.Helper()

	c, s, root := newControllerWithFixture(t)
	s.AddObject(
		&manifest.Kustomization{
			Name:      "unrelated",
			Namespace: "flux-system",
			PostBuild: &kustomizev1.PostBuild{
				Substitute: map[string]string{"VAR": "unrelated", "ENV": "unrelated"},
			},
		},
	)
	switch testCaseInput.reference {
	case "ConfigMap":
		s.AddObject(
			&manifest.ConfigMap{
				Name:      "settings",
				Namespace: "flux-system",
				Data:      map[string]any{"VAR": "referenced"},
			},
		)
	case "Secret":
		s.AddObject(
			&manifest.Secret{
				Name:       "settings",
				Namespace:  "flux-system",
				StringData: map[string]any{"VAR": "referenced"},
			},
		)
	}
	input := testCaseInput.input
	if input == "" {
		input = "${VAR:=default}"
	}
	optOut := ""
	if testCaseInput.disabled != "" {
		optOut = fmt.Sprintf(", %s: {kustomize.toolkit.fluxcd.io/substitute: disabled}", testCaseInput.disabled)
	}
	testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), fmt.Sprintf(`apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: hello, namespace: default%s}
  data: {greeting: '%[2]s'}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system%[1]s}
spec:
  path: '%[2]s'
  sourceRef: {kind: GitRepository, name: '%[2]s', namespace: '%[2]s'}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system%[1]s}
spec: {url: 'https://example.invalid/repo', ref: {branch: '%[2]s'}}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: flux-system%[1]s}
spec:
  chart: {spec: {chart: example, sourceRef: {kind: HelmRepository, name: charts}}}
  valuesFrom: [{kind: ConfigMap, name: '%[2]s', valuesKey: '%[2]s', optional: true}]
`, optOut, input))

	spec := map[string]any{
		"path":      "./apps",
		"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
	}
	if testCaseInput.postBuild != nil {
		if testCaseInput.strategy != "" {
			testCaseInput.postBuild["substituteStrategy"] = string(testCaseInput.strategy)
		}
		spec["postBuild"] = testCaseInput.postBuild
	}
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       spec,
	}, manifest.ParseDocOptions{DefaultNamespace: ""})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "ParseDoc: %v", err)
	ks := obj.(*manifest.Kustomization)
	s.AddObject(ks)
	l := loader.New(s)
	l.SourceRoot = root
	_, err = l.Load(t.Context(), filepath.Join(root, "apps"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	s.SetDiscoveryIndex(l.Index)
	childID := testutil.KustomizationID("flux-system", "child")
	record, found := l.Index.Lookup(childID)
	assert.FatalIf(
		t,
		!found || record.Parent != ks.Named() || s.GetObject(childID) != nil,
		"child must start as owned discovery evidence: record=%+v object=%v",
		record,
		s.GetObject(childID),
	)
	authored := manifest.DeepCopyMap(spec)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         s,
				ID:            ks.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}
	art, _ := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		art == nil || len(art.Manifests) != 4,
		"expected four rendered manifests, got %+v",
		art,
	)
	for _, doc := range art.Manifests {
		if manifest.DocKind(doc.Document) == manifest.KindConfigMap {
			assert.Equal(t, doc.Document["data"].(map[string]any)["greeting"], any(testCaseInput.want))
		}
	}
	published := make(map[manifest.NamedResource]map[string]any)
	verifySubstitutionPublications(
		t,
		&substitutionPublications{
			testCaseInput: &testCaseInput,
			c:             &c,
			s:             &s,
			spec:          &spec,
			ks:            &ks,
			childID:       &childID,
			authored:      &authored,
			published:     &published,
		},
	)
}

type substitutionPublications struct {
	testCaseInput **struct {
		name      string
		strategy  kustomizev1.SubstituteStrategy
		postBuild map[string]any
		disabled  string
		input     string
		want      string
		reference string
	}
	c         **Controller
	s         **store.Store
	spec      *map[string]any
	ks        **manifest.Kustomization
	childID   *manifest.NamedResource
	authored  *map[string]any
	published *map[manifest.NamedResource]map[string]any
}

func verifySubstitutionPublications(t *testing.T, scope *substitutionPublications) {
	t.Helper()
	for run := range 2 {
		for _, id := range []manifest.NamedResource{
			(*scope.childID),
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "source"},
			{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "release"},
		} {
			obj, doc, held := (*scope.s).GetEmission(id)
			held.Finish()
			assert.FatalIf(
				t,
				obj == nil || doc == nil || (*scope.s).EmissionOwner(id) != (*scope.ks).Named(),
				"missing published child %s: %v %v",
				id,
				obj,
				doc,
			)
			scope.checkChild(t, obj, doc, id)
			if run == 0 {
				(*scope.published)[id] = manifest.DeepCopyMap(doc)
			} else {
				assert.Diff(t, doc, (*scope.published)[id])
			}
		}
		assert.Diff(t, (*scope.spec), (*scope.authored))
		if run == 0 {
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{
					Tasks:         (*scope.c).Tasks,
					Store:         (*scope.s),
					ID:            (*scope.ks).Named(),
					ReconcileNode: (*scope.c).ReconcileNode,
				},
			)
			assert.Equal(t, info.Status, store.StatusReady)
		}
	}
}

func (p *substitutionPublications) checkChild(
	t *testing.T,
	obj manifest.BaseManifest,
	doc map[string]any,
	id manifest.NamedResource,
) {
	t.Helper()
	var fields []string
	typedSpec := doc["spec"].(map[string]any)
	switch obj := obj.(type) {
	case *manifest.Kustomization:
		fields = []string{
			obj.Path,
			obj.SourceRef.Name,
			obj.SourceRef.Namespace,
			typedSpec["path"].(string),
			typedSpec["sourceRef"].(map[string]any)["name"].(string),
			typedSpec["sourceRef"].(map[string]any)["namespace"].(string),
		}
		assert.Equal(t, obj.SourceID().Name, (*p.testCaseInput).want)
		assert.Equal(t, obj.SourceID().Namespace, (*p.testCaseInput).want)
	case *manifest.GitRepository:
		fields = []string{obj.Reference.Branch, typedSpec["ref"].(map[string]any)["branch"].(string)}
	case *manifest.HelmRelease:
		ref := typedSpec["valuesFrom"].([]any)[0].(map[string]any)
		fields = []string{
			obj.ValuesFrom[0].Name,
			obj.ValuesFrom[0].ValuesKey,
			ref["name"].(string),
			ref["valuesKey"].(string),
		}
		assert.Equal(t, obj.ValuesFrom[0].Optional, true)
	default:
		t.Fatalf("unexpected child %T", obj)
	}
	for _, got := range fields {
		assert.Equal(t, got, (*p.testCaseInput).want)
	}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "source" && id.Kind == manifest.KindKustomization || ref.Use == "valuesFrom" {
			assert.Equal(t, ref.Name, (*p.testCaseInput).want)
		}
	}
}
