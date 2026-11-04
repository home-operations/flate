package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRun_EntryApplication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, owner, kind, url, path                     string
		instance, admitted, pinned, qualified, dependent bool
	}{
		{name: "gotk-sync", owner: "gotk-sync", qualified: true},
		{name: "arbitrary applier", owner: "applies-here", qualified: true},
		{name: "different path", owner: "elsewhere", path: "./elsewhere"},
		{name: "escaping path", owner: "escape", path: "../elsewhere"},
		{name: "dependent applier", owner: "applies-here", dependent: true, qualified: true},
		{name: "wrong source", owner: "foreign", kind: manifest.KindGitRepository, url: "https://fixture.invalid/foreign"},
		{name: "pinned source", owner: "pinned", kind: manifest.KindGitRepository, url: "https://fixture.invalid/self", admitted: true, pinned: true},
		{name: "admitted oci", owner: "oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", admitted: true, qualified: true},
		{name: "unadmitted oci", owner: "oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self"},
		{name: "admitted bucket", owner: "bucket", kind: manifest.KindBucket, url: "s3.fixture.invalid", admitted: true, qualified: true},
		{name: "unadmitted bucket", owner: "bucket", kind: manifest.KindBucket, url: "s3.fixture.invalid"},
		{name: "instance git", owner: "flux", kind: manifest.KindGitRepository, url: "https://fixture.invalid/self", instance: true, admitted: true, qualified: true},
		{name: "instance foreign git", owner: "flux", kind: manifest.KindGitRepository, url: "https://fixture.invalid/foreign", instance: true},
		{name: "instance oci", owner: "flux", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", instance: true, admitted: true, qualified: true},
		{name: "instance unadmitted oci", owner: "flux", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", instance: true},
		{name: "instance bucket", owner: "flux", kind: manifest.KindBucket, url: "s3.fixture.invalid", instance: true, admitted: true, qualified: true},
		{name: "instance unadmitted bucket", owner: "flux", kind: manifest.KindBucket, url: "s3.fixture.invalid", instance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunEntryApplicationCase(t, &tc)
		},
		)
	}
}

func TestBindEntry_EvidenceAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		publication  bool
		livePath     string
		history      bool
		historyFirst bool
		dependent    bool
		substitution bool
		invalid      string
		qualified    bool
		reuse        string
	}{
		{name: "retired_publication", publication: true, livePath: "./elsewhere"},
		{name: "withdrawn_publication", publication: true},
		{name: "effective_publication", publication: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "same_content_replacement", publication: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "receipt_only", history: true, qualified: true, reuse: "history"},
		{name: "receipt_unrelated_live", history: true, livePath: "./elsewhere", qualified: true},
		{name: "current_then_receipt", history: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "receipt_then_current", history: true, historyFirst: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "dependent_receipt", history: true, dependent: true, qualified: true},
		{name: "substituted_receipt", history: true, substitution: true, qualified: true},
		{name: "malformed_history", history: true, invalid: "malformed"},
		{name: "wrong_id_history", history: true, invalid: "wrong_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testBindEntryEvidenceAuthorityCase(t, &tc)
		},
		)
	}
}

func TestBindEntry_ReemittedPublication(t *testing.T) {
	t.Parallel()
	const repo = "/virtual/reemitted-publication"
	input := &store.SourceArtifact{LocalPath: repo, FS: fstest.MapFS{}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "./generator"}
	st.AddObject(producer)
	a := &Admission{input: input, path: filepath.Join(repo, "entry")}
	applier := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: "./entry"}
	sibling := &manifest.ConfigMap{Name: "sibling", Namespace: "apps", Data: map[string]any{"value": "old"}}
	var publications int
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if obj, ok := payload.(manifest.BaseManifest); ok {
			a.AddApplication(st, obj)
		}
		if id == applier.Named() {
			publications++
		}
	}, false)
	defer unsubscribe()
	st.RecordEmitted(
		producer.Named(),
		&store.Emission{Children: []store.Child{{Obj: applier}, {Obj: sibling}}, Complete: true},
		nil,
	)
	replacement := applier.Clone()
	st.RecordEmitted(producer.Named(), &store.Emission{
		Children: []store.Child{{Obj: replacement}, {Obj: &manifest.ConfigMap{
			Name: sibling.Name, Namespace: sibling.Namespace, Data: map[string]any{"value": "new"},
		}}}, Complete: true,
	}, nil)
	assert.Equal(t, publications, 1)
	assert.Equal(t, st.GetObject(applier.Named()), manifest.BaseManifest(replacement))
	appliers, err := a.qualifyEntry(t.Context(), st, nil)
	assert.NoError(t, err)
	assert.FatalIf(t, len(appliers) != 1, "qualifying publications: got %d, want 1", len(appliers))
	assert.Equal(t, appliers[0], manifest.BaseManifest(applier))
	root, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, root, replacement)
	assert.Equal(t, binding.Producer, replacement.Named())
}

func TestRun_EntryPackage(t *testing.T) {
	t.Parallel()
	for _, filename := range manifest.KustomizeBuilderFilenames {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-package"
			input := fstest.MapFS{
				filename:  {Data: []byte("resources: [cm.yaml]\n")},
				"cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")},
			}
			st := store.New()
			observed := &entryGeneratorFS{FS: input}
			result, err := Run(
				t.Context(),
				Config{
					Input: &store.SourceArtifact{LocalPath: root, FS: observed},
					Path:  root,
					Store: st,
					Trees: testTrees(t),
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, result.Binding.Producer.ReportHidden(), true)
			assert.FatalIf(t, observed.calls.Load() == 0, "%v", "positive control did not observe the root generator")
			record, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "literal"})
			assert.Equal(t, ok, true)
			assert.Equal(t, record.Parent, result.Binding.Producer)
		})
	}
}

func TestBindEntry_PackageReadFailure(t *testing.T) {
	t.Parallel()
	const root = "/virtual/unreadable-entry"
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, FS: unreadableEntryFS{fstest.MapFS{}}},
	)
	d := newDiscoverer(&Config{Path: root, Store: st})
	_, err := d.bindEntry(t.Context(), root)
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || !errors.Is(err, fs.ErrPermission),
		"entry package failure lost its classification or cause: %v",
		err,
	)
	assert.FatalIf(
		t,
		strings.Contains(err.Error(), "input error") || strings.Contains(err.Error(), "flux error"),
		"entry package diagnostic contains classification prose: %v",
		err,
	)
	assert.Equal(t, d.binding, Binding{})
}

type unreadableEntryFS struct{ fs.FS }

func (f unreadableEntryFS) Open(name string) (fs.File, error) {
	if name == "kustomization.yaml" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return f.FS.Open(name)
}

type entryGeneratorFS struct {
	fs.FS

	calls atomic.Int64
}

func (f *entryGeneratorFS) Open(name string) (fs.File, error) {
	f.observe()
	return f.FS.Open(name)
}

func (f *entryGeneratorFS) Stat(name string) (fs.FileInfo, error) {
	f.observe()
	return fs.Stat(f.FS, name)
}

func (f *entryGeneratorFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.observe()
	return fs.ReadDir(f.FS, name)
}

func (f *entryGeneratorFS) observe() {
	var stack [64]uintptr
	frames := runtime.CallersFrames(stack[:runtime.Callers(2, stack[:])])
	for {
		frame, more := frames.Next()
		if frame.Function == "github.com/home-operations/flate/pkg/kustomize.generateManifest" {
			f.calls.Add(1)
			return
		}
		if !more {
			return
		}
	}
}

func TestAdvance_PlaceholderInputReachesFixpoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "namespace: apps\nresources: [consumer.yaml, source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"",
			"  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n  postBuild:\n    substituteFrom: [{kind: Secret, name: settings}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster}\nspec: {url: 'git://fixture.invalid/repo'}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [settings.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/settings.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: settings}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
	)
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"flux/consumer.yaml"})
	session, err := Begin(
		t.Context(),
		Config{
			Path:        filepath.Join(root, "flux"),
			RepoRoot:    root,
			SelfURLs:    []string{"git://fixture.invalid/repo"},
			Store:       st,
			Loader:      l,
			WipeSecrets: true,
			Trees:       kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)),
			Selection:   manifest.Selection{Changed: true},
		},
	)
	assert.NoError(t, err)
	for range 3 {
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		advanced, err := session.Advance(t.Context(), proposal)
		assert.NoError(t, err)
		if !advanced {
			return
		}
	}
	t.Fatal("construction repeatedly readmitted the placeholder input without reaching a fixpoint")
}

type constructionFS struct {
	fs.FS

	statErr error
}

func (f *constructionFS) Stat(name string) (fs.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return fs.Stat(f.FS, name)
}

func TestAdvance_LeafAdmissionPreservesConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &constructionFS{
		FS: fstest.MapFS{
			"flux/owner.yaml":        {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"))},
			"app/kustomization.yaml": {Data: []byte("resources: []\n")},
		},
	}
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	session, err := Begin(
		t.Context(),
		Config{
			Input:     &store.SourceArtifact{LocalPath: root, FS: input},
			Path:      filepath.Join(root, "flux"),
			Store:     st,
			Loader:    l,
			Selection: manifest.Selection{Changed: true},
		},
	)
	assert.NoError(t, err)
	input.statErr = fs.ErrPermission
	leaf := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "stable"}}
	advanced, err := session.Advance(t.Context(), &loader.Admission{Objects: []store.Child{{Obj: leaf}}})
	assert.NoError(t, err)
	assert.Equal(t, advanced, true)
	assert.Equal(t, st.GetObject(leaf.Named()), manifest.BaseManifest(leaf))
}

func testRunEntryApplicationCase(t *testing.T, testCaseInput *struct {
	name string

	owner     string
	kind      string
	url       string
	path      string
	instance  bool
	admitted  bool
	pinned    bool
	qualified bool
	dependent bool
},
) {
	t.Helper()

	const root = "/virtual/entry-application"
	input := fstest.MapFS{
		"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")},
	}
	st := store.New()
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      testCaseInput.kind,
		Name:      "self",
		Namespace: "apps",
	}
	var declaration string
	if testCaseInput.instance {
		input["owner.yaml"] = &fstest.MapFile{
			Data: []byte(
				fmt.Sprintf(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: %s, url: '%s', ref: main, path: ./}\n",
					testCaseInput.kind,
					testCaseInput.url,
				),
			),
		}
	} else {
		extra := ""
		if testCaseInput.dependent {
			extra = "  dependsOn: [{name: sibling}]\n"
			input["sibling.yaml"] = &fstest.MapFile{
				Data: []byte(testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n")),
			}
			input["leaf/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
		}
		if testCaseInput.kind != "" {
			extra = "  sourceRef: {kind: " + testCaseInput.kind + ", name: self}\n"
			spec := "url: '" + testCaseInput.url + "'"
			if testCaseInput.kind == manifest.KindBucket {
				spec = "endpoint: '" + testCaseInput.url + "', bucketName: cluster"
			}
			if testCaseInput.pinned {
				spec += ", ref: {tag: pinned}"
			}
			declaration = fmt.Sprintf(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: self, namespace: apps}\nspec: {%s}\n",
				testCaseInput.kind,
				spec,
			)
			input["source.yaml"] = &fstest.MapFile{Data: []byte(declaration)}
		}
		path := testCaseInput.path
		if path == "" {
			path = "./"
		}
		input["owner.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML(testCaseInput.owner, "apps", "  path: "+path+"\n"+extra)),
		}
	}
	if testCaseInput.admitted {
		artifact := &store.SourceArtifact{
			LocalPath:  "/virtual/application-alias",
			LocalRoot:  root,
			InputAlias: true,
			FS:         input,
		}
		if testCaseInput.pinned {
			artifact.InputAlias, artifact.Content, artifact.Key = false, "committed", "committed"
		}
		st.SetArtifact(source, artifact)
	}
	d := newDiscoverer(
		&Config{
			Input:    &store.SourceArtifact{LocalPath: root, FS: input},
			Path:     root,
			Store:    st,
			Trees:    testTrees(t),
			SelfURLs: []string{"https://fixture.invalid/self"},
		},
	)
	_, err := d.resolveEntrySources(t.Context(), false)
	assert.NoError(t, err)
	assert.NoError(t, d.loadManifests(t.Context(), root, true))
	assert.Equal(t, d.binding != (Binding{}), testCaseInput.qualified)
	if testCaseInput.qualified {
		assert.Equal(t, d.entryApplier.Name, testCaseInput.owner)
		if testCaseInput.dependent {
			assert.Equal(t, d.binding.Producer.ReportHidden(), true)
		}
		if testCaseInput.instance {
			assert.Equal(t, d.entryApplier.Kind, "FluxInstance")
		}
	} else {
		assert.Equal(t, len(d.prepared), 0)
	}
	if testCaseInput.instance {
		assert.Equal(t, st.GetObject(source), manifest.BaseManifest(nil))
		instance := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      "FluxInstance",
			Namespace: "apps",
			Name:      "flux",
		}
		assert.Equal(t, st.GetObject(instance), manifest.BaseManifest(nil))
		_, indexed := d.loader.Index.Lookup(instance)
		assert.Equal(t, indexed, true)
	}
}

func testBindEntryEvidenceAuthorityCase(t *testing.T, testCaseInput *struct {
	name string

	publication  bool
	livePath     string
	history      bool
	historyFirst bool
	dependent    bool
	substitution bool
	invalid      string
	qualified    bool
	reuse        string
},
) {
	t.Helper()

	const repo = "/virtual/evidence-authority"
	input := &store.SourceArtifact{LocalPath: repo, FS: fstest.MapFS{
		"entry/literal.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	a := &Admission{input: input, path: filepath.Join(repo, "entry")}
	current := &manifest.Kustomization{
		Name:            "applier",
		Namespace:       "apps",
		Path:            testCaseInput.livePath,
		TargetNamespace: "current",
	}
	if testCaseInput.publication {
		previous := current.Clone()
		previous.Path = "./entry"
		st.AddObject(previous)
		a.AddApplication(st, previous)
		if testCaseInput.name == "same_content_replacement" || testCaseInput.livePath == "" {
			st.DeleteObject(previous.Named())
		}
	}
	spec := map[string]any{"path": "./entry", "targetNamespace": "history"}
	if testCaseInput.dependent {
		spec["dependsOn"] = []any{map[string]any{"name": "sibling"}}
	}
	if testCaseInput.substitution {
		spec["postBuild"] = map[string]any{
			"substituteFrom": []any{map[string]any{"kind": "ConfigMap", "name": "settings"}},
		}
	}
	document := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization",
		"metadata": map[string]any{"name": current.Name, "namespace": current.Namespace}, "spec": spec,
	}
	data, err := json.Marshal(document)
	assert.NoError(t, err)
	record := manifest.DiscoveryRecord{Application: string(data)}
	id := current.Named()
	switch testCaseInput.invalid {
	case "malformed":
		record.Application = "{"
	case "wrong_id":
		id.Name = "different"
	}
	addHistory := func() {
		if testCaseInput.history {
			a.AddHistory(st, id, record)
		}
	}
	if testCaseInput.historyFirst {
		addHistory()
	}
	if testCaseInput.livePath != "" {
		st.AddObject(current)
		current = st.GetObject(current.Named()).(*manifest.Kustomization)
		a.AddApplication(st, current)
	}
	if !testCaseInput.historyFirst {
		addHistory()
	}
	root, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	if !testCaseInput.qualified {
		assert.Equal(t, root, (*manifest.Kustomization)(nil))
		assert.Equal(t, binding, Binding{})
		return
	}
	assert.FatalIf(t, root == nil, "%v", "qualifying evidence lost its entry")
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", current.Named()).Named()
	assert.Equal(t, binding.Key, key)
	assert.Equal(t, binding.Path, "entry")
	assert.Equal(t, root.Named().ReportHidden(), testCaseInput.reuse == "")
	switch testCaseInput.reuse {
	case "current":
		assert.Equal(t, root, current)
	case "history":
		assert.Equal(t, root.TargetNamespace, "history")
	}
	assert.Equal(t, a.entryBound.Load(), true)
	assert.Equal(t, a.applications.Load(), (*applicationEvidence)(nil))
}
