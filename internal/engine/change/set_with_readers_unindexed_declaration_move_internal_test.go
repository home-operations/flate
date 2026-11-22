package change

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSet_WithReadersUnindexedDeclarationMove(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"equal", "changed", "namespace", "partial", "unknown", "malformed", "duplicate_equal", "duplicate_conflict", "duplicate_reverse", "indexed_peer"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testSetWithReadersUnindexedDeclarationMoveCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func TestSet_WithReadersPreservesDeclarationOwners(t *testing.T) {
	t.Parallel()
	root := testutil.KustomizationID("apps", "root")
	driver := testutil.KustomizationID("apps", "driver")
	child := testutil.KustomizationID("apps", "child")
	foreign := testutil.KustomizationID("apps", "foreign")
	other := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "sources",
		Name:      "other",
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		root: {File: "entry/root.yaml", Path: "/tree/entry/root.yaml"},
		driver: {
			File:        "app/driver.yaml",
			Parent:      root,
			Productions: []manifest.Production{{Owner: root, Source: manifest.BootstrapSourceID}},
		},
		child: {
			Authored:    child,
			File:        "declarations/child.yaml",
			Parent:      driver,
			Productions: []manifest.Production{{Owner: driver, Source: manifest.BootstrapSourceID}},
		},
		foreign: {
			File:        "entry/root.yaml",
			Path:        "/other/entry/root.yaml",
			Productions: []manifest.Production{{Source: other}},
		},
	}, Readers: map[string][]manifest.FileReader{
		"leaf/kustomization.yaml": {{Reader: child, Source: manifest.BootstrapSourceID}},
	}}
	for _, before := range []bool{false, true} {
		t.Run(strconv.FormatBool(before), func(t *testing.T) {
			t.Parallel()
			set := &Set{paths: map[string][2]filePresence{"entry/root.yaml": {}, "leaf/kustomization.yaml": {}}}
			set.paths["leaf/kustomization.yaml"] = [2]filePresence{filePresent, fileUnknown}
			indexes := [2]*manifest.DiscoveryIndex{index, nil}
			if !before {
				indexes = [2]*manifest.DiscoveryIndex{nil, index}
				set.paths["leaf/kustomization.yaml"] = [2]filePresence{fileUnknown, filePresent}
			}
			resolved, err := set.WithReaders(
				t.Context(), [2]*manifest.DiscoveryIndex{
					indexes[0],
					indexes[1],
				},
				[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			want := InputChange{
				File:         "entry/root.yaml",
				Declarations: []string{"app/driver.yaml", "declarations/child.yaml", "entry/root.yaml"},
				Productions: []manifest.Production{
					{
						Owner:       driver,
						Declaration: driver,
						Authored:    child,
						Source:      manifest.BootstrapSourceID,
						File:        "declarations/child.yaml",
					},
					{
						Owner:       root,
						Declaration: root,
						Authored:    child,
						Source:      manifest.BootstrapSourceID,
						File:        "declarations/child.yaml",
					},
				},
			}
			assert.Diff(t, resolved.inputs[child], want)
			_, admitted := resolved.inputs[foreign]
			assert.Equal(t, admitted, false)
			assert.Equal(t, len(set.inputs), 0)
			repeated, err := set.WithReaders(
				t.Context(), [2]*manifest.DiscoveryIndex{
					indexes[0],
					indexes[1],
				},
				[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Diff(t, repeated.inputs, resolved.inputs)
		})
	}
	t.Run("prepared evidence preserves direct paths", func(t *testing.T) {
		t.Parallel()
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
		reader := testutil.HelmReleaseID("apps", "reader")
		stores := [2]*store.Store{store.New(), store.New()}
		artifacts := [2]*store.SourceArtifact{}
		indexes := [2]*manifest.DiscoveryIndex{}
		prepared := [2]map[manifest.NamedResource]store.Child{}
		for side, file := range []string{"old.yaml", "new.yaml"} {
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "settings", "namespace": "apps"},
				"data":       map[string]any{"value": "same"},
			}
			parseDocResult, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			object := parseDocResult.Object
			assert.NoError(t, err)
			artifacts[side] = &store.SourceArtifact{
				FS: fstest.MapFS{
					file: {
						Data: []byte(
							"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: same}\n",
						),
					},
				},
			}
			indexes[side] = &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					id: {
						File:        file,
						Productions: []manifest.Production{{Source: manifest.BootstrapSourceID, File: file}},
					},
				},
				Readers: map[string][]manifest.FileReader{
					file: {
						{Reader: reader, Source: manifest.BootstrapSourceID, Declaration: id, Exact: true},
						{Reader: root, Source: manifest.BootstrapSourceID, Exact: true},
					},
				},
				CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
			}
			stores[side].SetArtifact(manifest.BootstrapSourceID, artifacts[side])
			stores[side].SetDiscoveryIndex(indexes[side])
			prepared[side] = map[manifest.NamedResource]store.Child{id: {Obj: object, Doc: doc}}
		}
		set, err := Detect(t.Context(), artifacts[0], artifacts[1], DetectConfig{})
		assert.NoError(t, err)
		for side := range indexes {
			completeTestInventory(t, set, artifacts[side], indexes[side])
		}
		set, err = set.WithReaders(
			t.Context(), [2]*manifest.DiscoveryIndex{
				indexes[0],
				indexes[1],
			},
			[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
		)
		assert.NoError(t, err)
		_, direct := set.inputs[root]
		_, named := set.inputs[reader]
		assert.Equal(t, direct, true)
		assert.Equal(t, named, false)
		set, err = set.WithDeclarations(
			t.Context(),
			DeclarationInputs{Stores: [2]*store.Store{stores[0], stores[1]}, Prepared: prepared},
		)
		assert.NoError(t, err)
		set, err = set.WithReaders(
			t.Context(), [2]*manifest.DiscoveryIndex{
				indexes[0],
				indexes[1],
			},
			[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
		)
		assert.NoError(t, err)
		_, direct = set.inputs[root]
		_, named = set.inputs[reader]
		assert.Equal(t, direct, true)
		assert.Equal(t, named, false)
	})
}

func TestDetect_OneSidedInputIgnorePolicies(t *testing.T) {
	t.Parallel()
	for _, ignored := range []string{"before", "after", "both"} {
		t.Run(ignored, func(t *testing.T) {
			t.Parallel()
			testDetectOneSidedInputIgnorePoliciesCase(
				t,
				&ignored,
			)
		},
		)
	}
}

func TestDetect_OneSidedInputReadFailure(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"open", "read"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("evidence unavailable")
			input := &evidenceFailureFS{
				FS: fstest.MapFS{
					"input.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional}\n")},
				},
				operation: operation,
				cause:     cause,
			}
			set, err := Detect(
				t.Context(),
				&store.SourceArtifact{FS: input},
				&store.SourceArtifact{FS: fstest.MapFS{}},
				DetectConfig{},
			)

			assert.FatalIf(t, set != nil || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrInput) ||
				!errors.Is(err, manifest.ErrFlux) ||
				!strings.Contains(err.Error(), "input.yaml"), "read failure set=%v err=%v", set, err)

			if operation == "read" {
				assert.Equal(t, input.closed.Load(), int32(1))
			}
		})
	}
}

type evidenceFailureFS struct {
	fs.FS

	operation string
	cause     error
	read      func([]byte) (int, error)
	opened    atomic.Int32
	closed    atomic.Int32
}

func (f *evidenceFailureFS) Open(name string) (fs.File, error) {
	if name == "input.yaml" && f.operation == "open" {
		return nil, f.cause
	}
	file, err := f.FS.Open(name)
	if err != nil || name != "input.yaml" {
		return file, err
	}
	f.opened.Add(1)
	return &evidenceFailureFile{File: file, input: f}, nil
}

type evidenceFailureFile struct {
	fs.File

	input *evidenceFailureFS
}

func (f *evidenceFailureFile) Read(p []byte) (int, error) {
	if f.input.read != nil {
		return f.input.read(p)
	}
	return 0, f.input.cause
}

func (f *evidenceFailureFile) Close() error { f.input.closed.Add(1); return f.File.Close() }

func TestDetect_OneSidedInputCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &cancellingInput{
		FS: fstest.MapFS{
			"file.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional}\n")},
		},
		cancel:    cancel,
		cancelled: func() bool { return ctx.Err() != nil },
		dataEOF:   true,
	}
	set, err := Detect(
		ctx,
		&store.SourceArtifact{FS: input},
		&store.SourceArtifact{FS: fstest.MapFS{}},
		DetectConfig{},
	)

	assert.FatalIf(t, set != nil || !errors.Is(err, context.Canceled), "cancellation set=%v err=%v", set, err)

	assert.Equal(t, input.opened.Load(), int32(1))
	assert.Equal(t, input.closed.Load(), input.opened.Load())
	assert.Equal(t, input.postCancel.Load(), int32(0))
}

func TestDetect_ExistingInputHasNoOneSidedEvidence(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: apps}\n"
	before := fstest.MapFS{"input.yaml": {Data: []byte(body)}}
	after := fstest.MapFS{"input.yaml": {Data: []byte(body + "data: {VALUE: after}\n")}}
	set, err := Detect(
		t.Context(),
		&store.SourceArtifact{FS: before},
		&store.SourceArtifact{FS: after},
		DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), true)
	assert.Diff(t, set.inputs, map[manifest.NamedResource]InputChange(nil))
}

type digestReadFS struct {
	fs.FS

	opens, lists, stats atomic.Int64
}

func (f *digestReadFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

func (f *digestReadFS) Stat(name string) (fs.FileInfo, error) {
	f.stats.Add(1)
	return fs.Stat(f.FS, name)
}

func (f *digestReadFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.lists.Add(1)
	return fs.ReadDir(f.FS, name)
}

func TestDetect_SelectedFileEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"both", "before", "after", "neither", "empty", "nested", "nested_file", "mode", "reverse_mode", "normalized_mode", "ignore_exclude", "ignore_reinclude", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testDetectSelectedFileEvidenceCase(t, &mode)
		},
		)
	}
}

func TestDetect_SparseDirectoryBodies(t *testing.T) {
	t.Parallel()
	before, after := fstest.MapFS{}, fstest.MapFS{}
	for n := range 24 {
		name := fmt.Sprintf("disjoint/file-%02d.yaml", n)
		body := []byte(fmt.Sprintf("unchanged-%02d", n))
		before[name], after[name] = &fstest.MapFile{Data: body}, &fstest.MapFile{Data: body}
	}
	before["app/input.yaml"] = &fstest.MapFile{Data: []byte("before")}
	after["app/input.yaml"] = &fstest.MapFile{Data: []byte("after!")}
	left, right := &directoryBodyFS{FS: before}, &directoryBodyFS{FS: after}
	changes, err := Detect(
		t.Context(),
		&store.SourceArtifact{FS: left},
		&store.SourceArtifact{FS: right},
		DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"app/input.yaml"})
	assert.Equal(t, left.opens.Load(), int64(25))
	assert.Equal(t, right.opens.Load(), int64(25))
	t.Logf(
		"directory control: before/after regular body opens=%d/%d; metadata equality supplies no Git identity proof",
		left.opens.Load(),
		right.opens.Load(),
	)
}

type directoryBodyFS struct {
	fs.FS

	opens atomic.Int64
}

func (f *directoryBodyFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err == nil {
		info, statErr := file.Stat()
		if statErr == nil && info.Mode().IsRegular() {
			f.opens.Add(1)
		}
	}
	return file, err
}

func testSetWithReadersUnindexedDeclarationMoveCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	body := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart}\nspec: {url: oci://fixture.invalid/chart}\n"
	current := body
	if (*testCaseInput) == "changed" {
		current = strings.ReplaceAll(current, "fixture.invalid/chart", "fixture.invalid/other")
	}
	if (*testCaseInput) == "namespace" {
		current = strings.ReplaceAll(current, "name: chart", "name: chart, namespace: other")
	}
	inputs := [2]*store.SourceArtifact{
		{LocalPath: t.TempDir(), FS: fstest.MapFS{"outside/source.yaml": {Data: []byte(body)}}},
		{LocalPath: t.TempDir(), FS: fstest.MapFS{"entry/source.yaml": {Data: []byte(current)}}},
	}
	docs, err := manifest.DecodeDocs(strings.NewReader(current), manifest.IncidentalDocuments)
	assert.NoError(t, err)
	defer manifest.ReleaseDoc(docs[0].Document)
	authored := manifest.DocIdentity(docs[0].Document)
	id := manifest.Resolve(authored, "", "flux-system")
	reader := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Namespace: "apps",
		Name:      "reader",
	}
	indexes := [2]*manifest.DiscoveryIndex{
		{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
			CompleteSources: map[manifest.NamedResource]bool{
				manifest.BootstrapSourceID: (*testCaseInput) != "partial",
			},
		},
		{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Authored: authored, File: "entry/source.yaml"},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
			Readers: map[string][]manifest.FileReader{
				"entry/source.yaml": {
					{Reader: reader, Source: manifest.BootstrapSourceID, Declaration: id, Exact: true},
				},
			},
		},
	}
	configureUnindexedMove(*testCaseInput, inputs, indexes)
	changes, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{})
	assert.NoError(t, err)
	for _, reverse := range []bool{false, true} {
		before, after := indexes[0], indexes[1]
		if reverse {
			before, after = after, before
			changes, err = Detect(t.Context(), inputs[1], inputs[0], DetectConfig{})
			assert.NoError(t, err)
		}
		pair := inputs
		if reverse {
			pair = [2]*store.SourceArtifact{inputs[1], inputs[0]}
		}
		completeTestInventory(t, changes, pair[0], before)
		completeTestInventory(t, changes, pair[1], after)
		if *testCaseInput == "unknown" {
			delete(indexes[0].Declarations[manifest.BootstrapSourceID], "outside/source.yaml")
		}
		qualified, err := changes.WithReaders(
			t.Context(),
			[2]*manifest.DiscoveryIndex{before, after},
			[2]map[manifest.NamedResource]inputprojection.Projection{},
			manifest.WorkScope{},
		)
		assert.NoError(t, err)
		declarationChangedResult := qualified.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, id, nil)
		changed, known := declarationChangedResult.Changed, declarationChangedResult.Known
		uncertain := *testCaseInput == "partial" || *testCaseInput == "unknown" || *testCaseInput == "malformed"
		uncertain = uncertain || *testCaseInput == "duplicate_conflict" || *testCaseInput == "duplicate_reverse"
		equal := *testCaseInput == "equal" || *testCaseInput == "duplicate_equal" || *testCaseInput == "indexed_peer"
		assert.Equal(t, known, !uncertain)
		if known {
			assert.Equal(t, changed, !equal)
		}
		prepared := manifest.DeepCopyMap(docs[0].Document)
		prepared["metadata"].(map[string]any)["namespace"] = id.Namespace
		side := manifest.TreeHead
		if reverse {
			side = manifest.TreeBase
		}
		declarationChangedResult2 := qualified.DeclarationChanged(
			side,
			manifest.BootstrapSourceID,
			id,
			&store.DeclarationContent{
				Exact:            manifest.Fingerprint(manifest.WithoutOwnershipLabels(prepared)),
				WithoutNamespace: manifest.Fingerprint(manifest.WithoutOwnershipLabels(docs[0].Document)),
				Namespace:        id.Namespace,
			},
		)
		changed, known = declarationChangedResult2.Changed, declarationChangedResult2.Known
		if known {
			assert.Equal(t, changed, !equal)
		}
		_, selected := maps.Collect(qualified.Inputs(manifest.NamedResource{}))[reader]
		assert.Equal(t, selected, !equal)
	}
}

func testDetectOneSidedInputIgnorePoliciesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: apps}\n"
	before := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "before")}}
	after := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "after")}}
	if (*testCaseInput) != "after" {
		before[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
	}
	if (*testCaseInput) != "before" {
		after[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
	}
	set, err := Detect(
		t.Context(),
		admitTestSource(t, &store.SourceArtifact{FS: before}),
		admitTestSource(t, &store.SourceArtifact{FS: after}),
		DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), (*testCaseInput) != "both")
	if (*testCaseInput) != "both" {
		side := 1
		if (*testCaseInput) == "after" {
			side = 0
		}
		present, known := set.Present(manifest.TreeSide(side), "input.yaml")
		assert.Equal(t, present && known, true)
	}
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
}

func TestDeclarationChanged_PreparedEvidence(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("apps", "child")
	for _, tc := range []struct {
		name                     string
		content                  *store.DeclarationContent
		implicit, changed, known bool
	}{
		{name: "comparison only", known: true},
		{name: "empty evidence", content: &store.DeclarationContent{}},
		{name: "exact", content: &store.DeclarationContent{Exact: "authored"}, known: true},
		{name: "defaulted", content: &store.DeclarationContent{Exact: "prepared", WithoutNamespace: "authored", Namespace: "apps"}, implicit: true, known: true},
		{name: "explicit", content: &store.DeclarationContent{Exact: "prepared", WithoutNamespace: "authored", Namespace: "apps"}, changed: true, known: true},
		{name: "transformed", content: &store.DeclarationContent{Exact: "prepared", WithoutNamespace: "authored", Namespace: "other"}, implicit: true, changed: true, known: true},
		{name: "missing namespace evidence", content: &store.DeclarationContent{Exact: "prepared", Namespace: "apps"}, implicit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := &Set{declarations: map[comparisonKey]declarationComparison{
				{source: manifest.BootstrapSourceID, owner: id, input: id}: {
					declared: [2]string{
						"authored",
						"authored",
					},
					declaredKnown:     true,
					implicitNamespace: [2]bool{tc.implicit, tc.implicit},
				},
			}}
			for _, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
				assert.Diff(
					t,
					set.DeclarationChanged(side, manifest.BootstrapSourceID, id, tc.content),
					Comparison{Changed: tc.changed, Known: tc.known},
				)
			}
		})
	}
}

func configureUnindexedMove(scenario string, inputs [2]*store.SourceArtifact, indexes [2]*manifest.DiscoveryIndex) {
	body := string(inputs[0].FS.(fstest.MapFS)["outside/source.yaml"].Data)
	if scenario == "malformed" {
		inputs[0].FS.(fstest.MapFS)["outside/source.yaml"].Data = []byte(body + "---\nbroken: [")
	}
	if strings.HasPrefix(scenario, "duplicate_") {
		second := body
		if scenario != "duplicate_equal" {
			second = strings.ReplaceAll(body, "fixture.invalid/chart", "fixture.invalid/other")
		}
		if scenario == "duplicate_reverse" {
			body, second = second, body
		}
		inputs[0].FS.(fstest.MapFS)["outside/source.yaml"].Data = []byte(body + "---\n" + second)
	}
	if scenario == "indexed_peer" {
		for id := range indexes[1].Records {
			record := indexes[1].Records[id]
			indexes[0].Records[id] = manifest.DiscoveryRecord{Authored: record.Authored, File: "outside/source.yaml"}
		}
		inputs[0].FS.(fstest.MapFS)["outside/unrelated.yaml"] = &fstest.MapFile{
			Data: []byte(strings.ReplaceAll(body, "fixture.invalid/chart", "fixture.invalid/other")),
		}
	}
}

func TestSet_WithReadersInventoryOnlyMoves(t *testing.T) {
	t.Parallel()
	for _, kind := range []struct{ version, kind string }{{"apps/v1", "Deployment"}, {"example.io/v1", "Widget"}} {
		t.Run(kind.kind, func(t *testing.T) {
			t.Parallel()
			body := fmt.Sprintf(
				"apiVersion: %s\nkind: %s\nmetadata: {name: resource, namespace: apps}\nspec: {value: same}\n",
				kind.version,
				kind.kind,
			)
			group, _, _ := strings.Cut(kind.version, "/")
			id := manifest.NamedResource{Group: group, Kind: kind.kind, Namespace: "apps", Name: "resource"}
			reader := testutil.KustomizationID("apps", "reader")
			inputs := [2]*store.SourceArtifact{
				{FS: fstest.MapFS{"old.yaml": {Data: []byte(body)}}},
				{FS: fstest.MapFS{"new.yaml": {Data: []byte(body)}}},
			}
			indexes := [2]*manifest.DiscoveryIndex{}
			changes, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{})
			assert.NoError(t, err)
			for side, file := range []string{"old.yaml", "new.yaml"} {
				indexes[side] = &manifest.DiscoveryIndex{
					CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
					Readers: map[string][]manifest.FileReader{
						file: {{Reader: reader, Source: manifest.BootstrapSourceID, Exact: true}},
					},
				}
				completeTestInventory(t, changes, inputs[side], indexes[side])
			}
			resolved, err := changes.WithReaders(
				t.Context(),
				indexes,
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Diff(
				t,
				resolved.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, id, nil),
				Comparison{Known: true},
			)
			_, selected := resolved.inputs[reader]
			assert.Equal(t, selected, true)
			assert.Equal(t, len(indexes[0].Records)+len(indexes[1].Records), 0)
		})
	}
}
