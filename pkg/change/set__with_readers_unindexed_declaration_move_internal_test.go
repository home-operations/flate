package change

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSet_WithReadersUnindexedDeclarationMove(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"equal", "changed", "namespace", "partial"} {
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
			set := NewSet([]string{"entry/root.yaml", "leaf/kustomization.yaml"})
			set.oneSided[0] = map[string]map[manifest.NamedResource]string{"leaf/kustomization.yaml": nil}
			indexes := [2]*manifest.DiscoveryIndex{index, nil}
			if !before {
				indexes = [2]*manifest.DiscoveryIndex{nil, index}
				set.oneSided[1], set.oneSided[0] = set.oneSided[0], nil
			}
			resolved, err := set.WithReaders(
				t.Context(),
				indexes[0],
				indexes[1],
				[2]map[manifest.NamedResource]inputprojection.Projection{},
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
				t.Context(),
				indexes[0],
				indexes[1],
				[2]map[manifest.NamedResource]inputprojection.Projection{},
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
			object, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
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
		set, err := Detect(t.Context(), artifacts[0], artifacts[1])
		assert.NoError(t, err)
		set, err = set.WithReaders(
			t.Context(),
			indexes[0],
			indexes[1],
			[2]map[manifest.NamedResource]inputprojection.Projection{},
		)
		assert.NoError(t, err)
		_, direct := set.inputs[root]
		_, named := set.inputs[reader]
		assert.Equal(t, direct, true)
		assert.Equal(t, named, false)
		set, err = set.WithDeclarations(t.Context(), stores[0], stores[1], prepared)
		assert.NoError(t, err)
		set, err = set.WithReaders(
			t.Context(),
			indexes[0],
			indexes[1],
			[2]map[manifest.NamedResource]inputprojection.Projection{},
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
			set, err := Detect(t.Context(), &store.SourceArtifact{FS: input}, &store.SourceArtifact{FS: fstest.MapFS{}})
			{
				assert.FatalIf(t, set != nil || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrInput) ||
					!errors.Is(err, manifest.ErrFlux) ||
					!strings.Contains(err.Error(), "input.yaml"), "read failure set=%v err=%v", set, err)
			}
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
	set, err := Detect(ctx, &store.SourceArtifact{FS: input}, &store.SourceArtifact{FS: fstest.MapFS{}})
	{
		assert.FatalIf(t, set != nil || !errors.Is(err, context.Canceled), "cancellation set=%v err=%v", set, err)
	}
	assert.Equal(t, input.opened.Load(), int32(1))
	assert.Equal(t, input.closed.Load(), input.opened.Load())
	assert.Equal(t, input.postCancel.Load(), int32(0))
}

func TestDetect_ExistingInputHasNoOneSidedEvidence(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: apps}\n"
	before := fstest.MapFS{"input.yaml": {Data: []byte(body)}}
	after := fstest.MapFS{"input.yaml": {Data: []byte(body + "data: {VALUE: after}\n")}}
	set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), true)
	assert.Diff(t, set.inputs, map[manifest.NamedResource]InputChange(nil))
}

func TestCollectInputs_CancelBeforeOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	var set Set
	err := set.CollectInputs(ctx, input, "input.yaml", false)
	{
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancellation error=%v", err)
	}
	assert.Equal(t, input.opened.Load(), int32(0))
}

type inputCheckContext func() context.Context

func (c inputCheckContext) Deadline() (time.Time, bool) { return c().Deadline() }

func (c inputCheckContext) Done() <-chan struct{} { return c().Done() }

func (c inputCheckContext) Err() error { return c().Err() }

func (c inputCheckContext) Value(key any) any { return c().Value(key) }

func TestCollectInputs_CancelAfterOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := 0
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	input.read = func([]byte) (int, error) {
		reads++
		return 0, io.EOF
	}
	checked := inputCheckContext(func() context.Context {
		if input.opened.Load() != 0 {
			cancel()
		}
		return ctx
	})
	var set Set
	err := set.CollectInputs(checked, input, "input.yaml", false)
	{
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancellation error=%v", err)
	}
	assert.Equal(t, reads, 0)
	assert.Equal(t, input.opened.Load(), int32(1))
	assert.Equal(t, input.closed.Load(), int32(1))
	assert.Equal(t, len(set.inputs), 0)
}

func TestCollectInputs_ReadResults(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: apps}\n"
	cause := errors.New("input read failed")
	wrappedEOF := fmt.Errorf("unexpected EOF: %w", io.EOF)
	for _, tc := range []struct {
		name    string
		readErr error
		cancel  bool
		wantErr error
		reads   int
	}{
		{name: "bytes then EOF", reads: 2},
		{name: "bytes with EOF", readErr: io.EOF, reads: 1},
		{name: "bytes with error", readErr: cause, wantErr: cause, reads: 1},
		{name: "wrapped EOF", readErr: wrappedEOF, wantErr: wrappedEOF, reads: 1},
		{name: "cancel with bytes", cancel: true, wantErr: context.Canceled, reads: 1},
		{name: "cancel with EOF", readErr: io.EOF, cancel: true, wantErr: context.Canceled, reads: 1},
		{name: "cancel with error", readErr: cause, cancel: true, wantErr: context.Canceled, reads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCollectInputsReadResultsCase(t, testCollectInputsReadResultsCaseInputs{body: &body, tc: &tc})
		},

		// Final cancellation resolution must not mask a missing read-boundary check.

		)
	}
}

func TestCollectInputs_ParseFailureDrain(t *testing.T) {
	t.Parallel()
	const malformed = "\x00invalid\n"
	cause := errors.New("later read failed")
	probeReads := 0
	probe := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	probe.read = func(p []byte) (int, error) {
		probeReads++
		if probeReads == 1 {
			return copy(p, malformed), nil
		}
		return 0, cause
	}
	f, err := probe.Open("input.yaml")
	assert.NoError(t, err)
	_, err = testutil.DecodeDocs(f)
	assert.NoError(t, f.Close())
	{
		assert.FatalIf(
			t,
			err == nil || probeReads != 1,
			"malformed fixture must stop before the later read: reads=%d error=%v",
			probeReads,
			err,
		)
	}
	t.Logf("pinned decoder rejects malformed prefix after %d read; later read remains pending", probeReads)
	for _, tc := range []struct {
		name    string
		readErr error
		cancel  bool
		wantErr error
	}{
		{name: "malformed ending at EOF", readErr: io.EOF},
		{name: "later read error", readErr: cause, wantErr: cause},
		{name: "later cancellation", cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCollectInputsParseFailureDrainCase(t, &tc)
		},
		)
	}
}

type digestReadFS struct {
	fs.FS

	opens, lists atomic.Int64
}

func (f *digestReadFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

func (f *digestReadFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.lists.Add(1)
	return fs.ReadDir(f.FS, name)
}

func TestDetect_SelectedFileEvidence(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"both", "before", "after", "neither", "empty", "nested", "nested_file", "canceled"} {
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
	changes, err := Detect(t.Context(), &store.SourceArtifact{FS: left}, &store.SourceArtifact{FS: right})
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
	changes, err := Detect(t.Context(), inputs[0], inputs[1])
	assert.NoError(t, err)
	for _, reverse := range []bool{false, true} {
		before, after := indexes[0], indexes[1]
		if reverse {
			before, after = after, before
			changes, err = Detect(t.Context(), inputs[1], inputs[0])
			assert.NoError(t, err)
		}
		qualified, err := changes.WithReaders(
			t.Context(), before, after, [2]map[manifest.NamedResource]inputprojection.Projection{},
		)
		assert.NoError(t, err)
		changed, known := qualified.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, id, nil)
		assert.Equal(t, known, (*testCaseInput) != "partial")
		if known {
			assert.Equal(t, changed, (*testCaseInput) != "equal")
		}
		prepared := manifest.DeepCopyMap(docs[0].Document)
		manifest.EnsureMetadata(prepared)["namespace"] = id.Namespace
		side := manifest.TreeHead
		if reverse {
			side = manifest.TreeBase
		}
		changed, known = qualified.DeclarationChanged(side, manifest.BootstrapSourceID, id, prepared)
		if known {
			assert.Equal(t, changed, (*testCaseInput) != "equal")
		}
		_, selected := maps.Collect(qualified.Inputs(manifest.NamedResource{}))[reader]
		assert.Equal(t, selected, (*testCaseInput) != "equal")
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
	set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), (*testCaseInput) != "both")
	if (*testCaseInput) != "both" {
		side, name := 1, "after"
		if (*testCaseInput) == "after" {
			side, name = 0, "before"
		}
		assert.Diff(
			t,
			slices.SortedFunc(maps.Keys(set.oneSided[side]["input.yaml"]), manifest.NamedResource.Compare),
			[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}},
		)
	}
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
}

type testCollectInputsReadResultsCaseInputs struct {
	body *string
	tc   *struct {
		name string

		readErr error
		cancel  bool
		wantErr error
		reads   int
	}
}

func testCollectInputsReadResultsCase(t *testing.T, testCaseInput testCollectInputsReadResultsCaseInputs) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancellationChecks := 0
	checked := inputCheckContext(func() context.Context {
		if ctx.Err() != nil {
			cancellationChecks++
		}
		return ctx
	})
	reads := 0
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	input.read = func(p []byte) (int, error) {
		reads++
		if reads > 1 {
			return 0, io.EOF
		}
		if testCaseInput.tc.cancel {
			cancel()
		}
		return copy(p, (*testCaseInput.body)), testCaseInput.tc.readErr
	}
	var set Set
	err := set.CollectInputs(checked, input, "input.yaml", false)
	if testCaseInput.tc.wantErr == nil {
		assert.NoError(t, err)
		assert.Diff(
			t,
			slices.SortedFunc(maps.Keys(set.oneSided[1]["input.yaml"]), manifest.NamedResource.Compare),
			[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}},
		)
	} else {
		{
			assert.FatalIf(
				t,
				!errors.Is(err, testCaseInput.tc.wantErr) || len(set.inputs) != 0,
				"collected inputs=%v error=%v",
				set.inputs,
				err,
			)
		}
		{
			assert.FatalIf(t, !testCaseInput.tc.cancel &&
				(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")), "read failure classification=%v", err)
		}
	}
	assert.Equal(t, reads, testCaseInput.tc.reads)
	assert.Equal(t, input.closed.Load(), int32(1))
	if testCaseInput.tc.cancel {
		assert.Equal(t, cancellationChecks, 2)
	}
}

func testCollectInputsParseFailureDrainCase(t *testing.T, testCaseInput *struct {
	name string

	readErr error
	cancel  bool
	wantErr error
},
) {
	t.Helper()
	const malformed = "\x00invalid\n"

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := 0
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	input.read = func(p []byte) (int, error) {
		reads++
		if reads == 1 {
			return copy(p, malformed), nil
		}
		if testCaseInput.cancel {
			cancel()
		}
		return copy(p, "later content"), testCaseInput.readErr
	}
	var set Set
	err := set.CollectInputs(ctx, input, "input.yaml", false)
	if testCaseInput.wantErr == nil {
		assert.NoError(t, err)
	} else if !errors.Is(err, testCaseInput.wantErr) {
		t.Fatalf("drained error=%v, want %v", err, testCaseInput.wantErr)
	} else {
		assert.FatalIf(
			t,
			!testCaseInput.cancel &&
				(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")),
			"drained failure classification=%v",
			err,
		)
	}
	assert.Equal(t, len(set.inputs), 0)
	assert.Equal(t, reads, 2)
	assert.Equal(t, input.closed.Load(), int32(1))
}
