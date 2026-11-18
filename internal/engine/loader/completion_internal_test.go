package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/gittree"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func completionFixture(t *testing.T, input fs.FS) (*Loader, *change.Set) {
	t.Helper()
	root := t.TempDir()
	current := &store.SourceArtifact{LocalPath: root, FS: input}
	changes, err := change.Detect(t.Context(), &store.SourceArtifact{FS: fstest.MapFS{}}, current,
		change.DetectConfig{Candidates: map[string]bool{"input.yaml": false}})
	assert.NoError(t, err)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, current)
	l := New(st)
	l.SourceRoot = root
	return l, changes
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

func TestCompleteDeclarations_CancelBeforeOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	l, set := completionFixture(t, input)
	_, err := l.CompleteDeclarations(ctx, selection.DemandDeclarations(manifest.TreeHead, set, l.Index))

	assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancellation error=%v", err)

	assert.Equal(t, input.opened.Load(), int32(0))
}

type inputCheckContext func() context.Context

func (c inputCheckContext) Deadline() (time.Time, bool) { return c().Deadline() }

func (c inputCheckContext) Done() <-chan struct{} { return c().Done() }

func (c inputCheckContext) Err() error { return c().Err() }

func (c inputCheckContext) Value(key any) any { return c().Value(key) }

func TestCompleteDeclarations_CancelAfterOpen(t *testing.T) {
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
	l, set := completionFixture(t, input)
	l.documents.mountSources(l.Store, nil, l.Index)
	path := filepath.Join(l.SourceRoot, "input.yaml")
	previous := document{possible: true, sourceRoot: l.SourceRoot}
	l.documents.cache[path] = previous
	_, err := l.CompleteDeclarations(checked, selection.DemandDeclarations(manifest.TreeHead, set, l.Index))

	assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancellation error=%v", err)

	assert.Equal(t, reads, 0)
	assert.Equal(t, input.opened.Load(), int32(1))
	assert.Equal(t, input.closed.Load(), int32(1))
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
	assert.Equal(t, l.documents.cache[path].possible, true)
	assert.Equal(t, l.documents.cache[path].decodeErr, nil)
}

func TestCompleteDeclarations_ReadResults(t *testing.T) {
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
			testCompleteDeclarationsReadResultsCase(
				t,
				testCompleteDeclarationsReadResultsCaseInputs{body: &body, tc: &tc},
			)
		},

		// Final cancellation resolution must not mask a missing read-boundary check.

		)
	}
}

func TestCompleteDeclarations_ParseFailureDrain(t *testing.T) {
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

	assert.FatalIf(
		t,
		err == nil || probeReads != 1,
		"malformed fixture must stop before the later read: reads=%d error=%v",
		probeReads,
		err,
	)

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
			testCompleteDeclarationsParseFailureDrainCase(t, &tc)
		},
		)
	}
}

type testCompleteDeclarationsReadResultsCaseInputs struct {
	body *string
	tc   *struct {
		name string

		readErr error
		cancel  bool
		wantErr error
		reads   int
	}
}

func testCompleteDeclarationsReadResultsCase(
	t *testing.T,
	testCaseInput testCompleteDeclarationsReadResultsCaseInputs,
) {
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
	l, set := completionFixture(t, input)
	l.documents.mountSources(l.Store, nil, l.Index)
	path := filepath.Join(l.SourceRoot, "input.yaml")
	l.documents.cache[path] = document{possible: true, sourceRoot: l.SourceRoot}
	_, err := l.CompleteDeclarations(checked, selection.DemandDeclarations(manifest.TreeHead, set, l.Index))
	if testCaseInput.tc.wantErr == nil {
		assert.NoError(t, err)
		assert.Diff(
			t,
			slices.SortedFunc(
				maps.Keys(l.Index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects),
				manifest.NamedResource.Compare,
			),
			[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}},
		)
	} else {
		cached := l.documents.cache[path]
		assert.FatalIf(t, !cached.possible || cached.sourceRoot != l.SourceRoot ||
			cached.objects != nil || cached.declarations != nil || cached.decodeErr != nil,
			"failed completion replaced retained document: %+v", cached)
		assert.FatalIf(
			t,
			!errors.Is(err, testCaseInput.tc.wantErr) || len(maps.Collect(set.Inputs(manifest.NamedResource{}))) != 0,
			"collected inputs=%v error=%v",
			maps.Collect(set.Inputs(manifest.NamedResource{})),
			err,
		)

		assert.FatalIf(t, !testCaseInput.tc.cancel &&
			(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")), "read failure classification=%v", err)
	}
	assert.Equal(t, reads, testCaseInput.tc.reads)
	assert.Equal(t, input.closed.Load(), int32(1))
	if testCaseInput.tc.cancel {
		assert.Equal(t, cancellationChecks, 2)
	}
}

func testCompleteDeclarationsParseFailureDrainCase(t *testing.T, testCaseInput *struct {
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
	l, set := completionFixture(t, input)
	_, err := l.CompleteDeclarations(ctx, selection.DemandDeclarations(manifest.TreeHead, set, l.Index))
	switch {
	case testCaseInput.wantErr == nil:
		assert.NoError(t, err)
	case !errors.Is(err, testCaseInput.wantErr):
		t.Fatalf("drained error=%v, want %v", err, testCaseInput.wantErr)
	default:
		assert.FatalIf(
			t,
			!testCaseInput.cancel &&
				(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")),
			"drained failure classification=%v",
			err,
		)
	}
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
	assert.Equal(t, reads, 2)
	assert.Equal(t, input.closed.Load(), int32(1))
}

func TestCompleteDeclarations_FileEvidence(t *testing.T) {
	t.Parallel()
	cm := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: apps}\n"
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: apps}\n"
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}
	secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	for _, tc := range []struct {
		name, file, body string
		after            bool
		want             []manifest.NamedResource
	}{
		{"before", "input.yaml", cm, false, []manifest.NamedResource{cmID}},
		{"after", "input.yaml", cm, true, []manifest.NamedResource{cmID}},
		{"multiple documents", "input.YML", cm + "---\n- noise\n---\n" + secret, false, []manifest.NamedResource{cmID, secretID}},
		{"json", "input.JSON", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"optional","namespace":"apps"}}`, false, []manifest.NamedResource{cmID}},
		{"wrong suffix", "input.txt", cm, false, nil},
		{"foreign group", "input.yaml", strings.Replace(cm, "apiVersion: v1", "apiVersion: foreign.test/v1", 1), false, []manifest.NamedResource{{Group: "foreign.test", Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}}},
		{"noncanonical core", "input.yaml", strings.Replace(cm, "apiVersion: v1", "apiVersion: v2", 1), false, []manifest.NamedResource{cmID}},
		{"nondata", "input.yaml", strings.Replace(cm, "kind: ConfigMap", "kind: Pod", 1), false, []manifest.NamedResource{{Kind: "Pod", Namespace: "apps", Name: "optional"}}},
		{"empty name", "input.yaml", strings.Replace(cm, "name: optional", "name: ''", 1), false, nil},
		{"template name", "input.yaml", strings.Replace(cm, "name: optional", "name: '${NAME}'", 1), false, nil},
		{"template namespace", "input.yaml", strings.Replace(cm, "namespace: apps", "namespace: '${NAMESPACE}'", 1), false, nil},
		{"empty namespace exact", "input.yaml", strings.Replace(cm, "namespace: apps", "namespace: ''", 1), false, []manifest.NamedResource{{Kind: manifest.KindConfigMap, Name: "optional"}}},
		{"malformed", "input.yaml", cm + "---\nmalformed: [", false, nil},
		{"empty", "input.yaml", "", false, nil},
		{"incomplete", "input.yaml", "hello: world\n", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, after := fstest.MapFS{}, fstest.MapFS{}
			if tc.after {
				after[tc.file] = &fstest.MapFile{Data: []byte(tc.body)}
			} else {
				before[tc.file] = &fstest.MapFile{Data: []byte(tc.body)}
			}
			set, err := change.Detect(
				t.Context(),
				&store.SourceArtifact{LocalPath: "/before", FS: before},
				&store.SourceArtifact{LocalPath: "/after", FS: after},
				change.DetectConfig{},
			)
			assert.NoError(t, err)
			assert.Diff(t, set.Paths(), []string{tc.file})
			side := 0
			if tc.after {
				side = 1
			}
			_, artifacts, _ := set.ForSource(manifest.BootstrapSourceID)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, artifacts[side])
			l := New(st)
			l.SourceRoot = artifacts[side].LocalPath
			index, err := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeSide(side), set, l.Index),
			)
			assert.NoError(t, err)
			observation := index.Declarations[manifest.BootstrapSourceID][tc.file]
			var objects map[manifest.NamedResource]string
			if observation.Complete {
				objects = observation.Objects
			}
			assert.Diff(
				t,
				slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare),
				tc.want,
			)
			assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
		})
	}
}

func TestCompleteDeclarations_SnapshotAuthority(t *testing.T) {
	t.Parallel()
	body := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: app}\nspec: {replicas: 1}\n"
	input := &declarationReadFS{FS: fstest.MapFS{"input.yaml": {Data: []byte(body)}}}
	l, changes := completionFixture(t, input)
	id := manifest.NamedResource{Group: "apps", Kind: "Deployment", Name: "app"}
	production := manifest.Production{Source: manifest.BootstrapSourceID, File: "construction.yaml"}
	l.Index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
	before := l.Index
	objects := l.Store.Objects()
	index, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	assert.Equal(t, len(l.Store.Productions(id, nil)), 0)
	observation := index.Declarations[manifest.BootstrapSourceID]["input.yaml"]
	assert.Equal(t, observation.Present && observation.Complete, true)
	assert.Equal(t, observation.Objects[id] != "", true)
	assert.Equal(t, len(before.Declarations), 0)
	assert.Diff(t, l.Store.Objects(), objects)
	assert.Diff(t, index.Records, before.Records)
	assert.Diff(t, index.Claims, before.Claims)
	assert.Diff(t, index.Readers, before.Readers)
	assert.Diff(t, index.CompleteSources, before.CompleteSources)
	reads := input.opens.Load()
	repeated, err := l.CompleteDeclarations(
		t.Context(),
		selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
	)
	assert.NoError(t, err)
	assert.Equal(t, input.opens.Load(), reads)
	assert.Diff(t, repeated.Declarations, index.Declarations)
	assert.Equal(t, len(l.Store.Productions(id, nil)), 0)
	// Freeze MUST release construction scratch without discarding source observations.
	l.frozen = true
	l.scopes, l.facts, l.documents.parsed, l.Options.ParsedInputs = nil, nil, nil, nil
	_, err = l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	assert.Equal(t, input.opens.Load(), reads)
	_, artifacts, _ := changes.ForSource(manifest.BootstrapSourceID)
	replacement := &store.SourceArtifact{LocalPath: artifacts[1].LocalPath, FS: fstest.MapFS{"input.yaml": {
		Data: []byte(strings.ReplaceAll(body, "replicas: 1", "replicas: 2")),
	}}}
	l.Store.SetArtifact(manifest.BootstrapSourceID, replacement)
	stale, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	assert.Equal(t, len(stale.Declarations[manifest.BootstrapSourceID]), 0)
	changes, err = change.Detect(t.Context(), artifacts[0], replacement, change.DetectConfig{})
	assert.NoError(t, err)
	current, err := l.CompleteDeclarations(
		t.Context(),
		selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
	)
	assert.NoError(t, err)
	updated := current.Declarations[manifest.BootstrapSourceID]["input.yaml"]
	assert.Equal(t, updated.Complete, true)
	assert.Equal(t, updated.Objects[id] != observation.Objects[id], true)
	assert.Equal(t, index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects[id], observation.Objects[id])
	assert.Diff(t, index.Records, before.Records)
	published := *current
	published.Records = maps.Clone(current.Records)
	production.File = "frozen.yaml"
	published.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
	l.Store.SetDiscoveryIndex(&published)
	assert.Diff(t, l.Store.Productions(id, nil), []manifest.Production{production})
	assert.Equal(t, index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects[id], observation.Objects[id])
}

func TestCompleteDeclarations_FailedReplacement(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n"
	l, changes := completionFixture(t, fstest.MapFS{"input.yaml": {Data: []byte(body)}})
	index, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	path := filepath.Join(l.SourceRoot, "input.yaml")
	previous := l.documents.cache[path].declarations
	_, inputs, _ := changes.ForSource(manifest.BootstrapSourceID)
	cause := errors.New("replacement unavailable")
	replacement := &store.SourceArtifact{LocalPath: l.SourceRoot, FS: &evidenceFailureFS{
		FS: fstest.MapFS{"input.yaml": {}}, operation: "open", cause: cause,
	}}
	updated, err := change.Detect(t.Context(), inputs[0], replacement,
		change.DetectConfig{Candidates: map[string]bool{"input.yaml": false}})
	assert.NoError(t, err)
	l.Store.SetArtifact(manifest.BootstrapSourceID, replacement)
	_, err = l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, updated, l.Index))
	assert.FatalIf(t, !errors.Is(err, cause), "replacement failure=%v", err)
	assert.Equal(t, l.Index, index)
	assert.Equal(t, l.documents.cache[path].declarations, previous)
	assert.Equal(
		t,
		inputs[1].SameMaterial(
			l.documents.artifacts[l.SourceRoot],
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		),
		true,
	)
	l.Store.SetArtifact(manifest.BootstrapSourceID, inputs[1])
	restored, err := l.CompleteDeclarations(
		t.Context(),
		selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
	)
	assert.NoError(t, err)
	assert.Diff(t, restored.Declarations, index.Declarations)
}

func TestCompleteDeclarations_UnknownAndAbsent(t *testing.T) {
	t.Parallel()
	input := &declarationReadFS{FS: fstest.MapFS{}}
	l, _ := completionFixture(t, input)
	peer := store.New()
	peer.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	changes, err := change.Qualify(
		t.Context(),
		detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"}),
		[2]*store.Store{peer, l.Store},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name    string
		changes *change.Set
		known   bool
	}{
		{name: "requested", changes: detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"})},
		{name: "admitted", changes: changes, known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current := New(l.Store)
			current.SourceRoot = l.SourceRoot
			index, err := current.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, tc.changes, current.Index),
			)
			assert.NoError(t, err)
			_, known := index.Declarations[manifest.BootstrapSourceID]["input.yaml"]
			assert.Equal(t, known, tc.known)
			if known {
				observation := index.Declarations[manifest.BootstrapSourceID]["input.yaml"]
				assert.Equal(t, observation.Complete && !observation.Present, true)
			}
		})
	}
}

func TestCompleteDeclarations_InputOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		routing []string
		want    []string
		readErr error
	}{
		{name: "duplicates", routing: []string{".sourceignore", "input.yaml", "z.yaml"}, want: []string{"input.yaml", "z.yaml"}, readErr: io.EOF},
		{name: "first_error", routing: []string{".sourceignore", "z.yaml"}, want: []string{"input.yaml"}, readErr: fs.ErrPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {Data: []byte("*.yaml\n")}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			after, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			var reads []string
			input := &declarationFailureFS{FS: after, fail: func(name string) error {
				reads = append(reads, name)
				return tc.readErr
			}}
			current, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: input},
			)
			assert.NoError(t, err)
			candidates := make(map[string]bool, len(tc.routing))
			for _, name := range tc.routing {
				candidates[name] = false
			}
			changes, err := change.Detect(
				t.Context(),
				&store.SourceArtifact{LocalPath: "/before", FS: before},
				&store.SourceArtifact{LocalPath: "/after", FS: current},
				change.DetectConfig{Candidates: candidates},
			)
			assert.NoError(t, err)
			_, artifacts, _ := changes.ForSource(manifest.BootstrapSourceID)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, artifacts[1])
			l := New(st)
			l.SourceRoot = "/after"
			index, err := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.Diff(t, reads, tc.want)
			if errors.Is(tc.readErr, io.EOF) {
				assert.NoError(t, err)
				assert.Diff(t, changes.Paths(), []string{".sourceignore", "input.yaml", "z.yaml"})
			} else {
				assert.FatalIf(
					t,
					!errors.Is(err, tc.readErr) || index != nil ||
						!strings.Contains(err.Error(), "input.yaml:"),
					"first input failure: changes=%v err=%v",
					changes,
					err,
				)
			}
		})
	}
}

type declarationFailureFS struct {
	*gittree.FS

	fail func(string) error
}

func (f *declarationFailureFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || !strings.HasSuffix(name, ".yaml") {
		return file, err
	}
	return &declarationFailureFile{File: file, fail: func() error { return f.fail(name) }}, nil
}

type declarationFailureFile struct {
	fs.File

	fail func() error
}

func (f *declarationFailureFile) Read([]byte) (int, error) { return 0, f.fail() }

func TestCompleteDeclarations_IgnorePolicies(t *testing.T) {
	t.Parallel()
	for _, ignored := range []string{"before", "after", "both"} {
		t.Run(ignored, func(t *testing.T) { t.Parallel(); testCompleteDeclarationsIgnorePoliciesCase(t, &ignored) })
	}
}

func testCompleteDeclarationsIgnorePoliciesCase(
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
	set, err := change.Detect(
		t.Context(),
		&store.SourceArtifact{LocalPath: "/before", FS: before},
		&store.SourceArtifact{LocalPath: "/after", FS: after},
		change.DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), (*testCaseInput) != "both")
	if (*testCaseInput) != "both" {
		side, name := 1, "after"
		if (*testCaseInput) == "after" {
			side, name = 0, "before"
		}
		_, artifacts, _ := set.ForSource(manifest.BootstrapSourceID)
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, artifacts[side])
		l := New(st)
		l.SourceRoot = artifacts[side].LocalPath
		index, err := l.CompleteDeclarations(
			t.Context(),
			selection.DemandDeclarations(manifest.TreeSide(side), set, l.Index),
		)
		assert.NoError(t, err)
		assert.Diff(
			t,
			slices.SortedFunc(
				maps.Keys(index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects),
				manifest.NamedResource.Compare,
			),
			[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}},
		)
	}
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
}

func TestCompleteDeclarations_Coverage(t *testing.T) {
	t.Parallel()
	for _, limited := range []string{"application", "coordinate"} {
		t.Run(limited, func(t *testing.T) {
			t.Parallel()
			body := testutil.KustomizationYAML(
				"app",
				"apps",
				"  path: ./work\n",
			) + "---\napiVersion: apps/v1\nkind: Deployment\nmetadata: {name: work}\n"
			input := &declarationReadFS{FS: fstest.MapFS{"input.yaml": {Data: []byte(body)}}}
			l, changes := completionFixture(t, input)
			l.documents.mountSources(l.Store, nil, l.Index)
			reader := l.documents
			if limited == "application" {
				reader.intent = manifest.ApplicationDocuments
			} else {
				reader.coordinate = &occurrenceID{}
			}
			_, err := reader.fileObjects(
				t.Context(),
				filepath.Join(l.SourceRoot, "input.yaml"),
				manifest.ParseDocOptions{},
			)
			assert.NoError(t, err)
			assert.Equal(t, reader.cache[filepath.Join(l.SourceRoot, "input.yaml")].declarations.Complete, false)
			input.opens.Store(0)
			completed, err := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.NoError(t, err)
			observation := completed.Declarations[manifest.BootstrapSourceID]["input.yaml"]
			assert.Equal(t, observation.Complete, true)
			assert.Equal(t, len(observation.Objects), 2)
			assert.Equal(t, input.opens.Load(), int64(1))
			_, err = l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.NoError(t, err)
			assert.Equal(t, input.opens.Load(), int64(1))
		})
	}
}

//nolint:paralleltest // This witness inspects a map after returning it to the process-global decode pool.
func TestCompleteDeclarations_PreparesRetainedDocuments(t *testing.T) {
	for _, mode := range []string{"file", "census", "close"} {
		t.Run(mode, func(t *testing.T) {
			input := &declarationReadFS{
				FS: fstest.MapFS{
					"input.yaml": {Data: []byte(testutil.KustomizationYAML("app", "apps", "  path: ./work\n"))},
				},
			}
			l, changes := completionFixture(t, input)
			input.opens.Store(0)
			l.documents.compact, l.documents.eager = mode == "census", true
			_, err := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.NoError(t, err)
			file := filepath.Join(l.SourceRoot, "input.yaml")
			doc := l.documents.cache[file]
			assert.Equal(t, doc.unprepared, true)
			doc.snapshot()
			prior := doc.parsed
			saved, known, err := cacheInput("input.yaml", "digest", &doc)
			assert.NoError(t, err)
			assert.Equal(t, known && !saved.Parsed, true)
			restored := readerForTest(l.SourceRoot)
			restored.cache[file] = document{deferred: &saved}
			restored.restoreCachedFile(file)
			observation := restored.cache[file]
			assert.NoError(t, observation.restoreDeclarations())
			restored.cache[file] = observation
			assert.Diff(t, restored.cache[file].declarations.FileDeclarations, doc.declarations.FileDeclarations)
			if mode == "close" {
				id := manifest.DocIdentity(doc.objects[0].Doc)
				fingerprint := l.Index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects[id]
				l.Close()
				assert.Equal(t, len(doc.objects[0].Doc), 0)
				assert.Equal(t, manifest.DocIdentity(prior.objects[0].Doc).Name, "app")
				assert.Equal(t, l.Index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects[id], fingerprint)
				return
			}
			var objects []parsedObject
			if mode == "census" {
				objects, err = l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
			} else {
				objects, err = l.documents.fileObjects(t.Context(), file, manifest.ParseDocOptions{})
			}
			assert.NoError(t, err)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, l.DeclarationContents()(objects[0].Doc).Exact != "", true)
			assert.Equal(t, l.documents.cache[file].unprepared, false)
			assert.Equal(t, prior.unprepared, true)
			assert.Equal(t, prior.objects[0].Obj, nil)
			assert.Equal(t, manifest.DocIdentity(prior.objects[0].Doc).Name, "app")
			_, err = l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeHead, changes, l.Index),
			)
			assert.NoError(t, err)
			assert.Equal(t, input.opens.Load(), int64(1))
			l.Close()
			assert.Equal(t, manifest.DocIdentity(objects[0].Doc).Name, "app")
		})
	}
}

func TestCompleteDeclarations_ReleasesFailedDocuments(t *testing.T) {
	t.Parallel()
	cause := errors.New("document read failed")
	reads := 0
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}, read: func(p []byte) (int, error) {
		reads++
		if reads == 1 {
			return copy(p, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: partial}\n"), cause
		}
		return 0, io.EOF
	}}
	l, _ := completionFixture(t, input)
	l.documents.mountSources(l.Store, nil, l.Index)
	file := filepath.Join(l.SourceRoot, "input.yaml")
	_, err := l.documents.completeFile(t.Context(), file)
	assert.Equal(t, errors.Is(err, cause), true)
	objects := l.documents.cache[file].objects
	assert.Equal(t, len(objects) > 0, true)
	for i := range objects {
		assert.Equal(t, len(objects[i].Doc), 0)
		assert.Equal(t, len(objects[i].outcome.Document), 0)
	}
	assert.Equal(t, input.closed.Load(), int32(1))
}
