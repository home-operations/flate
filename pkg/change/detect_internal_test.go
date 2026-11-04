package change

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_BothPathsRequired(t *testing.T) {
	t.Parallel()
	{
		{
			_, err := Detect(t.Context(), testInput(""), testInput("/tmp/x"))
			assert.ErrorIf(t, err == nil, "expected error for empty before")
		}
	}
	{
		{
			_, err := Detect(t.Context(), testInput("/tmp/x"), testInput(""))
			assert.ErrorIf(t, err == nil, "expected error for empty after")
		}
	}
}

func TestDetect_IdenticalTreesEmpty(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "a.yaml", "x")
	testutil.WriteFile(t, after, "a.yaml", "x")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
	{
		assert.FatalIf(t, err != nil, "Detect: %v", err)
	}
	if got.Len() != 0 {
		t.Errorf("expected 0 changes, got %v", got.Paths())
	}
}

func TestDetect_AddedRemovedModified(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	// unchanged: ident.yaml on both sides with same content
	testutil.WriteFile(t, before, "ident.yaml", "same")
	testutil.WriteFile(t, after, "ident.yaml", "same")
	// added: only on after side
	testutil.WriteFile(t, after, "added.yaml", "new")
	// removed: only on before side
	testutil.WriteFile(t, before, "removed.yaml", "gone")
	// modified-same-size: trigger the hash path with same byte length
	testutil.WriteFile(t, before, "mod.yaml", "AAA")
	testutil.WriteFile(t, after, "mod.yaml", "BBB")
	// Bump mtime so the mtime equality bypass doesn't short-circuit.
	{
		{
			err := os.Chtimes(filepath.Join(after, "mod.yaml"), time.Now(), time.Now().Add(time.Hour))
			assert.FatalIf(t, err != nil, "chtimes: %v", err)
		}
	}

	got, err := Detect(t.Context(), testInput(before), testInput(after))
	{
		assert.FatalIf(t, err != nil, "Detect: %v", err)
	}
	want := []string{"added.yaml", "mod.yaml", "removed.yaml"}
	{
		{
			got := got.Paths()
			assert.ErrorIf(t, !slices.Equal(got, want), "paths = %v, want %v", got, want)
		}
	}
}

func TestDetect_SizeDifferAlwaysChanged(t *testing.T) {
	t.Parallel()
	// Different sizes: detector should flag without hashing.
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "f.yaml", "small")
	testutil.WriteFile(t, after, "f.yaml", "this is a larger payload")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
	{
		assert.FatalIf(t, err != nil, "Detect: %v", err)
	}
	if !got.Contains("f.yaml") {
		t.Errorf("expected f.yaml in change set, got %v", got.Paths())
	}
}

func TestDetect_HiddenScanRoot(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	before := filepath.Join(parent, ".before")
	after := filepath.Join(parent, ".after")
	testutil.WriteFile(t, before, "same.yaml", "x")
	testutil.WriteFile(t, after, "same.yaml", "x")
	testutil.WriteFile(t, before, "mod.yaml", "old")
	testutil.WriteFile(t, after, "mod.yaml", "new")

	got, err := Detect(t.Context(), testInput(before), testInput(after))
	{
		assert.FatalIf(t, err != nil, "Detect: %v", err)
	}
	{
		{
			paths := got.Paths()
			assert.ErrorIf(
				t,
				!slices.Equal(paths, []string{"mod.yaml"}),
				"hidden scan root paths = %v, want [mod.yaml]",
				paths,
			)
		}
	}
}

// TestDetect_SameSizeSameMtime_StillDetected pins the correctness
// fix: the old (size, mtime) fast path silently treated two distinct
// same-sized files written within the same coarse-mtime tick as
// identical, dropping real changes. Removing the fast path means
// same-size files always get content-compared, so a write-write
// pattern that happens within an HFS+ 1-second tick is still
// reported. We simulate the worst case by explicitly stamping
// identical mtimes on both files post-write.
func TestDetect_SameSizeSameMtime_StillDetected(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "mod.yaml", "OLD")
	testutil.WriteFile(t, after, "mod.yaml", "NEW")
	// Exact same mtime — what the mtime-fast-path used to short-circuit on.
	stamp := time.Now()
	{
		{
			err := os.Chtimes(filepath.Join(before, "mod.yaml"), stamp, stamp)
			assert.FatalIf(t, err != nil, "chtimes before: %v", err)
		}
	}
	{
		{
			err := os.Chtimes(filepath.Join(after, "mod.yaml"), stamp, stamp)
			assert.FatalIf(t, err != nil, "chtimes after: %v", err)
		}
	}

	got, err := Detect(t.Context(), testInput(before), testInput(after))
	{
		assert.FatalIf(t, err != nil, "Detect: %v", err)
	}
	if !got.Contains("mod.yaml") {
		t.Errorf("Detect missed same-size same-mtime modification — fast-path regression. Paths: %v", got.Paths())
	}
}

func TestHashFile(t *testing.T) {
	t.Parallel()
	prefix := strings.Repeat("x", 32*1024)
	for _, tc := range []struct {
		name, same, different string
		dataEOF               bool
	}{
		{name: "small", same: "hello", different: "different"},
		{name: "data_eof", same: "hello", different: "different", dataEOF: true},
		{name: "multi_chunk", same: prefix + "first", different: prefix + "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, content := range []string{tc.same, tc.same, tc.different} {
				data := []byte(content)
				input := &cancellingInput{FS: fstest.MapFS{"file.yaml": {Data: data}}, dataEOF: tc.dataEOF}
				got, err := hashFile(t.Context(), input, "file.yaml")
				{
					assert.FatalIf(t, err != nil, "hashFile: %v", err)
				}
				{
					{
						want := sha256.Sum256(data)
						assert.ErrorIf(t, got != want, "hash = %x, want %x", got, want)
					}
				}
			}
		})
	}
}

func testInput(root string) *store.SourceArtifact {
	if root == "" {
		return nil
	}
	return &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
}

func TestDetect_MissingInputsWrapInput(t *testing.T) {
	t.Parallel()
	for _, input := range []*store.SourceArtifact{nil, {}} {
		_, err := Detect(t.Context(), input, &store.SourceArtifact{FS: fstest.MapFS{}})
		{
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
				err.Error() != "both admitted inputs required", "missing input error=%v", err)
		}
	}
}

func TestDetect_LastBodyCancellation(t *testing.T) {
	t.Parallel()
	for _, side := range []string{"before", "after"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()
			for _, shape := range []string{"data_eof", "multi_chunk"} {
				t.Run(shape, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					data := "same"
					if shape == "multi_chunk" {
						data = strings.Repeat(data, 16*1024)
					}
					input := fstest.MapFS{"file.yaml": {Data: []byte(data)}}
					cancelled := func() bool { return ctx.Err() != nil }
					before := &cancellingInput{FS: input, cancelled: cancelled, dataEOF: shape == "data_eof"}
					after := &cancellingInput{FS: input, cancelled: cancelled, dataEOF: shape == "data_eof"}
					if side == "before" {
						before.cancel = cancel
					} else {
						after.cancel = cancel
					}
					set, err := Detect(ctx, &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
					t.Logf(
						"before: opened=%d closed=%d reads after cancel=%d; after: opened=%d closed=%d reads after cancel=%d",
						before.opened.Load(),
						before.closed.Load(),
						before.postCancel.Load(),
						after.opened.Load(),
						after.closed.Load(),
						after.postCancel.Load(),
					)
					{
						assert.ErrorIf(
							t,
							!errors.Is(err, context.Canceled) || set != nil,
							"cancelled last body=%v,%v",
							set,
							err,
						)
					}
					{
						assert.ErrorIf(
							t,
							before.postCancel.Load() != 0 || after.postCancel.Load() != 0,
							"%v",
							"content read started after cancellation",
						)
					}
					{
						assert.ErrorIf(
							t,
							side == "before" && after.opened.Load() != 0,
							"%v",
							"after-side hash opened after cancellation",
						)
					}
					{
						assert.ErrorIf(
							t,
							before.opened.Load() != before.closed.Load() || after.opened.Load() != after.closed.Load(),
							"%v",
							"hash files not closed",
						)
					}
				})
			}
		})
	}
}

type cancellingInput struct {
	fs.FS

	cancel                     context.CancelFunc
	cancelled                  func() bool
	dataEOF                    bool
	opened, closed, postCancel atomic.Int32
}

func (c *cancellingInput) Open(name string) (fs.File, error) {
	f, err := c.FS.Open(name)
	if err != nil || name != "file.yaml" {
		return f, err
	}
	c.opened.Add(1)
	return &cancellingFile{File: f, input: c}, nil
}

type cancellingFile struct {
	fs.File

	input *cancellingInput
}

func (c *cancellingFile) Read(p []byte) (int, error) {
	if c.input.cancelled != nil && c.input.cancelled() {
		c.input.postCancel.Add(1)
	}
	n, err := c.File.Read(p)
	if n > 0 {
		if c.input.cancel != nil {
			c.input.cancel()
		}
		if c.input.dataEOF {
			err = io.EOF
		}
	}
	return n, err
}

func (c *cancellingFile) Close() error {
	c.input.closed.Add(1)
	return c.File.Close()
}

func TestDetect_HashLimit(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed", "canceled", "read error"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				testDetectHashLimitCase(t, &scenario)
			})
		},
		)
	}
}

type hashReads struct {
	release                                 chan struct{}
	cancelled                               func() bool
	err                                     error
	failed                                  atomic.Bool
	active, maximum, opened, closed, before atomic.Int32
	started, postCancel                     atomic.Int32
}

type hashInput struct {
	fs.FS

	reads  *hashReads
	before bool
}

func (f *hashInput) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || filepath.Ext(name) != ".yaml" {
		return file, err
	}
	if f.before {
		f.reads.before.Add(1)
	}
	f.reads.opened.Add(1)
	active := f.reads.active.Add(1)
	for maximum := f.reads.maximum.Load(); active > maximum; maximum = f.reads.maximum.Load() {
		if f.reads.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	return &hashReadFile{File: file, reads: f.reads, block: f.before}, nil
}

type hashReadFile struct {
	fs.File

	reads   *hashReads
	started bool
	block   bool
}

func (f *hashReadFile) Read(p []byte) (int, error) {
	f.reads.started.Add(1)
	if f.reads.cancelled() {
		f.reads.postCancel.Add(1)
	}
	if !f.started {
		f.started = true
		if f.block {
			<-f.reads.release
			if f.reads.err != nil && f.reads.failed.CompareAndSwap(false, true) {
				return 0, f.reads.err
			}
		}
	}
	return f.File.Read(p)
}

func (f *hashReadFile) Close() error {
	f.reads.closed.Add(1)
	f.reads.active.Add(-1)
	return f.File.Close()
}

func TestDetect_OneSidedInputEvidence(t *testing.T) {
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
			set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
			assert.NoError(t, err)
			assert.Diff(t, set.Paths(), []string{tc.file})
			side := 0
			if tc.after {
				side = 1
			}
			assert.Diff(
				t,
				slices.SortedFunc(maps.Keys(set.oneSided[side][tc.file]), manifest.NamedResource.Compare),
				tc.want,
			)
			assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
		})
	}
}

func TestSet_WithReaders(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"move", "changed", "added", "removed", "partial", "ambiguous content"} {
		t.Run("declaration "+scenario, func(t *testing.T) {
			t.Parallel()
			testSetWithReadersCase(t, &scenario)
		},
		)
	}
	for _, tc := range []struct {
		name, file, body string
		before           bool
	}{
		{"before", "templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n", true},
		{"after", "templates/cm.yaml", "{{ template }}", false},
		{"binary", "charts/child.tgz", "archive", false},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testSetWithReadersCase2(t, &tc) },
		)
	}
}

func TestSet_WithReadersBuildInputs(t *testing.T) {
	t.Parallel()
	child := &manifest.Kustomization{Name: "child", Namespace: "flux-system"}
	reader := testutil.HelmReleaseID("apps", "reader")
	inputs := [2]*store.SourceArtifact{}
	indexes := [2]*manifest.DiscoveryIndex{}
	for side := range inputs {
		inputs[side] = &store.SourceArtifact{
			LocalPath: t.TempDir(),
			FS:        fstest.MapFS{"control.yaml": {Data: fmt.Appendf(nil, "resources: [side-%d.yaml]\n", side)}},
		}
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Named(): {File: "child.yaml", Inputs: []string{"control.yaml"}},
				reader:        {File: "nested/release.yaml", Parent: child.Named()},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
		}
	}
	changes, err := Detect(t.Context(), inputs[0], inputs[1])
	assert.NoError(t, err)
	qualified, err := changes.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	changed, known := qualified.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, child.Named(), nil)
	assert.Equal(t, known, false)
	assert.Equal(t, changed, false)
	for _, id := range []manifest.NamedResource{child.Named(), reader} {
		assert.Equal(t, qualified.inputs[id].File, "control.yaml")
	}
}

func testDetectHashLimitCase(t *testing.T, testCaseInput *string) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reads := &hashReads{release: make(chan struct{}), cancelled: func() bool { return ctx.Err() != nil }}
	released := false
	defer func() {
		if !released {
			close(reads.release)
		}
	}()
	readErr := errors.New("hash read failed")
	if (*testCaseInput) == "read error" {
		reads.err = readErr
	}
	before, after := fstest.MapFS{}, fstest.MapFS{}
	want := make([]string, 0, 17)
	for i := range 17 {
		name := fmt.Sprintf("file-%02d.yaml", i)
		before[name] = &fstest.MapFile{Data: []byte("old")}
		after[name] = &fstest.MapFile{Data: []byte("new")}
		want = append(want, name)
	}
	type result struct {
		set *Set
		err error
	}
	done := make(chan result, 1)
	go func() {
		set, err := Detect(
			ctx,
			&store.SourceArtifact{FS: &hashInput{FS: before, reads: reads, before: true}},
			&store.SourceArtifact{FS: &hashInput{FS: after, reads: reads}},
		)
		done <- result{set, err}
	}()
	synctest.Wait()
	if reads.active.Load() != 8 || reads.maximum.Load() != 8 || reads.before.Load() != 8 ||
		reads.started.Load() != 8 {
		t.Fatalf(
			"blocked jobs: active=%d peak=%d before=%d reads=%d, want eight overlapping jobs",
			reads.active.Load(),
			reads.maximum.Load(),
			reads.before.Load(),
			reads.started.Load(),
		)
	}
	switch *testCaseInput {
	case "canceled":
		cancel()
	case "read error":
		reads.release <- struct{}{}
	}
	synctest.Wait()
	select {
	case got := <-done:
		t.Fatalf("Detect returned before blocked readers drained: %+v", got)
	default:
	}
	if (*testCaseInput) == "read error" && (reads.active.Load() != 7 || reads.before.Load() != 8) {
		t.Fatalf(
			"failed submission started extra jobs: active=%d before=%d",
			reads.active.Load(),
			reads.before.Load(),
		)
	}
	close(reads.release)
	released = true
	got := <-done
	assertHashReadersDrained(t, testCaseInput, reads)
	switch *testCaseInput {
	case "changed":
		if got.err != nil || got.set == nil || !slices.Equal(got.set.Paths(), want) ||
			reads.before.Load() != 17 ||
			reads.opened.Load() != 34 {
			t.Fatalf(
				"hash results: set=%v error=%v before=%d opened=%d",
				got.set,
				got.err,
				reads.before.Load(),
				reads.opened.Load(),
			)
		}
	default:
		cause := readErr
		if (*testCaseInput) == "canceled" {
			cause = context.Canceled
		}
		if !errors.Is(got.err, cause) || got.set != nil || reads.before.Load() != 8 {
			t.Fatalf(
				"failed hash results: set=%v error=%v before=%d",
				got.set,
				got.err,
				reads.before.Load(),
			)
		}
	}
}

func assertHashReadersDrained(t *testing.T, testCaseInput *string, reads *hashReads) {
	t.Helper()
	if (*testCaseInput) == "canceled" {
		t.Logf(
			"read starts=%d reads after cancel=%d before opens=%d after opens=%d closes=%d",
			reads.started.Load(),
			reads.postCancel.Load(),
			reads.before.Load(),
			reads.opened.Load()-reads.before.Load(),
			reads.closed.Load(),
		)
		{
			assert.ErrorIf(
				t,
				reads.postCancel.Load() != 0 || reads.opened.Load() != reads.before.Load(),
				"%v",
				"hash work started after cancellation",
			)
		}
	}
	if reads.active.Load() != 0 || reads.opened.Load() != reads.closed.Load() || reads.maximum.Load() > 8 {
		t.Fatalf(
			"readers not drained: active=%d opened=%d closed=%d peak=%d",
			reads.active.Load(),
			reads.opened.Load(),
			reads.closed.Load(),
			reads.maximum.Load(),
		)
	}
}

func testSetWithReadersCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	declaration := &manifest.ConfigMap{
		Name:      "settings",
		Namespace: "apps",
		Data:      map[string]any{"value": "same"},
	}
	reader := testutil.HelmReleaseID("apps", "reader")
	var indexes [2]*manifest.DiscoveryIndex
	inputs := [2]*store.SourceArtifact{}
	for side, file := range []string{"old.yaml", "new.yaml"} {
		body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: same}\n"
		object := *declaration
		if (*testCaseInput) == "changed" && side == 1 {
			body = strings.ReplaceAll(body, "same", "different")
			object.Data = map[string]any{"value": "different"}
		}
		files := fstest.MapFS{file: {Data: []byte(body)}}
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{object.Named(): {File: file}},
			Readers: map[string][]manifest.FileReader{file: {
				{Reader: reader, Source: manifest.BootstrapSourceID, Declaration: object.Named(), Exact: true},
				{
					Reader: testutil.KustomizationID("apps", []string{"old-owner", "new-owner"}[side]),
					Source: manifest.BootstrapSourceID,
				},
			}},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
		}
		if (*testCaseInput) == "added" && side == 0 || (*testCaseInput) == "removed" && side == 1 {
			files = fstest.MapFS{}
			indexes[side].Records = nil
			indexes[side].Readers = nil
		}
		if (*testCaseInput) == "partial" && side == 1 {
			indexes[side].CompleteSources = nil
		}
		if (*testCaseInput) == "ambiguous content" && side == 1 {
			files[file].Data = append(
				files[file].Data,
				[]byte("---\n"+strings.ReplaceAll(body, "same", "conflicting"))...)
		}
		inputs[side] = &store.SourceArtifact{FS: files}
	}
	set, err := Detect(t.Context(), inputs[0], inputs[1])
	assert.NoError(t, err)
	resolved, err := set.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	changed, known := resolved.DeclarationChanged(
		manifest.TreeBase,
		manifest.BootstrapSourceID,
		declaration.Named(),
		nil,
	)
	assert.Equal(t, known, (*testCaseInput) != "partial" && (*testCaseInput) != "ambiguous content")
	if known {
		assert.Equal(t, changed, (*testCaseInput) != "move")
	}
	_, selected := resolved.inputs[reader]
	assert.Equal(t, selected, (*testCaseInput) != "move")
	repeated, err := set.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	assert.Diff(t, repeated.inputs, resolved.inputs)
}

func testSetWithReadersCase2(t *testing.T, testCaseInput *struct {
	name string

	file   string
	body   string
	before bool
},
) {
	t.Helper()

	before, after := fstest.MapFS{}, fstest.MapFS{}
	present := after
	if testCaseInput.before {
		present = before
	}
	present[testCaseInput.file] = &fstest.MapFile{Data: []byte(testCaseInput.body)}
	set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
	assert.NoError(t, err)
	original := maps.Clone(set.inputs)
	reader := testutil.KustomizationID("apps", "reader")
	wrongSide := testutil.KustomizationID("apps", "opposite")
	unrelated := testutil.KustomizationID("apps", "unrelated")
	admitted := &manifest.DiscoveryIndex{Readers: map[string][]manifest.FileReader{
		testCaseInput.file: {
			{Reader: reader, Source: manifest.BootstrapSourceID},
		},
		path.Dir(testCaseInput.file) + "/other": {{Reader: unrelated, Source: manifest.BootstrapSourceID}},
	}}
	opposite := &manifest.DiscoveryIndex{Readers: map[string][]manifest.FileReader{
		testCaseInput.file: {{Reader: wrongSide, Source: manifest.BootstrapSourceID}},
	}}
	indexes := [2]*manifest.DiscoveryIndex{opposite, admitted}
	if testCaseInput.before {
		indexes = [2]*manifest.DiscoveryIndex{admitted, opposite}
	}
	want := map[manifest.NamedResource]InputChange{reader: {File: testCaseInput.file}}
	for id, input := range original {
		input.Unbound = false
		want[id] = input
	}
	resolved, err := set.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	assert.Diff(t, resolved.inputs, want)
	assert.Diff(t, set.inputs, original)
	repeated, err := set.WithReaders(
		t.Context(),
		indexes[0],
		indexes[1],
		[2]map[manifest.NamedResource]inputprojection.Projection{},
	)
	assert.NoError(t, err)
	assert.Diff(t, repeated.inputs, want)
}
