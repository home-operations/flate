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
	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_BothPathsRequired(t *testing.T) {
	t.Parallel()
	if _, err := Detect(t.Context(), testInput(""), testInput("/tmp/x")); err == nil {
		t.Errorf("expected error for empty before")
	}
	if _, err := Detect(t.Context(), testInput("/tmp/x"), testInput("")); err == nil {
		t.Errorf("expected error for empty after")
	}
}

func TestDetect_IdenticalTreesEmpty(t *testing.T) {
	t.Parallel()
	before := t.TempDir()
	after := t.TempDir()
	testutil.WriteFile(t, before, "a.yaml", "x")
	testutil.WriteFile(t, after, "a.yaml", "x")
	got, err := Detect(t.Context(), testInput(before), testInput(after))
	if err != nil {
		t.Fatalf("Detect: %v", err)
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
	if err := os.Chtimes(filepath.Join(after, "mod.yaml"), time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	got, err := Detect(t.Context(), testInput(before), testInput(after))
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	want := []string{"added.yaml", "mod.yaml", "removed.yaml"}
	if got := got.Paths(); !slices.Equal(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
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
	if err != nil {
		t.Fatalf("Detect: %v", err)
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
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if paths := got.Paths(); !slices.Equal(paths, []string{"mod.yaml"}) {
		t.Errorf("hidden scan root paths = %v, want [mod.yaml]", paths)
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
	if err := os.Chtimes(filepath.Join(before, "mod.yaml"), stamp, stamp); err != nil {
		t.Fatalf("chtimes before: %v", err)
	}
	if err := os.Chtimes(filepath.Join(after, "mod.yaml"), stamp, stamp); err != nil {
		t.Fatalf("chtimes after: %v", err)
	}

	got, err := Detect(t.Context(), testInput(before), testInput(after))
	if err != nil {
		t.Fatalf("Detect: %v", err)
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
				if err != nil {
					t.Fatalf("hashFile: %v", err)
				}
				if want := sha256.Sum256(data); got != want {
					t.Errorf("hash = %x, want %x", got, want)
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
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing input error=%v", err)
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
					t.Logf("before: opened=%d closed=%d reads after cancel=%d; after: opened=%d closed=%d reads after cancel=%d", before.opened.Load(), before.closed.Load(), before.postCancel.Load(), after.opened.Load(), after.closed.Load(), after.postCancel.Load())
					if !errors.Is(err, context.Canceled) || set != nil {
						t.Errorf("cancelled last body=%v,%v", set, err)
					}
					if before.postCancel.Load() != 0 || after.postCancel.Load() != 0 {
						t.Error("content read started after cancellation")
					}
					if side == "before" && after.opened.Load() != 0 {
						t.Error("after-side hash opened after cancellation")
					}
					if before.opened.Load() != before.closed.Load() || after.opened.Load() != after.closed.Load() {
						t.Error("hash files not closed")
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
				if scenario == "read error" {
					reads.err = readErr
				}
				before, after := fstest.MapFS{}, fstest.MapFS{}
				var want []string
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
					set, err := Detect(ctx, &store.SourceArtifact{FS: &hashInput{FS: before, reads: reads, before: true}}, &store.SourceArtifact{FS: &hashInput{FS: after, reads: reads}})
					done <- result{set, err}
				}()
				synctest.Wait()
				if reads.active.Load() != 8 || reads.maximum.Load() != 8 || reads.before.Load() != 8 || reads.started.Load() != 8 {
					t.Fatalf("blocked jobs: active=%d peak=%d before=%d reads=%d, want eight overlapping jobs", reads.active.Load(), reads.maximum.Load(), reads.before.Load(), reads.started.Load())
				}
				switch scenario {
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
				if scenario == "read error" && (reads.active.Load() != 7 || reads.before.Load() != 8) {
					t.Fatalf("failed submission started extra jobs: active=%d before=%d", reads.active.Load(), reads.before.Load())
				}
				close(reads.release)
				released = true
				got := <-done
				if scenario == "canceled" {
					t.Logf("read starts=%d reads after cancel=%d before opens=%d after opens=%d closes=%d", reads.started.Load(), reads.postCancel.Load(), reads.before.Load(), reads.opened.Load()-reads.before.Load(), reads.closed.Load())
					if reads.postCancel.Load() != 0 || reads.opened.Load() != reads.before.Load() {
						t.Error("hash work started after cancellation")
					}
				}
				if reads.active.Load() != 0 || reads.opened.Load() != reads.closed.Load() || reads.maximum.Load() > 8 {
					t.Fatalf("readers not drained: active=%d opened=%d closed=%d peak=%d", reads.active.Load(), reads.opened.Load(), reads.closed.Load(), reads.maximum.Load())
				}
				if scenario == "changed" {
					if got.err != nil || got.set == nil || !slices.Equal(got.set.Paths(), want) || reads.before.Load() != 17 || reads.opened.Load() != 34 {
						t.Fatalf("hash results: set=%v error=%v before=%d opened=%d", got.set, got.err, reads.before.Load(), reads.opened.Load())
					}
				} else {
					cause := readErr
					if scenario == "canceled" {
						cause = context.Canceled
					}
					if !errors.Is(got.err, cause) || got.set != nil || reads.before.Load() != 8 {
						t.Fatalf("failed hash results: set=%v error=%v before=%d", got.set, got.err, reads.before.Load())
					}
				}
			})
		})
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
		{"foreign group", "input.yaml", strings.Replace(cm, "apiVersion: v1", "apiVersion: foreign.test/v1", 1), false, nil},
		{"noncanonical core", "input.yaml", strings.Replace(cm, "apiVersion: v1", "apiVersion: v2", 1), false, nil},
		{"nondata", "input.yaml", strings.Replace(cm, "kind: ConfigMap", "kind: Pod", 1), false, nil},
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
			var want map[manifest.NamedResource]struct{}
			if len(tc.want) > 0 {
				want = make(map[manifest.NamedResource]struct{})
				for _, id := range tc.want {
					want[id] = struct{}{}
				}
			}
			assert.Diff(t, set.inputs, want)
		})
	}
}

func TestSet_WithReaders(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, file, body string
		before           bool
	}{
		{"before", "templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n", true},
		{"after", "templates/cm.yaml", "{{ template }}", false},
		{"binary", "charts/child.tgz", "archive", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, after := fstest.MapFS{}, fstest.MapFS{}
			present := after
			if tc.before {
				present = before
			}
			present[tc.file] = &fstest.MapFile{Data: []byte(tc.body)}
			set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
			assert.NoError(t, err)
			original := maps.Clone(set.inputs)
			reader := testutil.KustomizationID("apps", "reader")
			wrongSide := testutil.KustomizationID("apps", "opposite")
			unrelated := testutil.KustomizationID("apps", "unrelated")
			admitted := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				reader:    {File: "reader.yaml", Inputs: []string{tc.file}},
				unrelated: {File: "unrelated.yaml", Inputs: []string{path.Dir(tc.file) + "/other"}},
			}}
			opposite := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				wrongSide: {File: "opposite.yaml", Inputs: []string{tc.file}},
			}}
			indexes := [2]*manifest.DiscoveryIndex{opposite, admitted}
			if tc.before {
				indexes = [2]*manifest.DiscoveryIndex{admitted, opposite}
			}
			want := map[manifest.NamedResource]struct{}{reader: {}}
			maps.Copy(want, original)
			resolved := set.WithReaders(indexes[0], indexes[1])
			assert.Diff(t, resolved.inputs, want)
			assert.Diff(t, set.inputs, original)
			assert.Diff(t, set.WithReaders(indexes[0], indexes[1]).inputs, want)
		})
	}
}

func TestDetect_OneSidedInputIgnorePolicies(t *testing.T) {
	t.Parallel()
	for _, ignored := range []string{"before", "after", "both"} {
		t.Run(ignored, func(t *testing.T) {
			t.Parallel()
			body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: apps}\n"
			before := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "before")}}
			after := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "after")}}
			if ignored != "after" {
				before[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
			}
			if ignored != "before" {
				after[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
			}
			set, err := Detect(t.Context(), &store.SourceArtifact{FS: before}, &store.SourceArtifact{FS: after})
			assert.NoError(t, err)
			assert.Equal(t, set.Contains("input.yaml"), ignored != "both")
			var want map[manifest.NamedResource]struct{}
			if ignored != "both" {
				name := "after"
				if ignored == "after" {
					name = "before"
				}
				want = map[manifest.NamedResource]struct{}{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}: {}}
			}
			assert.Diff(t, set.inputs, want)
		})
	}
}

func TestDetect_OneSidedInputReadFailure(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"open", "read"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("evidence unavailable")
			input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional}\n")}}, operation: operation, cause: cause}
			set, err := Detect(t.Context(), &store.SourceArtifact{FS: input}, &store.SourceArtifact{FS: fstest.MapFS{}})
			if set != nil || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml") {
				t.Fatalf("read failure set=%v err=%v", set, err)
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
	input := &cancellingInput{FS: fstest.MapFS{"file.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional}\n")}}, cancel: cancel, cancelled: func() bool { return ctx.Err() != nil }, dataEOF: true}
	set, err := Detect(ctx, &store.SourceArtifact{FS: input}, &store.SourceArtifact{FS: fstest.MapFS{}})
	if set != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation set=%v err=%v", set, err)
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
	assert.Diff(t, set.inputs, map[manifest.NamedResource]struct{}(nil))
}

func TestCollectInputs_CancelBeforeOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
	var set Set
	err := set.CollectInputs(ctx, input, "input.yaml", false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	assert.Equal(t, input.opened.Load(), int32(0))
}

type inputCheckContext func() context.Context

func (c inputCheckContext) Deadline() (time.Time, bool) { return c().Deadline() }
func (c inputCheckContext) Done() <-chan struct{}       { return c().Done() }
func (c inputCheckContext) Err() error                  { return c().Err() }
func (c inputCheckContext) Value(key any) any           { return c().Value(key) }

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
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
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
				if tc.cancel {
					cancel()
				}
				return copy(p, body), tc.readErr
			}
			var set Set
			err := set.CollectInputs(checked, input, "input.yaml", false)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				assert.Diff(t, set.inputs, map[manifest.NamedResource]struct{}{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}: {}})
			} else {
				if !errors.Is(err, tc.wantErr) || len(set.inputs) != 0 {
					t.Fatalf("collected inputs=%v error=%v", set.inputs, err)
				}
				if !tc.cancel && (!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")) {
					t.Fatalf("read failure classification=%v", err)
				}
			}
			assert.Equal(t, reads, tc.reads)
			assert.Equal(t, input.closed.Load(), int32(1))
			if tc.cancel {
				// Final cancellation resolution must not mask a missing read-boundary check.
				assert.Equal(t, cancellationChecks, 2)
			}
		})
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
	_, err = manifest.DecodeDocs(f)
	assert.NoError(t, f.Close())
	if err == nil || probeReads != 1 {
		t.Fatalf("malformed fixture must stop before the later read: reads=%d error=%v", probeReads, err)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			input := &evidenceFailureFS{FS: fstest.MapFS{"input.yaml": {}}}
			input.read = func(p []byte) (int, error) {
				reads++
				if reads == 1 {
					return copy(p, malformed), nil
				}
				if tc.cancel {
					cancel()
				}
				return copy(p, "later content"), tc.readErr
			}
			var set Set
			err := set.CollectInputs(ctx, input, "input.yaml", false)
			if tc.wantErr == nil {
				assert.NoError(t, err)
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("drained error=%v, want %v", err, tc.wantErr)
			} else if !tc.cancel && (!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "input.yaml")) {
				t.Fatalf("drained failure classification=%v", err)
			}
			assert.Equal(t, len(set.inputs), 0)
			assert.Equal(t, reads, 2)
			assert.Equal(t, input.closed.Load(), int32(1))
		})
	}
}
