package kustomize

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type reads struct {
	files, dirs, probed []string
}

func recordAgainst(root string, lower fs.FS, read reads) readSetSnapshot {
	rs := newReadSet(root)
	for _, rel := range read.files {
		abs := filepath.Join(root, rel)
		b, err := fs.ReadFile(lower, rel)
		rs.recordFile(abs, b, err)
	}
	for _, rel := range read.dirs {
		abs := filepath.Join(root, rel)
		e, _ := fs.ReadDir(lower, rel)
		rs.recordDir(abs, entryNames(e))
	}
	for _, rel := range read.probed {
		abs := filepath.Join(root, rel)
		info, err := fs.Stat(lower, rel)
		rs.recordNode(abs, observedKind(err == nil, err == nil && info.IsDir()))
	}
	return rs.snapshot()
}

func TestReadSet_Snapshot(t *testing.T) {
	t.Parallel()
	const root = "/src"
	record := func(names []string) *readSet {
		rs := newReadSet(root)
		for _, name := range names {
			rs.recordFile(filepath.Join(root, name+".yaml"), []byte(name), nil)
			rs.recordDir(filepath.Join(root, name+"-dir"), []string{"child"})
			rs.recordNode(filepath.Join(root, name+"-node"), kindAbsent)
			rs.recordNode(filepath.Join(root, name+".yaml"), kindFile)
			rs.recordNode(filepath.Join(root, name+"-dir"), kindDir)
		}
		return rs
	}
	rs := record([]string{"z", "a"})
	snap := rs.snapshot()
	want := readSetSnapshot{
		Files: []localEntry{
			{Path: "a.yaml", Hash: manifest.SHA256Hex([]byte("a"))},
			{Path: "z.yaml", Hash: manifest.SHA256Hex([]byte("z"))},
		},
		Dirs: []localEntry{
			{Path: "a-dir", Hash: hashEntries([]string{"child"})},
			{Path: "z-dir", Hash: hashEntries([]string{"child"})},
		},
		Nodes: []localEntry{{Path: "a-node", Kind: kindAbsent}, {Path: "z-node", Kind: kindAbsent}},
	}
	{
		diff := cmp.Diff(want, snap)
		assert.FatalIf(t, diff != "", "snapshot (-want +got):\n%s", diff)
	}
	frame := testRenderFrame(t, snap, []byte("output"))
	reversed := record([]string{"a", "z"})
	{
		assert.FatalIf(
			t,
			!bytes.Equal(frame, testRenderFrame(t, reversed.snapshot(), []byte("output"))),
			"%s",
			"snapshot frame depends on observation order",
		)
	}
	rs.recordFile(filepath.Join(root, "a.yaml"), []byte("changed"), nil)
	rs.recordDir(filepath.Join(root, "a-dir"), []string{"new-child"})
	rs.recordNode(filepath.Join(root, "a-node"), kindFile)
	rs.recordNode(filepath.Join(root, "new-node"), kindDir)
	{
		diff := cmp.Diff(want, snap)
		assert.FatalIf(t, diff != "", "frozen snapshot mutated (-want +got):\n%s", diff)
	}
	{
		assert.FatalIf(
			t,
			!bytes.Equal(frame, testRenderFrame(t, snap, []byte("output"))),
			"%s",
			"frozen snapshot frame changed after recording",
		)
	}
	empty := readSetSnapshot{Files: []localEntry{}, Dirs: []localEntry{}, Nodes: []localEntry{}}
	{
		diff := cmp.Diff(empty, newReadSet(root).snapshot())
		assert.FatalIf(t, diff != "", "empty snapshot (-want +got):\n%s", diff)
	}
}

func TestReadSet_SnapshotConcurrentRecording(t *testing.T) {
	t.Parallel()
	const root = "/src"
	rs := newReadSet(root)
	for _, name := range []string{"z", "a"} {
		rs.recordFile(filepath.Join(root, name+".yaml"), []byte(name), nil)
		rs.recordDir(filepath.Join(root, name+"-dir"), []string{"child"})
		rs.recordNode(filepath.Join(root, name+"-node"), kindAbsent)
	}
	start := make(chan struct{})
	written := make(chan struct{})
	read := make(chan readSetSnapshot)
	go func() {
		defer close(written)
		<-start
		rs.recordFile(filepath.Join(root, "b.yaml"), []byte("b"), nil)
		rs.recordDir(filepath.Join(root, "b-dir"), []string{"child"})
		rs.recordNode(filepath.Join(root, "b.yaml"), kindFile)
		rs.recordNode(filepath.Join(root, "b-dir"), kindDir)
		rs.recordNode(filepath.Join(root, "b-node"), kindAbsent)
	}()
	go func() {
		<-start
		read <- rs.snapshot()
	}()
	close(start)
	snap := <-read
	<-written
	for _, entries := range [][]localEntry{snap.Files, snap.Dirs} {
		{
			assert.FatalIf(
				t,
				!slices.IsSortedFunc(entries, func(a, b localEntry) int { return strings.Compare(a.Path, b.Path) }),
				"unsorted snapshot entries: %+v",
				entries,
			)
		}
	}
	assert.FatalIf(
		t,
		!slices.IsSortedFunc(snap.Nodes, func(a, b localEntry) int { return strings.Compare(a.Path, b.Path) }),
		"unsorted snapshot nodes: %+v",
		snap.Nodes,
	)
	for _, node := range snap.Nodes {
		matches := func(entry localEntry) bool { return entry.Path == node.Path }
		{
			assert.ErrorIf(
				t,
				slices.ContainsFunc(snap.Files, matches) || slices.ContainsFunc(snap.Dirs, matches),
				"redundant node in snapshot: %+v",
				node,
			)
		}
	}
	want := readSetSnapshot{
		Files: []localEntry{
			{Path: "a.yaml", Hash: manifest.SHA256Hex([]byte("a"))},
			{Path: "b.yaml", Hash: manifest.SHA256Hex([]byte("b"))},
			{Path: "z.yaml", Hash: manifest.SHA256Hex([]byte("z"))},
		},
		Dirs: []localEntry{
			{Path: "a-dir", Hash: hashEntries([]string{"child"})},
			{Path: "b-dir", Hash: hashEntries([]string{"child"})},
			{Path: "z-dir", Hash: hashEntries([]string{"child"})},
		},
		Nodes: []localEntry{
			{Path: "a-node", Kind: kindAbsent},
			{Path: "b-node", Kind: kindAbsent},
			{Path: "z-node", Kind: kindAbsent},
		},
	}
	{
		diff := cmp.Diff(want, rs.snapshot())
		assert.FatalIf(t, diff != "", "completed writes (-want +got):\n%s", diff)
	}
}

func TestReadSet_HitAndInvalidation(t *testing.T) {
	t.Parallel()
	const root = "/src/app"
	base := func() fstest.MapFS {
		return fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [a.yaml]\n")},
			"a.yaml":             {Data: []byte("kind: ConfigMap\n")},
			"b.yaml":             {Data: []byte("kind: Secret\n")},
		}
	}
	// A build that read the kustomization + a.yaml, listed the dir, and probed a
	// (missing) optional component.
	snap := recordAgainst(root, base(), reads{
		files:  []string{"kustomization.yaml", "a.yaml"},
		dirs:   []string{"."},
		probed: []string{"comp"},
	})

	t.Run("unchanged tree is a hit", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationUnchangedTreeIsAHit(
			t,
			&testReadSetHitAndInvalidationUnchangedTreeIsAHitInputs{base: &base, snap: &snap},
		)
	},
	)

	t.Run("editing a recorded file misses", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationEditingARecordedFileMisses(
			t,
			&testReadSetHitAndInvalidationEditingARecordedFileMissesInputs{base: &base, snap: &snap},
		)
	},
	)

	t.Run("editing an UNREAD file stays a hit (granularity)", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationEditingAnUNREADFileStaysAHitGranularity(
			t,
			&testReadSetHitAndInvalidationEditingAnUNREADFileStaysAHitGranularityInputs{base: &base, snap: &snap},
		)
	},
	)

	t.Run("adding a file to the listed dir misses", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationAddingAFileToTheListedDirMisses(
			t,
			&testReadSetHitAndInvalidationAddingAFileToTheListedDirMissesInputs{base: &base, snap: &snap},
		)
	},
	)

	t.Run("deleting a recorded file misses", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationDeletingARecordedFileMisses(
			t,
			&testReadSetHitAndInvalidationDeletingARecordedFileMissesInputs{base: &base, snap: &snap},
		)
	},
	)

	t.Run("a probed-absent component appearing misses", func(t *testing.T) {
		t.Parallel()
		testReadSetHitAndInvalidationAProbedAbsentComponentAppearingMisses(
			t,
			&testReadSetHitAndInvalidationAProbedAbsentComponentAppearingMissesInputs{base: &base, snap: &snap},
		)
	},
	)
	for _, tc := range []struct {
		name          string
		before, after fstest.MapFS
	}{
		{name: "absent to file", before: fstest.MapFS{}, after: fstest.MapFS{"comp": {}}},
		{name: "file to directory", before: fstest.MapFS{"comp": {}}, after: fstest.MapFS{"comp/child": {}}},
		{name: "directory to file", before: fstest.MapFS{"comp/child": {}}, after: fstest.MapFS{"comp": {}}},
		{name: "directory to absent", before: fstest.MapFS{"comp/child": {}}, after: fstest.MapFS{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReadSetHitAndInvalidationCase(t, &tc)
		},
		)
	}
}

func TestReadSet_RemoteObservations(t *testing.T) {
	t.Parallel()
	entries := []remoteEntry{
		{
			Input: remoteInput{
				Operation:  remoteGit,
				URL:        "file:///example.invalid/b.git",
				Ref:        "z",
				Timeout:    27 * time.Second,
				Submodules: true,
			},
			Identity: remoteIdentity{ArtifactInput: store.ArtifactInput{Key: "family", Content: "1"}},
		},
		{
			Input:    remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/b.git//file"},
			Identity: remoteIdentity{ArtifactInput: store.ArtifactInput{Content: "http-b"}},
		},
		{
			Input: remoteInput{
				Operation:  remoteGit,
				URL:        "file:///example.invalid/b.git",
				Ref:        "a",
				Timeout:    27 * time.Second,
				Submodules: true,
			},
			Identity: remoteIdentity{ArtifactInput: store.ArtifactInput{Key: "family", Content: "2"}},
		},
		{
			Input:    remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/a"},
			Identity: remoteIdentity{HTTPStatus: http.StatusNotFound},
		},
	}
	policy := entries[0]
	policy.Input.Timeout = time.Minute
	entries = append(entries, policy)
	policy.Input.Submodules = false
	entries = append(entries, policy)
	entries[1].Parents = []remoteInput{entries[0].Input}
	entries[3].Parents = []remoteInput{
		entries[0].Input,
		entries[2].Input,
		entries[0].Input,
		entries[4].Input,
		entries[5].Input,
	}
	want := slices.Clone(entries)
	want[3].Parents = []remoteInput{entries[2].Input, entries[0].Input, entries[5].Input, entries[4].Input}
	slices.SortFunc(want, func(a, b remoteEntry) int { return compareRemoteInputs(a.Input, b.Input) })
	var first []byte
	var firstOnce sync.Once
	for _, reverse := range []bool{false, true} {
		t.Run(strconv.FormatBool(reverse), func(t *testing.T) {
			t.Parallel()
			testReadSetRemoteObservationsCase(
				t,
				&testReadSetRemoteObservationsCaseInputs{
					entries:   &entries,
					want:      &want,
					first:     &first,
					firstOnce: &firstOnce,
					reverse:   &reverse,
				},
			)
		},
		)
	}
}

func TestReadSet_RemoteValidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"stable-chain", "shared-parent-changes", "changed-leaf", "parent-failure", "canceled-parent", "independent-input", "descriptor-cycle", "missing-parent", "duplicate-entry"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testReadSetRemoteValidationCase(t, &scenario)
		},
		)
	}
}

func validReadSet(t *testing.T, snap readSetSnapshot, lower fs.FS) bool {
	t.Helper()
	valid, err := snap.stillValid(t.Context(), nil, &treeFS{lower: lower}, false)
	{
		assert.NoError(t, err)
	}
	return valid
}

func TestReadSet_RemotePolicyValidation(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	var changed atomic.Bool
	c.SetGitBaseFetcher(
		func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			revision := fmt.Sprintf("%v/%t", request.Timeout, request.Submodules)
			if changed.Load() && request.Timeout == time.Minute && !request.Submodules {
				revision = "changed"
			}
			return &store.SourceArtifact{Key: "same", Content: revision, Revision: revision, FS: fstest.MapFS{}}, nil
		},
	)
	rec := newReadSet("/src")
	for _, input := range []remoteInput{
		{Operation: remoteGit, URL: "file:///repo", Timeout: time.Second, Submodules: true},
		{Operation: remoteGit, URL: "file:///repo", Timeout: time.Minute, Submodules: true},
		{Operation: remoteGit, URL: "file:///repo", Timeout: time.Minute, Submodules: false},
	} {
		_, identity, err := input.acquire(t.Context(), c, &treeFS{})
		{
			assert.NoError(t, err)
		}
		rec.recordRemote(input, identity, nil)
	}
	snap := rec.snapshot()
	{
		valid, err := snap.stillValid(t.Context(), c, &treeFS{}, true)
		assert.FatalIf(t, err != nil || !valid, "unchanged policies: valid=%t error=%v", valid, err)
	}
	changed.Store(true)
	{
		valid, err := snap.stillValid(t.Context(), c, &treeFS{}, true)
		assert.FatalIf(t, err != nil || valid, "policy-specific change: valid=%t error=%v", valid, err)
	}
}

func TestReadSet_FailedRemoteObservation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		operation remoteOperation
		status    int
		saved     remoteIdentity
		valid     bool
	}{
		{name: "unchanged-404", status: http.StatusNotFound, saved: remoteIdentity{HTTPStatus: http.StatusNotFound}, valid: true},
		{name: "404-to-403", status: http.StatusForbidden, saved: remoteIdentity{HTTPStatus: http.StatusNotFound}},
		{name: "404-to-429", status: http.StatusTooManyRequests, saved: remoteIdentity{HTTPStatus: http.StatusNotFound}},
		{name: "404-to-500", status: http.StatusInternalServerError, saved: remoteIdentity{HTTPStatus: http.StatusNotFound}},
		{name: "empty-to-404", status: http.StatusNotFound},
		{name: "empty-to-empty", status: http.StatusForbidden},
		{name: "unavailable-git", operation: remoteGit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)
			c := newTestTreeCache(t, "")
			descriptor := remoteInput{Operation: tc.operation, URL: srv.URL}
			snap := readSetSnapshot{Remotes: []remoteEntry{{Input: descriptor, Identity: tc.saved}}}
			{
				valid, err := snap.stillValid(t.Context(), c, &treeFS{}, true)
				assert.FatalIf(
					t,
					err != nil || valid != tc.valid,
					"validation=%t error=%v, want %t",
					valid,
					err,
					tc.valid,
				)
			}
		})
	}
}

func TestReadSet_RemoteFailureCancellation(t *testing.T) {
	t.Parallel()
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprintf("completed=%t", completed), func(t *testing.T) {
			t.Parallel()
			testReadSetRemoteFailureCancellationCase(
				t,
				&completed,
			)
		},
		)
	}
}

type testReadSetHitAndInvalidationUnchangedTreeIsAHitInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationUnchangedTreeIsAHit(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationUnchangedTreeIsAHitInputs,
) {
	t.Helper()

	{
		assert.ErrorIf(
			t,
			!validReadSet(t, (*testCaseInput.snap), (*testCaseInput.base)()),
			"%s",
			"identical tree must validate (cache hit)",
		)
	}
}

type testReadSetHitAndInvalidationEditingARecordedFileMissesInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationEditingARecordedFileMisses(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationEditingARecordedFileMissesInputs,
) {
	t.Helper()

	d := (*testCaseInput.base)()
	d["a.yaml"] = &fstest.MapFile{Data: []byte("kind: ConfigMap\ndata: {x: y}\n")}
	{
		assert.ErrorIf(
			t,
			validReadSet(t, (*testCaseInput.snap), d),
			"%s",
			"an edit to a recorded input must invalidate (cache miss)",
		)
	}
}

type testReadSetHitAndInvalidationEditingAnUNREADFileStaysAHitGranularityInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationEditingAnUNREADFileStaysAHitGranularity(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationEditingAnUNREADFileStaysAHitGranularityInputs,
) {
	t.Helper()

	d := (*testCaseInput.base)()
	d["b.yaml"] = &fstest.MapFile{Data: []byte("kind: Secret\nstringData: {k: v}\n")}
	{
		assert.ErrorIf(
			t,
			!validReadSet(t, (*testCaseInput.snap), d),
			"%s",
			"b.yaml was never read by this build; editing it must NOT invalidate",
		)
	}
}

type testReadSetHitAndInvalidationAddingAFileToTheListedDirMissesInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationAddingAFileToTheListedDirMisses(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationAddingAFileToTheListedDirMissesInputs,
) {
	t.Helper()

	d := (*testCaseInput.base)()
	d["c.yaml"] = &fstest.MapFile{Data: []byte("kind: Service\n")}
	{
		assert.ErrorIf(
			t,
			validReadSet(t, (*testCaseInput.snap), d),
			"%s",
			"a new file in the auto-scanned dir must invalidate",
		)
	}
}

type testReadSetHitAndInvalidationDeletingARecordedFileMissesInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationDeletingARecordedFileMisses(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationDeletingARecordedFileMissesInputs,
) {
	t.Helper()

	d := (*testCaseInput.base)()
	delete(d, "a.yaml")
	{
		assert.ErrorIf(t, validReadSet(t, (*testCaseInput.snap), d), "%s", "deleting a recorded input must invalidate")
	}
}

type testReadSetHitAndInvalidationAProbedAbsentComponentAppearingMissesInputs struct {
	base *func() fstest.MapFS
	snap *readSetSnapshot
}

func testReadSetHitAndInvalidationAProbedAbsentComponentAppearingMisses(
	t *testing.T,
	testCaseInput *testReadSetHitAndInvalidationAProbedAbsentComponentAppearingMissesInputs,
) {
	t.Helper()

	d := (*testCaseInput.base)()
	d["comp/kustomization.yaml"] = &fstest.MapFile{}
	{
		assert.ErrorIf(
			t,
			validReadSet(t, (*testCaseInput.snap), d),
			"%s",
			"an optional component that materialized must invalidate",
		)
	}
}

func testReadSetHitAndInvalidationCase(t *testing.T, testCaseInput *struct {
	name   string
	before fstest.MapFS
	after  fstest.MapFS
},
) {
	t.Helper()
	const root = "/src/app"

	snap := recordAgainst(root, testCaseInput.before, reads{probed: []string{"comp"}})
	{
		assert.FatalIf(
			t,
			!validReadSet(t, snap, testCaseInput.before) || validReadSet(t, snap, (*testCaseInput).after),
			"%s",
			"probed node transition did not invalidate",
		)
	}
}

type testReadSetRemoteObservationsCaseInputs struct {
	firstOnce *sync.Once
	entries   *[]remoteEntry
	want      *[]remoteEntry
	first     *[]byte
	reverse   *bool
}

func testReadSetRemoteObservationsCase(t *testing.T, testCaseInput *testReadSetRemoteObservationsCaseInputs) {
	t.Helper()
	rs := newReadSet("/src")
	{
		assert.FatalIf(
			t,
			rs.remotes != nil || rs.snapshot().Remotes != nil,
			"%s",
			"local-only recorder allocated remote observations",
		)
	}
	ordered := slices.Clone((*testCaseInput.entries))
	if *testCaseInput.reverse {
		slices.Reverse(ordered)
	}
	for entryIndex := range ordered {
		entry := ordered[entryIndex]
		parents := slices.Clone(entry.Parents)
		if *testCaseInput.reverse {
			slices.Reverse(parents)
		}
		for _, parent := range parents {
			rs.recordRemote(entry.Input, entry.Identity, []remoteInput{parent})
		}
		rs.recordRemote(entry.Input, entry.Identity, nil)
	}
	snap := rs.snapshot()
	{
		diff := cmp.Diff((*testCaseInput.want), snap.Remotes)
		assert.FatalIf(t, diff != "", "observations (-want +got):\n%s", diff)
	}
	frame, err := encodeFrame(&renderHeader{Reads: snap}, []byte("output"))
	{
		assert.NoError(t, err)
	}
	decodedFrame, err := decodeFrame(frame)
	decoded := decodedFrame.header.Reads
	assert.FatalIf(
		t,
		err != nil || !cmp.Equal(decoded.Remotes, snap.Remotes),
		"roundtrip=%+v error=%v",
		decoded.Remotes,
		err,
	)
	testCaseInput.firstOnce.Do(func() { *testCaseInput.first = frame })
	{
		assert.FatalIf(
			t,
			!bytes.Equal(frame, (*testCaseInput.first)),
			"%s",
			"snapshot order depends on observation order",
		)
	}
	rs.recordRemote(
		(*testCaseInput.entries)[3].Input,
		(*testCaseInput.entries)[3].Identity,
		[]remoteInput{(*testCaseInput.entries)[1].Input},
	)
	{
		diff := cmp.Diff((*testCaseInput.want), snap.Remotes)
		assert.FatalIf(t, diff != "", "frozen snapshot mutated (-want +got):\n%s", diff)
	}
	{
		assert.FatalIf(t, len(rs.snapshot().Remotes[0].Parents) != 5, "%s", "additional parent was not recorded")
	}
}
