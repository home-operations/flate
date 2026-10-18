package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"

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
		rs.recordNode(abs, diskKind(err == nil, err == nil && info.IsDir()))
	}
	return rs.snapshot()
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
		if !validReadSet(t, snap, base()) {
			t.Error("identical tree must validate (cache hit)")
		}
	})

	t.Run("editing a recorded file misses", func(t *testing.T) {
		t.Parallel()
		d := base()
		d["a.yaml"] = &fstest.MapFile{Data: []byte("kind: ConfigMap\ndata: {x: y}\n")}
		if validReadSet(t, snap, d) {
			t.Error("an edit to a recorded input must invalidate (cache miss)")
		}
	})

	t.Run("editing an UNREAD file stays a hit (granularity)", func(t *testing.T) {
		t.Parallel()
		d := base()
		d["b.yaml"] = &fstest.MapFile{Data: []byte("kind: Secret\nstringData: {k: v}\n")}
		if !validReadSet(t, snap, d) {
			t.Error("b.yaml was never read by this build; editing it must NOT invalidate")
		}
	})

	t.Run("adding a file to the listed dir misses", func(t *testing.T) {
		t.Parallel()
		d := base()
		d["c.yaml"] = &fstest.MapFile{Data: []byte("kind: Service\n")}
		if validReadSet(t, snap, d) {
			t.Error("a new file in the auto-scanned dir must invalidate")
		}
	})

	t.Run("deleting a recorded file misses", func(t *testing.T) {
		t.Parallel()
		d := base()
		delete(d, "a.yaml")
		if validReadSet(t, snap, d) {
			t.Error("deleting a recorded input must invalidate")
		}
	})

	t.Run("a probed-absent component appearing misses", func(t *testing.T) {
		t.Parallel()
		d := base()
		d["comp/kustomization.yaml"] = &fstest.MapFile{}
		if validReadSet(t, snap, d) {
			t.Error("an optional component that materialized must invalidate")
		}
	})
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
			snap := recordAgainst(root, tc.before, reads{probed: []string{"comp"}})
			if !validReadSet(t, snap, tc.before) || validReadSet(t, snap, tc.after) {
				t.Fatal("probed node transition did not invalidate")
			}
		})
	}

}

func TestReadSet_RemoteObservations(t *testing.T) {
	entries := []remoteEntry{
		{Input: remoteInput{Operation: remoteGit, URL: "https://example.invalid/b.git", Ref: "z"}, Identity: remoteIdentity{Key: "family", Revision: "1"}},
		{Input: remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/b.git//file"}, Identity: remoteIdentity{HTTP: "http-b"}},
		{Input: remoteInput{Operation: remoteGit, URL: "https://example.invalid/b.git", Ref: "a"}, Identity: remoteIdentity{Key: "family", Revision: "2"}},
		{Input: remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/a"}, Identity: remoteIdentity{HTTP: "http-a"}},
	}
	entries[1].Parents = []remoteInput{entries[0].Input}
	entries[3].Parents = []remoteInput{entries[0].Input, entries[2].Input, entries[0].Input}
	want := slices.Clone(entries)
	want[3].Parents = []remoteInput{entries[2].Input, entries[0].Input}
	slices.SortFunc(want, func(a, b remoteEntry) int { return compareRemoteInputs(a.Input, b.Input) })
	var first []byte
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			rs := newReadSet("/src")
			if rs.remotes != nil || rs.snapshot().Remotes != nil {
				t.Fatal("local-only recorder allocated remote observations")
			}
			ordered := slices.Clone(entries)
			if reverse {
				slices.Reverse(ordered)
			}
			for _, entry := range ordered {
				parents := slices.Clone(entry.Parents)
				if reverse {
					slices.Reverse(parents)
				}
				for _, parent := range parents {
					rs.recordRemote(entry.Input, entry.Identity, []remoteInput{parent})
				}
				rs.recordRemote(entry.Input, entry.Identity, nil)
			}
			snap := rs.snapshot()
			if diff := cmp.Diff(want, snap.Remotes); diff != "" {
				t.Fatalf("observations (-want +got):\n%s", diff)
			}
			frame, err := encodeFrame(snap, []byte("output"))
			if err != nil {
				t.Fatal(err)
			}
			decoded, _, err := decodeFrame(frame)
			if err != nil || !cmp.Equal(decoded.Remotes, snap.Remotes) {
				t.Fatalf("roundtrip=%+v error=%v", decoded.Remotes, err)
			}
			if first == nil {
				first = frame
			}
			if !bytes.Equal(frame, first) {
				t.Fatal("snapshot order depends on observation order")
			}
			rs.recordRemote(entries[3].Input, entries[3].Identity, []remoteInput{entries[1].Input})
			if diff := cmp.Diff(want, snap.Remotes); diff != "" {
				t.Fatalf("frozen snapshot mutated (-want +got):\n%s", diff)
			}
			if len(rs.snapshot().Remotes[0].Parents) != 3 {
				t.Fatal("additional parent was not recorded")
			}
		})
	}
}

func TestReadSet_RemoteValidation(t *testing.T) {
	for _, scenario := range []string{"stable-chain", "shared-parent-changes", "changed-leaf", "parent-failure", "canceled-parent", "independent-input", "descriptor-cycle", "missing-parent"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			var calls []string
			record := func(url string) {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, url)
			}
			var changed atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r.URL.Path)
				body := "remote data"
				if changed.Load() && (scenario == "changed-leaf" && r.URL.Path == "/a-child" || scenario == "independent-input" && r.URL.Path == "/0-independent") {
					body = "changed data"
				}
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(srv.Close)
			a := remoteInput{Operation: remoteGit, URL: "https://example.invalid/a.git", Ref: "main"}
			b := remoteInput{Operation: remoteGit, URL: "https://example.invalid/z.git", Ref: "main"}
			child := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/a-child"}
			independent := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/z-independent"}
			if scenario == "independent-input" || scenario == "descriptor-cycle" || scenario == "missing-parent" {
				independent.URL = srv.URL + "/0-independent"
			}
			repositories := map[string]map[string]string{
				a.URL: {"subdir/kustomization.yaml": "resources: ['" + b.URL + "//subdir?ref=main']\n", "leaf/kustomization.yaml": "resources: []\n"},
				b.URL: {"subdir/kustomization.yaml": "resources: ['" + a.URL + "//leaf?ref=main']\n"},
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			newCache := func() *TreeCache {
				c := newTestTreeCache(t, "")
				c.SetGitBaseFetcher(func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					record(request.URL)
					revision := "1"
					if changed.Load() {
						switch {
						case scenario == "shared-parent-changes" && request.URL == b.URL:
							revision = "2"
						case scenario == "parent-failure" && request.URL == a.URL:
							return nil, fs.ErrNotExist
						case scenario == "canceled-parent" && request.URL == a.URL:
							cancel()
							return nil, ctx.Err()
						}
					}
					return &store.SourceArtifact{FS: memoryTree(t, repositories[request.URL]), Key: request.URL, Revision: revision}, nil
				})
				return c
			}
			rec := newReadSet("/src")
			input := newTreeFS(memoryTree(t, map[string]string{"kustomization.yaml": "resources: ['" + independent.URL + "', '" + a.URL + "//subdir?ref=main']\n"}), "/src", rec)
			c := newCache()
			if scenario == "descriptor-cycle" {
				if err := preflightRemoteResources(t.Context(), c, input, "/src"); err != nil {
					t.Fatal(err)
				}
				want := map[remoteInput][]remoteInput{a: {b}, b: {a}, independent: nil}
				for _, entry := range rec.snapshot().Remotes {
					if diff := cmp.Diff(want[entry.Input], entry.Parents); diff != "" {
						t.Fatalf("finite subpath cycle parents (-want +got):\n%s", diff)
					}
				}
				if len(rec.remotes) != len(want) {
					t.Fatalf("cycle observations=%+v", rec.snapshot().Remotes)
				}
			} else {
				for _, descriptor := range []remoteInput{child, independent, b, a} {
					_, identity, err := descriptor.acquire(t.Context(), c, input)
					if err != nil {
						t.Fatal(err)
					}
					var parents []remoteInput
					switch descriptor {
					case child:
						parents = []remoteInput{b, a}
					case b:
						if scenario != "shared-parent-changes" {
							parents = []remoteInput{a}
						}
					}
					if scenario == "missing-parent" && descriptor == child {
						parents = append(parents, remoteInput{Operation: remoteGit, URL: "https://example.invalid/missing.git"})
					}
					rec.recordRemote(descriptor, identity, parents)
				}
			}
			frame, err := encodeFrame(rec.snapshot(), nil)
			if err != nil {
				t.Fatal(err)
			}
			snap, _, err := decodeFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			closeTestTreeCache(t, c)
			mu.Lock()
			calls = nil
			mu.Unlock()
			changed.Store(true)
			c = newCache()
			valid, err := snap.stillValid(ctx, c, input, true)
			if valid != (scenario == "stable-chain") || (err != nil) != (scenario == "canceled-parent") {
				t.Fatalf("validation=%t error=%v", valid, err)
			}
			if scenario == "canceled-parent" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			var want []string
			switch scenario {
			case "stable-chain":
				want = []string{a.URL, b.URL, "/a-child", "/z-independent"}
			case "shared-parent-changes":
				want = []string{a.URL, b.URL}
			case "changed-leaf":
				want = []string{a.URL, b.URL, "/a-child"}
			case "parent-failure", "canceled-parent":
				want = []string{a.URL}
			case "independent-input":
				want = []string{"/0-independent"}
			}
			mu.Lock()
			defer mu.Unlock()
			if diff := cmp.Diff(want, calls); diff != "" {
				t.Fatalf("acquisition order (-want +got):\n%s", diff)
			}
		})
	}
}

func validReadSet(t *testing.T, snap readSetSnapshot, lower fs.FS) bool {
	t.Helper()
	valid, err := snap.stillValid(t.Context(), nil, &treeFS{lower: lower}, false)
	if err != nil {
		t.Fatal(err)
	}
	return valid
}
