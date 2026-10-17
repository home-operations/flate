package kustomize

import (
	"bytes"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"
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
		if !validReadSet(t, snap, base()) {
			t.Error("identical tree must validate (cache hit)")
		}
	})

	t.Run("editing a recorded file misses", func(t *testing.T) {
		d := base()
		d["a.yaml"] = &fstest.MapFile{Data: []byte("kind: ConfigMap\ndata: {x: y}\n")}
		if validReadSet(t, snap, d) {
			t.Error("an edit to a recorded input must invalidate (cache miss)")
		}
	})

	t.Run("editing an UNREAD file stays a hit (granularity)", func(t *testing.T) {
		d := base()
		d["b.yaml"] = &fstest.MapFile{Data: []byte("kind: Secret\nstringData: {k: v}\n")}
		if !validReadSet(t, snap, d) {
			t.Error("b.yaml was never read by this build; editing it must NOT invalidate")
		}
	})

	t.Run("adding a file to the listed dir misses", func(t *testing.T) {
		d := base()
		d["c.yaml"] = &fstest.MapFile{Data: []byte("kind: Service\n")}
		if validReadSet(t, snap, d) {
			t.Error("a new file in the auto-scanned dir must invalidate")
		}
	})

	t.Run("deleting a recorded file misses", func(t *testing.T) {
		d := base()
		delete(d, "a.yaml")
		if validReadSet(t, snap, d) {
			t.Error("deleting a recorded input must invalidate")
		}
	})

	t.Run("a probed-absent component appearing misses", func(t *testing.T) {
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
				rs.recordRemote(entry.Input, entry.Identity)
				rs.recordRemote(entry.Input, entry.Identity)
			}
			snap := rs.snapshot()
			if len(snap.Remotes) != len(entries) {
				t.Fatalf("deduplicated observations=%v", snap.Remotes)
			}
			frame, err := encodeFrame(snap, []byte("output"))
			if err != nil {
				t.Fatal(err)
			}
			decoded, _, err := decodeFrame(frame)
			if err != nil || !slices.Equal(decoded.Remotes, snap.Remotes) {
				t.Fatalf("roundtrip=%+v error=%v", decoded.Remotes, err)
			}
			if first == nil {
				first = frame
			}
			if !bytes.Equal(frame, first) {
				t.Fatal("snapshot order depends on observation order")
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
