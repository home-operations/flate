package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/oci"
)

func sweepGit(t *testing.T, c *Cache, key string) (*GitGeneration, ocispec.Descriptor) {
	t.Helper()
	g, err := c.Git(t.Context(), GitKey{ID: key, RecurseSubmodules: true}, 0,
		func(storage.Storer, billy.Filesystem) (plumbing.Hash, error) { return plumbing.ZeroHash, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []billy.Filesystem{g.Backing, g.Worktree} {
		testutil.WriteFile(t, input.Root(), "marker", "retained")
	}
	desc, err := c.store.Resolve(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return g, desc
}

func sweepSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	if err := fs.WalkDir(os.DirFS(root), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		var data []byte
		if !d.IsDir() {
			data, err = os.ReadFile(filepath.Join(root, path))
			if err != nil {
				return err
			}
		}
		snapshot[path] = fmt.Sprintf("%s %d %s", info.Mode(), info.ModTime().UnixNano(), data)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSweep_GitGenerations(t *testing.T) {
	for _, scenario := range []string{"unheld", "held", "dry-run", "bad-index", "missing-payload", "bad-anchor", "bad-graph"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			c := newCache(t, root)
			first, _ := sweepGit(t, c, "source-git-sweep")
			second, desc := sweepGit(t, c, "source-git-sweep")
			if scenario == "bad-anchor" || scenario == "bad-graph" {
				if scenario == "bad-anchor" {
					desc.Annotations[prefix+"git-generation"] = "../outside"
				} else {
					desc.MediaType = ocispec.MediaTypeImageIndex
				}
				if err := c.store.Tag(t.Context(), desc, "source-git-sweep"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "bad-graph" {
				r, _, err := c.Blob(t.Context(), BlobKey{ID: "a-expired"}, 0, producer([]byte("stale"), ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				stale := time.Now().Add(-24 * time.Hour)
				if err := os.Chtimes(filepath.Join(c.layout.Root, blobPath(digest.FromString("stale"))), stale, stale); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "held" && scenario != "dry-run" {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "bad-index":
				testutil.WriteFile(t, c.layout.OCI(), "index.json", "{")
			case "missing-payload":
				if err := os.Remove(filepath.Join(c.layout.Root, blobPath(desc.Digest))); err != nil {
					t.Fatal(err)
				}
			}
			before := sweepSnapshot(t, root)
			res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour, DryRun: scenario == "dry-run"})
			fails := scenario != "unheld" && scenario != "dry-run"
			if fails {
				if err == nil || len(res.Removed) != 0 {
					t.Fatalf("unsafe sweep: %+v, %v", res, err)
				}
			} else {
				old := filepath.Dir(first.Backing.Root())
				if err != nil || len(res.Errors) != 0 || !slices.Equal(res.Removed, []string{old}) || res.Bytes != 16 {
					t.Fatalf("orphan not selected: %+v, %v", res, err)
				}
			}
			if scenario != "unheld" {
				if diff := cmp.Diff(before, sweepSnapshot(t, root)); diff != "" {
					t.Fatalf("sweep changed disk (-want +got):\n%s", diff)
				}
			} else if _, err := first.Backing.Stat("marker"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("orphan generation remains: %v", err)
			}
			for _, input := range []billy.Filesystem{second.Backing, second.Worktree} {
				if _, err := input.Stat("marker"); err != nil {
					t.Fatalf("reachable generation removed: %v", err)
				}
			}
		})
	}
}

func TestSweep_AgeUsesPayloadLastUse(t *testing.T) {
	for _, maxAge := range []time.Duration{0, time.Hour} {
		t.Run(maxAge.String(), func(t *testing.T) {
			root := t.TempDir()
			c := newCache(t, root)
			stale := time.Now().Add(-24 * time.Hour)
			var retired, generations []string
			for _, id := range []string{"source-git-stale", "git-base-stale", "source-git-touched", "git-base-touched"} {
				g, desc := sweepGit(t, c, id)
				if err := os.Chtimes(filepath.Join(c.layout.Root, blobPath(desc.Digest)), stale, stale); err != nil {
					t.Fatal(err)
				}
				if id == "source-git-touched" || id == "git-base-touched" {
					desc.Annotations[prefix+"resolved-at"] = stale.UTC().Format(time.RFC3339Nano)
					if err := c.store.Tag(t.Context(), desc, id); err != nil {
						t.Fatal(err)
					}
					if _, err := c.Git(t.Context(), GitKey{ID: id, RecurseSubmodules: true}, Forever, nil); err != nil {
						t.Fatal(err)
					}
				} else {
					retired = append(retired, id)
					generations = append(generations, filepath.Dir(g.Backing.Root()))
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: maxAge})
			if err != nil || len(res.Errors) > 0 {
				t.Fatalf("Sweep: %+v, %v", res, err)
			}
			var want []string
			if maxAge > 0 {
				want = append(retired, generations...)
				slices.Sort(want)
			}
			if !slices.Equal(res.Removed, want) {
				t.Fatalf("Removed = %v, want %v", res.Removed, want)
			}
			for _, path := range generations {
				_, err := os.Stat(path)
				if maxAge > 0 && !errors.Is(err, fs.ErrNotExist) || maxAge == 0 && err != nil {
					t.Fatalf("generation retirement: %v", err)
				}
			}
		})
	}
}

func TestSweep_SharedGraphRetainsAllGitAnchors(t *testing.T) {
	root := t.TempDir()
	c := newCache(t, root)
	var anchors []ocispec.Descriptor
	var generations []*GitGeneration
	for _, id := range []string{"source-git-shared", "git-base-shared"} {
		g, desc := sweepGit(t, c, id)
		generations = append(generations, g)
		anchors = append(anchors, desc)
		if err := c.store.Untag(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	var roots []ocispec.Descriptor
	for _, tag := range []string{"render-v1-expired", "render-v1-retained"} {
		data, err := json.Marshal(ocispec.Index{
			SchemaVersion: 2, Manifests: anchors,
			Annotations: map[string]string{"tag": tag},
		})
		if err != nil {
			t.Fatal(err)
		}
		desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)
		if err := c.store.Push(t.Context(), desc, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if err := c.store.Tag(t.Context(), desc, tag); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, desc)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-24 * time.Hour)
	for i, desc := range roots {
		if err := os.Chtimes(filepath.Join(c.layout.Root, blobPath(desc.Digest)), old, old); err != nil {
			t.Fatal(err)
		}
		before := sweepSnapshot(t, root)
		preview, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour, DryRun: true})
		if err != nil || len(preview.Errors) > 0 {
			t.Fatalf("shared dry-run: %+v, %v", preview, err)
		}
		if i == 0 && (!slices.Equal(preview.Removed, []string{"render-v1-expired"}) || preview.Bytes != 0) {
			t.Fatalf("retained shared graph: %+v", preview)
		}
		if diff := cmp.Diff(before, sweepSnapshot(t, root)); diff != "" {
			t.Fatalf("dry-run changed disk (-want +got):\n%s", diff)
		}
		res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour})
		if err != nil || len(res.Errors) > 0 {
			t.Fatalf("Sweep: %+v, %v", res, err)
		}
		if diff := cmp.Diff(preview, res); diff != "" {
			t.Fatalf("dry-run selection differs (-want +got):\n%s", diff)
		}
		for _, anchor := range anchors {
			_, err := os.Stat(filepath.Join(c.layout.Root, blobPath(anchor.Digest)))
			if i == 0 && err != nil || i == 1 && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("shared payload: %v", err)
			}
		}
		for _, g := range generations {
			_, err := g.Backing.Stat("marker")
			if i == 0 && err != nil || i == 1 && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("graph generation reachability: %v", err)
			}
		}
		if i == 1 && (len(res.Removed) != 3 || res.Bytes != 32) {
			t.Fatalf("unreachable graph generations: %+v", res)
		}
	}
}

func TestSweep_SharedIntermediateRetainsGitAnchor(t *testing.T) {
	root := t.TempDir()
	c := newCache(t, root)
	generation, anchor := sweepGit(t, c, "source-git-shared")
	if err := c.store.Untag(t.Context(), "source-git-shared"); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(ocispec.Index{SchemaVersion: 2, Manifests: []ocispec.Descriptor{anchor}})
	if err != nil {
		t.Fatal(err)
	}
	shared := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)
	if err := c.store.Push(t.Context(), shared, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	var stale ocispec.Descriptor
	for _, tag := range []string{"a-expired", "b-retained"} {
		data, err := json.Marshal(ocispec.Index{
			SchemaVersion: 2, Manifests: []ocispec.Descriptor{shared},
			Annotations: map[string]string{"tag": tag},
		})
		if err != nil {
			t.Fatal(err)
		}
		desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)
		if err := c.store.Push(t.Context(), desc, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if err := c.store.Tag(t.Context(), desc, tag); err != nil {
			t.Fatal(err)
		}
		if tag == "a-expired" {
			stale = desc
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(filepath.Join(root, blobPath(stale.Digest)), old, old); err != nil {
		t.Fatal(err)
	}
	res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour})
	if err != nil || len(res.Errors) != 0 || !slices.Equal(res.Removed, []string{"a-expired"}) || res.Bytes != 0 {
		t.Fatalf("retained intermediate graph: %+v, %v", res, err)
	}
	if _, err := generation.Backing.Stat("marker"); err != nil {
		t.Fatalf("retained generation: %v", err)
	}
}

func TestSweep_AbsentRootAndUnrelatedDirectories(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent")
	if res, err := Sweep(t.Context(), root, SweepOpts{DryRun: true}); err != nil || len(res.Removed) > 0 {
		t.Fatalf("absent dry-run: %+v, %v", res, err)
	}
	if _, err := os.Stat(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("dry-run created root: %v", err)
	}
	c := newCache(t, root)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"sources/repo/hash", "baselines/revision", "git-mirrors/repo", "render/helm/key"} {
		testutil.WriteFile(t, root, filepath.Join(dir, "marker"), "unrelated")
		old := time.Now().Add(-24 * time.Hour)
		if err := os.Chtimes(filepath.Join(root, dir), old, old); err != nil {
			t.Fatal(err)
		}
	}
	before := sweepSnapshot(t, root)
	if res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour}); err != nil || len(res.Removed) > 0 || len(res.Errors) > 0 {
		t.Fatalf("unrelated sweep: %+v, %v", res, err)
	}
	after := sweepSnapshot(t, root)
	for path, data := range before {
		if filepath.Dir(path) == "oci" || path == "oci" || path == "." {
			continue
		}
		if got := after[path]; got != data {
			t.Fatalf("unrelated entry %q changed", path)
		}
	}
	if _, err := oci.NewFromFS(t.Context(), os.DirFS(c.layout.OCI())); err != nil {
		t.Fatalf("swept store no longer readable: %v", err)
	}
}
