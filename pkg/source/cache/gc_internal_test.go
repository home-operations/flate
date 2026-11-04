package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/oci"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func sweepGit(t *testing.T, c *Cache, key string) (*GitGeneration, ocispec.Descriptor, string) {
	t.Helper()
	g, id, err := c.Git(t.Context(), GitKey{Content: key, RecurseSubmodules: true}, Policy{MaxAge: 0},
		func(context.Context,

			storage.Storer, billy.Filesystem,
		) (plumbing.Hash, error) {
			return plumbing.ZeroHash, nil
		})
	assert.NoError(t, err)

	for _, input := range []billy.Filesystem{g.Backing, g.Worktree} {
		testutil.WriteFile(t, input.Root(), "marker", "retained")
	}
	desc, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	return g, desc, id
}

func sweepSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	{
		err := fs.WalkDir(os.DirFS(root), ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			var data []byte
			if info.Mode()&os.ModeSymlink != 0 {
				var target string
				target, err = os.Readlink(filepath.Join(root, path))
				if err != nil {
					return err
				}
				data = []byte(target)
			} else if !d.IsDir() {
				data, err = os.ReadFile(filepath.Join(root, path))
				if err != nil {
					return err
				}
			}
			snapshot[path] = fmt.Sprintf("%s %d %s", info.Mode(), info.ModTime().UnixNano(), data)
			return nil
		})
		assert.NoError(t, err)
	}

	return snapshot
}

func TestSweep_GitGenerations(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"unheld", "held", "dry-run", "bad-index", "missing-payload", "bad-anchor", "bad-graph", "escaping-index", "escaping-index-dry-run", "escaping-payload", "escaping-payload-dry-run"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testSweepGitGenerationsCase(t, &scenario)
		},
		)
	}
	for _, component := range []string{"parent", "final"} {
		t.Run("selected-"+component+"-replacement", func(t *testing.T) {
			t.Parallel()
			testSweepGitGenerationsCase2(t, &component)
		},
		)
	}
}

func TestSweep_CoalescedAccessRetention(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "coalesced-access"}
		_, _, id, err := c.Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			producer([]byte("payload"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		v, _ := c.entries.Load(id)
		at := time.Now().Add(-500 * time.Millisecond)
		{
			chtimesErr := c.root.Chtimes(blobPath(v.(*entry).stored.Digest), at, at)
			assert.NoError(t, chtimesErr)
		}

		_, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, err)
		{
			err := c.Close()
			assert.NoError(t, err)
		}

		for _, expired := range []bool{false, true} {
			if expired {
				time.Sleep(2 * time.Second)
			}
			res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Nanosecond})
			assert.FatalIf(
				t,
				err != nil || len(res.Errors) != 0 || (len(res.Removed) == 1) != expired,
				"expired=%v: %+v, %v",
				expired,
				res,
				err,
			)
		}
	})
}

func TestSweep_AgeUsesPayloadLastUse(t *testing.T) {
	t.Parallel()
	for _, maxAge := range []time.Duration{0, time.Hour} {
		t.Run(maxAge.String(), func(t *testing.T) {
			t.Parallel()
			testSweepAgeUsesPayloadLastUseCase(t, &maxAge)
		},
		)
	}
}

func TestSweep_SharedGraphRetainsAllGitAnchors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	anchors := make([]ocispec.Descriptor, 0, 2)
	generations := make([]*GitGeneration, 0, 2)
	for _, id := range []string{"source-git-shared", "git-base-shared"} {
		g, desc, key := sweepGit(t, c, id)
		generations = append(generations, g)
		o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &desc)
		desc, err := c.publishOutcome(t.Context(), key, o, connReset())
		assert.NoError(t, err)

		anchors = append(anchors, desc)
		{
			err := c.store.Untag(t.Context(), key)
			assert.NoError(t, err)
		}
	}
	roots := make([]ocispec.Descriptor, 0, 2)
	for _, tag := range []string{"render-v1-expired", "render-v1-retained"} {
		data, err := json.Marshal(ocispec.Index{
			SchemaVersion: 2, Manifests: anchors,
			Annotations: map[string]string{"tag": tag},
		})
		assert.NoError(t, err)

		desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)
		{
			err := c.store.Push(t.Context(), desc, bytes.NewReader(data))
			assert.NoError(t, err)
		}
		{
			err := c.store.Tag(t.Context(), desc, tag)
			assert.NoError(t, err)
		}

		roots = append(roots, desc)
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	old := time.Now().Add(-24 * time.Hour)
	for i, desc := range roots {
		{
			err := os.Chtimes(filepath.Join(c.root.Name(), blobPath(desc.Digest)), old, old)
			assert.NoError(t, err)
		}

		before := sweepSnapshot(t, root)
		preview, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour, DryRun: true})
		assert.FatalIf(t, err != nil || len(preview.Errors) > 0, "shared dry-run: %+v, %v", preview, err)
		assert.FatalIf(
			t,
			i == 0 && (!slices.Equal(preview.Removed, []string{"render-v1-expired"}) || preview.Bytes != 0),
			"retained shared graph: %+v",
			preview,
		)
		{
			diff := cmp.Diff(before, sweepSnapshot(t, root))
			assert.FatalIf(t, diff != "", "dry-run changed disk (-want +got):\n%s", diff)
		}
		res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour})
		assert.FatalIf(t, err != nil || len(res.Errors) > 0, "Sweep: %+v, %v", res, err)
		{
			diff := cmp.Diff(preview, res)
			assert.FatalIf(t, diff != "", "dry-run selection differs (-want +got):\n%s", diff)
		}
		for _, anchor := range anchors {
			_, err := os.Stat(filepath.Join(c.root.Name(), blobPath(anchor.Digest)))
			assert.FatalIf(
				t,
				i == 0 && err != nil || i == 1 && !errors.Is(err, fs.ErrNotExist),
				"shared payload: %v",
				err,
			)
		}
		for _, g := range generations {
			_, err := g.Backing.Stat("marker")
			assert.FatalIf(
				t,
				i == 0 && err != nil || i == 1 && !errors.Is(err, fs.ErrNotExist),
				"graph generation reachability: %v",
				err,
			)
		}
		assert.FatalIf(
			t,
			i == 1 && (len(res.Removed) != 3 || res.Bytes != 32),
			"unreachable graph generations: %+v",
			res,
		)
	}
}

func TestSweep_SharedIntermediateRetainsGitAnchor(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	generation, anchor, key := sweepGit(t, c, "source-git-shared")
	{
		err := c.store.Untag(t.Context(), key)
		assert.NoError(t, err)
	}

	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &anchor)
	shared, err := c.publishOutcome(t.Context(), "shared-outcome", o, connReset())
	assert.NoError(t, err)
	{
		untagErr := c.store.Untag(t.Context(), "shared-outcome")
		assert.NoError(t, untagErr)
	}

	var stale ocispec.Descriptor
	for _, tag := range []string{"a-expired", "b-retained"} {
		data, marshalErr := json.Marshal(ocispec.Index{
			SchemaVersion: 2, Manifests: []ocispec.Descriptor{shared},
			Annotations: map[string]string{"tag": tag},
		})
		assert.NoError(t, marshalErr)

		desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)
		{
			pushErr := c.store.Push(t.Context(), desc, bytes.NewReader(data))
			assert.NoError(t, pushErr)
		}
		{
			tagErr := c.store.Tag(t.Context(), desc, tag)
			assert.NoError(t, tagErr)
		}

		if tag == "a-expired" {
			stale = desc
		}
	}
	{
		closeErr := c.Close()
		assert.NoError(t, closeErr)
	}

	old := time.Now().Add(-24 * time.Hour)
	{
		chtimesErr2 := os.Chtimes(filepath.Join(root, blobPath(stale.Digest)), old, old)
		assert.NoError(t, chtimesErr2)
	}

	res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: time.Hour})
	assert.FatalIf(
		t,
		err != nil || len(res.Errors) != 0 || !slices.Equal(res.Removed, []string{"a-expired"}) || res.Bytes != 0,
		"retained intermediate graph: %+v, %v",
		res,
		err,
	)
	{
		_, err := generation.Backing.Stat("marker")
		assert.FatalIf(t, err != nil, "retained generation: %v", err)
	}
}

func TestSweep_AbsentRootAndUnrelatedDirectories(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "absent")
	{
		res, err := Sweep(t.Context(), root, SweepOpts{DryRun: true})
		assert.FatalIf(t, err != nil || len(res.Removed) > 0, "absent dry-run: %+v, %v", res, err)
	}
	{
		_, err := os.Stat(root)
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "dry-run created root: %v", err)
	}
	c := cachetest.New(t, root, New)
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	for _, dir := range []string{"sources/repo/hash", "baselines/revision", "git-mirrors/repo", "render/helm/key"} {
		testutil.WriteFile(t, root, filepath.Join(dir, "marker"), "unrelated")
		old := time.Now().Add(-24 * time.Hour)
		{
			err := os.Chtimes(filepath.Join(root, dir), old, old)
			assert.NoError(t, err)
		}
	}
	before := sweepSnapshot(t, root)
	{
		res, err := Sweep(
			t.Context(),
			root,
			SweepOpts{MaxAge: time.Hour},
		)
		assert.FatalIf(t, err != nil || len(res.Removed) > 0 ||
			len(res.Errors) > 0, "unrelated sweep: %+v, %v", res, err)
	}
	after := sweepSnapshot(t, root)
	for path, data := range before {
		if filepath.Dir(path) == "oci" || path == "oci" || path == "." {
			continue
		}
		{
			got := after[path]
			assert.FatalIf(t, got != data, "unrelated entry %q changed", path)
		}
	}
	{
		_, err := oci.NewFromFS(t.Context(), os.DirFS(filepath.Join(c.root.Name(), ociDir)))
		assert.FatalIf(t, err != nil, "swept store no longer readable: %v", err)
	}
}

func testSweepGitGenerationsCase(t *testing.T, scenario *string,
) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	first, _, _ := sweepGit(t, c, "source-git-sweep")
	second, desc, key := sweepGit(t, c, "source-git-sweep")
	if (*scenario) == "bad-anchor" || (*scenario) == "bad-graph" {
		if (*scenario) == "bad-anchor" {
			desc.Annotations[prefix+"git-generation"] = "../outside"
		} else {
			desc.MediaType = ocispec.MediaTypeImageIndex
		}
		{
			err := c.store.Tag(t.Context(), desc, key)
			assert.NoError(t, err)
		}
	}
	if (*scenario) == "bad-graph" {
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "a-expired"},
			Policy{MaxAge: 0},
			producer([]byte("stale"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		stale := time.Now().Add(-24 * time.Hour)
		{
			err := os.Chtimes(
				filepath.Join(c.root.Name(), blobPath(digest.FromString("stale"))),
				stale,
				stale,
			)
			assert.NoError(t, err)
		}
	}
	if (*scenario) != "held" && (*scenario) != "dry-run" {
		{
			err := c.Close()
			assert.NoError(t, err)
		}
	}
	switch *scenario {
	case "bad-index":
		testutil.WriteFile(t, filepath.Join(c.root.Name(), ociDir), "index.json", "{")
	case "missing-payload":
		{
			err := os.Remove(filepath.Join(c.root.Name(), blobPath(desc.Digest)))
			assert.NoError(t, err)
		}
	}
	testSweepGitGenerationsCaseCompletion(
		t,
		&testSweepGitGenerationsCaseCompletionInputs{
			scenario: &scenario,
			c:        &c,
			root:     &root,
			desc:     &desc,
			first:    &first,
			second:   &second,
		},
	)
}

func testSweepGitGenerationsCaseCompletion(t *testing.T, captures *testSweepGitGenerationsCaseCompletionInputs) {
	t.Helper()
	outside := t.TempDir()
	if strings.HasPrefix((*(*captures.scenario)), "escaping-") {
		link := filepath.Join((*captures.c).root.Name(), indexPath())
		if strings.HasPrefix((*(*captures.scenario)), "escaping-payload") {
			link = filepath.Join((*captures.root), blobPath(captures.desc.Digest))
		}
		data, err := os.ReadFile(link)
		assert.NoError(t, err)

		testutil.WriteFile(t, outside, "target", string(data))
		{
			err := os.Rename(link, link+"-saved")
			assert.NoError(t, err)
		}
		{
			err := os.Symlink(filepath.Join(outside, "target"), link)
			assert.NoError(t, err)
		}
	}
	outsideBefore := sweepSnapshot(t, outside)
	before := sweepSnapshot(t, (*captures.root))
	res, err := Sweep(
		t.Context(), (*captures.root), SweepOpts{
			MaxAge: time.Hour,
			DryRun: (*(*captures.scenario)) == "dry-run" || strings.HasSuffix((*(*captures.scenario)), "-dry-run"),
		},
	)
	if strings.HasPrefix((*(*captures.scenario)), "escaping-") {
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux) ||
				len(res.Errors) != 0, "escape failed after selection: %+v, %v", res, err)
		}
		{
			_, ok := errors.AsType[*os.PathError](err)
			assert.FatalIf(t, !ok, "escape did not report a read failure: %v", err)
		}
	}
	{
		diff := cmp.Diff(outsideBefore, sweepSnapshot(t, outside))
		assert.FatalIf(t, diff != "", "outside changed (-want +got):\n%s", diff)
	}
	fails := (*(*captures.scenario)) != "unheld" && (*(*captures.scenario)) != "dry-run"
	if fails {
		assert.FatalIf(t, err == nil || len(res.Removed) != 0, "unsafe sweep: %+v, %v", res, err)
	} else {
		old := filepath.Dir((*captures.first).Backing.Root())
		assert.FatalIf(
			t,
			err != nil || len(res.Errors) != 0 || !slices.Equal(res.Removed, []string{old}) || res.Bytes != 16,
			"orphan not selected: %+v, %v",
			res,
			err,
		)
	}
	if (*(*captures.scenario)) != "unheld" {
		{
			diff := cmp.Diff(before, sweepSnapshot(t, (*captures.root)))
			assert.FatalIf(t, diff != "", "sweep changed disk (-want +got):\n%s", diff)
		}
	} else {
		_, err := (*captures.first).Backing.Stat("marker")
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "orphan generation remains: %v", err)
	}
	for _, input := range []billy.Filesystem{(*captures.second).Backing, (*captures.second).Worktree} {
		{
			_, err := input.Stat("marker")
			assert.FatalIf(t, err != nil, "reachable generation removed: %v", err)
		}
	}
}

func testSweepGitGenerationsCase2(t *testing.T, component *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	orphan, _, _ := sweepGit(t, c, "source-git-replacement")
	retained, _, _ := sweepGit(t, c, "source-git-replacement")
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	r, err := os.OpenRoot(c.root.Name())
	assert.NoError(t, err)

	defer func() { _ = r.Close() }()
	selection, err := selectSweep(t.Context(), r, 0)
	assert.NoError(t, err)

	path := generationPath(orphan.id)
	assert.FatalIf(t, !slices.Equal(selection.generations, []string{path}) ||
		!slices.Equal(selection.result.Removed, []string{filepath.Join(c.root.Name(), path)}) ||
		len(selection.tags) != 0, "selected relative orphan: %+v", selection)
	outside := t.TempDir()
	link, target := filepath.Join(c.root.Name(), generationPath("")), outside
	marker := filepath.Join(orphan.id, "marker")
	if (*component) == "final" {
		link = filepath.Join(c.root.Name(), path)
		marker = "marker"
	}
	testutil.WriteFile(t, outside, marker, "untouched")
	before := sweepSnapshot(t, outside)
	{
		err := os.Rename(link, link+"-saved")
		assert.NoError(t, err)
	}
	{
		err := os.Symlink(target, link)
		assert.NoError(t, err)
	}

	for _, name := range selection.generations {
		err := r.RemoveAll(name)
		assert.FatalIf(
			t, (*component) ==
				"parent" && err == nil || (*component) == "final" && err != nil, "remove selected %s: %v", (*component), err,
		)
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside))
		assert.FatalIf(t, diff != "", "outside changed (-want +got):\n%s", diff)
	}
	if (*component) == "parent" {
		{
			err := os.Remove(link)
			assert.NoError(t, err)
		}
	} else {
		_, err := os.Lstat(link)
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "selected final link remains: %v", err)
	}
	{
		err := os.Rename(link+"-saved", link)
		assert.NoError(t, err)
	}

	for _, input := range []billy.Filesystem{retained.Backing, retained.Worktree} {
		{
			_, err := input.Stat("marker")
			assert.FatalIf(t, err != nil, "retained generation removed: %v", err)
		}
	}
}

func testSweepAgeUsesPayloadLastUseCase(t *testing.T, maxAge *time.Duration,
) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	stale := time.Now().Add(-24 * time.Hour)
	var retired, generations []string
	for _, id := range []string{"source-git-stale", "git-base-stale", "source-git-touched", "git-base-touched"} {
		g, desc, key := sweepGit(t, c, id)
		{
			err := os.Chtimes(filepath.Join(c.root.Name(), blobPath(desc.Digest)), stale, stale)
			assert.NoError(t, err)
		}

		if id == "source-git-touched" || id == "git-base-touched" {
			desc.Annotations[prefix+"resolved-at"] = stale.UTC().Format(time.RFC3339Nano)
			{
				err := c.store.Tag(t.Context(), desc, key)
				assert.NoError(t, err)
			}
			{
				_, _, err := c.Git(
					t.Context(),
					GitKey{Content: id, RecurseSubmodules: true},
					Policy{MaxAge: Forever},
					nil,
				)
				assert.NoError(t, err)
			}
		} else {
			retired = append(retired, key)
			generations = append(generations, filepath.Dir(g.Backing.Root()))
		}
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	res, err := Sweep(t.Context(), root, SweepOpts{MaxAge: (*maxAge)})
	assert.FatalIf(t, err != nil || len(res.Errors) > 0, "Sweep: %+v, %v", res, err)
	var want []string
	if (*maxAge) > 0 {
		want = retired
		want = append(want, generations...)
		slices.Sort(want)
	}
	if !slices.Equal(res.Removed, want) {
		t.Fatalf("Removed = %v, want %v", res.Removed, want)
	}
	for _, path := range generations {
		_, err := os.Stat(path)
		assert.FatalIf(
			t, (*maxAge) >
				0 && !errors.Is(err, fs.ErrNotExist) || (*maxAge) == 0 && err != nil, "generation retirement: %v",
			err,
		)
	}
}

type testSweepGitGenerationsCaseCompletionInputs struct {
	scenario **string
	c        **Cache
	root     *string
	desc     *ocispec.Descriptor
	first    **GitGeneration
	second   **GitGeneration
}
