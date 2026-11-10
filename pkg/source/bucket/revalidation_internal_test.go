package bucket

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type revalidationObject struct{ key, etag, body string }

type revalidationListing struct {
	objects  []revalidationObject
	pageSize int
	failPage int
}

type revalidationBucket struct {
	listing               atomic.Pointer[revalidationListing]
	lists, probes, bodies atomic.Int32
}

func (b *revalidationBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	listing := b.listing.Load()
	if r.URL.Query().Has("list-type") {
		b.lists.Add(1)
		page, _ := strconv.Atoi(r.URL.Query().Get("continuation-token"))
		if listing.failPage == page {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>listing denied</Message></Error>`)
			return
		}
		start, end := 0, len(listing.objects)
		if listing.pageSize > 0 {
			start = page * listing.pageSize
			end = min(start+listing.pageSize, end)
		}
		_, _ = fmt.Fprintf(
			w,
			`<ListBucketResult><Name>data</Name><IsTruncated>%t</IsTruncated>`,
			end < len(listing.objects),
		)
		if end < len(listing.objects) {
			_, _ = fmt.Fprintf(w, `<NextContinuationToken>%d</NextContinuationToken>`, page+1)
		}
		for _, object := range listing.objects[start:end] {
			_, _ = fmt.Fprintf(
				w,
				`<Contents><Key>%s</Key><ETag>%s</ETag><Size>%d</Size></Contents>`,
				object.key,
				object.etag,
				len(object.body),
			)
		}
		_, _ = io.WriteString(w, `</ListBucketResult>`)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/data/")
	index := slices.IndexFunc(listing.objects, func(object revalidationObject) bool { return object.key == key })
	if index < 0 {
		http.NotFound(w, r)
		return
	}
	object := listing.objects[index]
	condition := ""
	if object.etag != "" {
		condition = strconv.Quote(object.etag)
		w.Header().Set("ETag", condition)
	}
	if r.Header.Get("If-Match") != condition {
		http.Error(w, "wrong ETag condition", http.StatusPreconditionFailed)
		return
	}
	if r.Header.Get("Range") == "bytes=0-0" {
		b.probes.Add(1)
	} else {
		b.bodies.Add(1)
	}
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	http.ServeContent(w, r, object.key, time.Unix(0, 0), strings.NewReader(object.body))
}

func TestFetcher_Revalidation(t *testing.T) {
	t.Parallel()
	a := revalidationObject{"a.yaml", "a1", "kind: ConfigMap\nmetadata:\n  name: a\n"}
	b := revalidationObject{"b.yaml", "b1", "kind: ConfigMap\nmetadata:\n  name: b\n"}
	missing := revalidationObject{"a.yaml", "", a.body}
	large := revalidationObject{"large", "large1", strings.Repeat("x", int(chunkBytes)+1)}
	for _, tc := range []struct {
		name                            string
		initial, updated                []revalidationObject
		pageSize, failPage              int
		missingPayload                  bool
		wantLists, wantBodies           int32
		changedRevision, changedContent bool
	}{
		{name: "unchanged", initial: []revalidationObject{b, a}, updated: []revalidationObject{a, b, {key: "directory/"}}, failPage: -1, wantLists: 1},
		{name: "added", initial: []revalidationObject{a}, updated: []revalidationObject{a, b}, failPage: -1, wantLists: 1, wantBodies: 2, changedRevision: true, changedContent: true},
		{name: "removed", initial: []revalidationObject{a, b}, updated: []revalidationObject{b}, failPage: -1, wantLists: 1, wantBodies: 1, changedRevision: true, changedContent: true},
		{name: "changed_etag", initial: []revalidationObject{a}, updated: []revalidationObject{{a.key, "a2", "changed bytes"}}, failPage: -1, wantLists: 1, wantBodies: 1, changedRevision: true, changedContent: true},
		{name: "missing_etag", initial: []revalidationObject{missing}, updated: []revalidationObject{missing}, failPage: -1, wantLists: 1, wantBodies: 1},
		{name: "empty", failPage: -1, wantLists: 1},
		{name: "paginated", initial: []revalidationObject{a, b}, updated: []revalidationObject{b, a}, pageSize: 1, failPage: -1, wantLists: 2},
		{name: "partial_listing", initial: []revalidationObject{a, b}, updated: []revalidationObject{a, b}, pageSize: 1, failPage: 1, wantLists: 2},
		{name: "failed_listing", initial: []revalidationObject{a}, updated: []revalidationObject{a}, failPage: 0, wantLists: 1},
		{name: "unsafe_marker", initial: []revalidationObject{a}, updated: []revalidationObject{a, {key: "../"}}, failPage: -1, wantLists: 1},
		{name: "unusable_previous", initial: []revalidationObject{a, b}, updated: []revalidationObject{a, b}, missingPayload: true, failPage: -1, wantLists: 1, wantBodies: 2},
		{name: "large_unchanged", initial: []revalidationObject{large}, updated: []revalidationObject{large}, failPage: -1, wantLists: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			remote := new(revalidationBucket)
			remote.listing.Store(&revalidationListing{objects: tc.initial, pageSize: tc.pageSize, failPage: -1})
			server := httptest.NewServer(remote)
			t.Cleanup(server.Close)
			root := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
			object := &manifest.Bucket{
				Name:       "revalidation",
				Namespace:  "test",
				BucketName: "data",
				Endpoint:   server.URL,
				Insecure:   true,
				Region:     "us-east-1",
			}
			reads := (testInputs{}).readSet(object)
			t.Cleanup(reads.Close)
			first, err := fetchtest.Fetch(t.Context(), f.Fetch, object, reads, &cache.Policy{MaxAge: 0}, nil)
			assert.NoError(t, err)
			assertRevalidationArtifact(t, first, &tc.initial)
			assert.Equal(t, remote.lists.Load(), max(int32(1), int32(tc.pageSize)*int32(len(tc.initial))))
			if tc.name == "large_unchanged" {
				assert.Equal(t, remote.probes.Load(), int32(1))
				assert.Equal(t, remote.bodies.Load(), int32(2))
			} else {
				assert.Equal(t, remote.probes.Load(), int32(0))
				assert.Equal(t, remote.bodies.Load(), int32(len(tc.initial)))
			}
			if tc.missingPayload {
				blobs, globErr := filepath.Glob(filepath.Join(root, "oci", "blobs", "sha256", "*"))
				assert.NoError(t, globErr)
				assert.FatalIf(t, len(blobs) != 1, "cache payloads: %v", blobs)
				assert.NoError(t, os.Remove(blobs[0]))
			}
			lists, probes, bodies := remote.lists.Load(), remote.probes.Load(), remote.bodies.Load()
			remote.listing.Store(
				&revalidationListing{objects: tc.updated, pageSize: tc.pageSize, failPage: tc.failPage},
			)
			second, err := fetchtest.Fetch(t.Context(), f.Fetch, object, reads, &cache.Policy{MaxAge: 0}, nil)
			assert.Equal(t, remote.lists.Load()-lists, tc.wantLists)
			assert.Equal(t, remote.probes.Load()-probes, int32(0))
			assert.Equal(t, remote.bodies.Load()-bodies, tc.wantBodies)
			if tc.failPage >= 0 || tc.name == "unsafe_marker" {
				assert.FatalIf(
					t,
					err == nil || second != nil,
					"incomplete or invalid listing reused content: %v, %v",
					second,
					err,
				)
				return
			}
			assert.NoError(t, err)
			assertRevalidationArtifact(t, second, &tc.updated)
			assert.Equal(t, second.Key, first.Key)
			assert.Equal(t, second.Revision != first.Revision, tc.changedRevision)
			assert.Equal(t, second.Content != first.Content, tc.changedContent)
		})
	}
}

func assertRevalidationArtifact(t *testing.T, artifact *store.SourceArtifact, objects *[]revalidationObject) {
	t.Helper()
	assert.FatalIf(
		t,
		artifact == nil || artifact.FS == nil || artifact.Content == "" || artifact.Key == "",
		"incomplete artifact: %+v",
		artifact,
	)
	want := make(map[string]string)
	for _, object := range *objects {
		if !strings.HasSuffix(object.key, "/") {
			want[object.key] = object.body
		}
	}
	got := make(map[string]string)
	err := fs.WalkDir(artifact.FS, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			data, readErr := fs.ReadFile(artifact.FS, name)
			if readErr != nil {
				return readErr
			}
			got[name] = string(data)
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Diff(t, got, want)
	assert.Diff(t, artifact.Metadata, map[string]string{"objectCount": strconv.Itoa(len(want))})
	assert.Equal(t, artifact.Kind, manifest.KindBucket)
	assert.FatalIf(t, artifact.Revision == "", "%s", "missing bucket revision")
}
