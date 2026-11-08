package bucket_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/bucket"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testFetcherArchiveIsolationAndWarmHitsCase(t *testing.T, attempts *int, backing *string) {
	t.Helper()
	var listings atomic.Int32
	sequences := map[string]*atomic.Int32{"first": {}, "second": {}, "canceled": {}}
	streaming, canceled := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherArchiveResponse(
			w,
			r,
			&testFetcherArchiveResponseInputs{
				canceled:  &canceled,
				listings:  &listings,
				sequences: &sequences,
				streaming: &streaming,
			},
		)
	}))
	t.Cleanup(srv.Close)
	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, cache.New)
	fInputs := testInputs{secrets: func(ns, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"accesskey": ns, "secretkey": "test"}}
	}}
	f := &bucket.Fetcher{Network: newTestNetwork(t), Cache: c}
	var wg sync.WaitGroup
	for _, namespace := range []string{"first", "second"} {
		wg.Go(func() {
			testFetcherArchiveAcquisition(
				&testFetcherArchiveAcquisitionInputs{
					attempts:  &attempts,
					f:         &f,
					fInputs:   &fInputs,
					namespace: &namespace,
					srv:       &srv,
					t:         &t,
				},
			)
		})
	}
	wg.Wait()
	want := int32(2 * min((*attempts), 2))
	if listings.Load() != want {
		t.Fatalf("producer sequences=%d, want %d", listings.Load(), want)
	}
	if (*attempts) == 1 {
		return
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		b := &manifest.Bucket{
			Name:       "source",
			Namespace:  "canceled",
			BucketName: "data",
			Prefix:     "prefix/",
			Endpoint:   srv.URL,
			Insecure:   true,
			Region:     "us-east-1",
			SecretRef:  &manifest.LocalObjectReference{Name: "creds"},
		}
		_, err := fetchtest.Fetch(ctx, f.Fetch, b, fInputs.readSet(b), &cache.Policy{
			MaxAge: b.SourceInterval(),
			Retry: cache.RetryConfig{
				Attempts: (*attempts),
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			},
		}, nil)
		done <- err
	}()
	select {
	case <-streaming:
	case err := <-done:
		t.Fatalf("fetch ended before cancellation: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fetch: %v", err)
	}
	<-canceled
	if sequences["canceled"].Load() != 1 {
		t.Fatalf("canceled producer sequences=%d, want 1", sequences["canceled"].Load())
	}
}

type testFetcherArchiveAcquisitionInputs struct {
	attempts  **int
	f         **bucket.Fetcher
	fInputs   *testInputs
	namespace *string
	srv       **httptest.Server
	t         **testing.T
}

func testFetcherArchiveAcquisition(captures *testFetcherArchiveAcquisitionInputs) {
	b := &manifest.Bucket{
		Name:       "source",
		Namespace:  (*captures.namespace),
		BucketName: "data",
		Prefix:     "prefix/",
		Provider:   sourcev1.BucketProviderAmazon,
		Endpoint:   (*captures.srv).URL,
		Insecure:   true,
		Region:     "us-east-1",
		Interval:   metav1.Duration{Duration: time.Hour},
		SecretRef:  &manifest.LocalObjectReference{Name: "creds"},
	}
	var family string
	for run := range 2 {
		key := manifest.WorkKey{
			Tree: manifest.TreeID{
				Cluster: "cluster",
				Side:    manifest.TreeHead,
			},
			Owner:  b.Named(),
			Source: b.Named(),
		}
		work := manifest.WorkRecorder{Key: key}
		art, err := fetchtest.Fetch(
			(*captures.t).Context(),
			(*captures.f).Fetch,
			b,
			captures.fInputs.readSet(b),
			&cache.Policy{
				MaxAge: b.SourceInterval(),
				Retry: cache.RetryConfig{
					Attempts: (*(*captures.attempts)),
					MinWait:  time.Nanosecond,
					MaxWait:  time.Nanosecond,
				},
				Work: &manifest.AcquisitionWork{Scope: (&work).Scope()},
			},
			nil,
		)
		func() {
			assertBucketAcquisitionWork(
				&assertBucketAcquisitionWorkInputs{b: &b, captures: &captures, key: &key, run: &run, work: &work},
			)
		}()
		if (*(*captures.attempts)) == 1 {
			if err == nil || art != nil {
				(*captures.t).Errorf("%s retry-off artifact=%v, error=%v", (*captures.namespace), art, err)
			}
			return
		}
		if err != nil {
			(*captures.t).Error(err)
			return
		}
		func() { assertBucketArchiveContent(&art, &captures, &family) }()
	}
}

type testFetcherArchiveResponseInputs struct {
	canceled  *chan struct{}
	listings  *atomic.Int32
	sequences *map[string]*atomic.Int32
	streaming *chan struct{}
}

func testFetcherArchiveResponse(w http.ResponseWriter, r *http.Request, captures *testFetcherArchiveResponseInputs) {
	namespace := "first"
	for _, name := range []string{"second", "canceled"} {
		if strings.Contains(r.Header.Get("Authorization"), "Credential="+name+"/") {
			namespace = name
		}
	}
	sequence := (*captures.sequences)[namespace]
	files := map[string]string{
		"dir/file.yaml": "first",
		"dir/drop.yaml": "drop",
		".sourceignore": "dir/drop.yaml\n",
		"image.png":     "image",
		".git/config":   "git",
	}
	if namespace == "second" {
		files["dir/file.yaml"] = "other"
	}
	listing := r.URL.Query().Get("list-type") != ""
	if listing {
		captures.listings.Add(1)
		sequence.Add(1)
	}
	if sequence.Load() == 1 {
		files["attempt-only.yaml"] = "discard"
	}
	if listing {
		names := []string{"dir/drop.yaml", "image.png", "dir/file.yaml", ".sourceignore", ".git/config"}
		if sequence.Load() == 1 {
			names = append(names, "attempt-only.yaml")
		}
		_, _ = fmt.Fprint(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>`)
		for _, name := range names {
			data := files[name]
			_, _ = fmt.Fprintf(
				w,
				`<Contents><Key>prefix/%s</Key><Size>%d</Size><ETag>"%x"</ETag></Contents>`,
				name,
				len(data),
				sha256.Sum256([]byte(data)),
			)
		}
		_, _ = fmt.Fprint(w, `</ListBucketResult>`)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/data/prefix/")
	data, ok := files[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("ETag", fmt.Sprintf(`"%x"`, sha256.Sum256([]byte(data))))
	w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	if name == "image.png" {
		if namespace == "canceled" {
			_, _ = fmt.Fprint(w, data[:2])
			w.(http.Flusher).Flush()
			close((*captures.streaming))
			<-r.Context().Done()
			close((*captures.canceled))
			return
		}
		if sequence.Load() == 1 {
			_, _ = fmt.Fprint(w, data[:2])
			return
		}
	}
	_, _ = fmt.Fprint(w, data)
}

type assertBucketAcquisitionWorkInputs struct {
	b        **manifest.Bucket
	captures **testFetcherArchiveAcquisitionInputs
	key      *manifest.WorkKey
	run      *int
	work     *manifest.WorkRecorder
}

func assertBucketAcquisitionWork(captures *assertBucketAcquisitionWorkInputs) {
	counts := map[string]manifest.WorkCounts{}
	{
		records := captures.work.Scope().Snapshot(0).Normalize()
		for recordIndex := range records {
			record := &records[recordIndex]
			assert.Equal((*(*captures.captures).t), record.Key.Tree, captures.key.Tree)
			assert.Equal((*(*captures.captures).t), record.Key.Owner, (*captures.b).Named())
			assert.Equal((*(*captures.captures).t), record.Key.Source, (*captures.b).Named())
			assert.Equal((*(*captures.captures).t), record.Key.Input != "", true)
			count := counts[string(record.Key.Phase)]
			count.Add(&record.Counts)
			counts[string(record.Key.Phase)] = count
		}
	}
	if (*captures.run) == 1 {
		assert.Diff((*(*captures.captures).t), counts, map[string]manifest.WorkCounts{
			string(manifest.WorkCache):       {Attempts: 1, Completed: 1, CacheService: 1},
			string(manifest.WorkPreparation): {RawKeyPrepares: 1},
		})
	} else {
		acquisition := counts[string(manifest.WorkAcquisition)]
		assert.Equal(
			(*(*captures.captures).t),
			acquisition.Attempts,
			uint64(min((*(*(*captures.captures).attempts)), 2)),
		)
		assert.Equal((*(*captures.captures).t), acquisition.Failures, uint64(1))
		assert.Equal(
			(*(*captures.captures).t),
			acquisition.Completed,
			uint64(min((*(*(*captures.captures).attempts)), 2)-1),
		)
		if acquisition.RemoteTransfers == 0 ||
			acquisition.RemoteTransfers > uint64(6+5*(min((*(*(*captures.captures).attempts)), 2)-1)) {
			(*(*captures.captures).t).Errorf(
				"%s cold transfer count=%d",
				(*(*captures.captures).namespace),
				acquisition.RemoteTransfers,
			)
		}
		assert.Equal((*(*captures.captures).t), counts[string(manifest.WorkCache)], manifest.WorkCounts{
			Attempts: uint64(
				min((*(*(*captures.captures).attempts)), 2),
			),
			Completed: uint64(min((*(*(*captures.captures).attempts)), 2) - 1),
			Failures:  1,
		})
		assert.Equal(
			(*(*captures.captures).t),
			counts[string(manifest.WorkPreparation)],
			manifest.WorkCounts{RawKeyPrepares: 1},
		)
	}
}

func assertBucketArchiveContent(
	art **store.SourceArtifact,
	captures **testFetcherArchiveAcquisitionInputs,
	family *string,
) {
	if (*art).Content == "" || (*art).Content != cache.AcquisitionIdentity((*art).FS) {
		(*(*captures).t).Errorf("content generation = %q", (*art).Content)
	}
	if (*art).Key == "" || (*family) != "" && (*art).Key != (*family) {
		(*(*captures).t).Errorf("warm acquisition family = %q, first = %q", (*art).Key, (*family))
	}
	(*family) = (*art).Key
	want := (*(*captures).namespace)
	if (*(*captures).namespace) == "second" {
		want = "other"
	}
	if data, err := fs.ReadFile((*art).FS, "dir/file.yaml"); err != nil || string(data) != want {
		(*(*captures).t).Errorf("%s content=%q err=%v", (*(*captures).namespace), data, err)
	}
	if _, err := fs.Stat((*art).FS, "attempt-only.yaml"); !errors.Is(err, fs.ErrNotExist) {
		(*(*captures).t).Errorf("failed acquisition leaked a file: %v", err)
	}
	if _, err := fs.Stat((*art).FS, "dir/drop.yaml"); !errors.Is(err, fs.ErrNotExist) {
		(*(*captures).t).Errorf("ignored file: %v", err)
	}
	if data, err := fs.ReadFile((*art).FS, "image.png"); err != nil || string(data) != "image" {
		(*(*captures).t).Errorf("complete image=%q, error=%v", data, err)
	}
	if data, err := fs.ReadFile((*art).FS, ".git/config"); err != nil || string(data) != "git" {
		(*(*captures).t).Errorf("bucket flavor excluded git content: %q %v", data, err)
	}
	if (*art).Metadata["objectCount"] != "5" || (*art).Revision == "" {
		(*(*captures).t).Errorf("bucket metadata: %v", (*art).Metadata)
	}
}
