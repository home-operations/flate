package bucket_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	"github.com/home-operations/flate/pkg/source/cache"
)

func newTestNetwork(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}

func TestFetcher_ProviderMissingSecret(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &bucket.Fetcher{Network: newTestNetwork(t)}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval()})
	assert.FatalIf(t, err == nil, "expected missing-secret error")
	assert.ErrorIf(
		t,
		!errors.Is(err, manifest.ErrMissingSecret),
		"want ErrMissingSecret from the generic path; got %v",
		err,
	)
}

func TestFetcher_ArchiveIsolationAndWarmHits(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, attempts := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/attempts-%d", backing, attempts), func(t *testing.T) {
				t.Parallel()
				testFetcherArchiveIsolationAndWarmHitsCase(
					t,
					&backing, &attempts,
				)
			},
			)
		}
	}
}

func TestFetcher_TransientStatusRecovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status        int
		code, message string
	}{
		{http.StatusTooManyRequests, "SlowDown", "temporary failure"},
		{http.StatusServiceUnavailable, "SlowDown", "temporary failure"},
		{http.StatusForbidden, "AccessDenied", "access denied"},
	} {
		t.Run(fmt.Sprintf("status-%d", tc.status), func(t *testing.T) {
			t.Parallel()
			testFetcherTransientStatusRecoveryCase(t, &tc)
		},
		)
	}
}

func TestFetcher_TimeoutRecovery(t *testing.T) {
	t.Parallel()
	for _, reopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen=%t", reopen), func(t *testing.T) {
			t.Parallel()
			testFetcherTimeoutRecoveryCase(t, &reopen)
		},
		)
	}
}

func TestFetcher_TerminalStatusReplay(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()

			testFetcherTerminalStatusReplayCase(t, &status)
		},
		)
	}
}

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"list", "object"} {
		t.Run(
			route,
			func(t *testing.T) {
				testFetcherNetworkRoutesCase(t, &route)
			},
		)
	}
}

func testFetcherArchiveIsolationAndWarmHitsCase(
	t *testing.T, backing *string,
	attempts *int,
) {
	t.Helper()

	var listings atomic.Int32
	sequences := map[string]*atomic.Int32{"first": {}, "second": {}, "canceled": {}}
	streaming, canceled := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherArchiveIsolationAndWarmHitsCaseCallback(
			w,
			r,
			testFetcherArchiveIsolationAndWarmHitsCaseCallbackInputs{
				listings:  &listings,
				sequences: &sequences,
				streaming: &streaming,
				canceled:  &canceled,
			},
		)
	},
	))
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
			testFetcherArchiveIsolationAndWarmHitsCaseCallback2(
				testFetcherArchiveIsolationAndWarmHitsCaseCallback2Inputs{
					t:         &t,
					attempts:  &attempts,
					srv:       &srv,
					fInputs:   &fInputs,
					f:         &f,
					namespace: &namespace,
				},
			)
		},
		)
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
		_, err := f.Fetch(
			ctx,
			b,
			fInputs.readSet(b),
			cache.Policy{
				MaxAge: b.SourceInterval(),
				Retry: cache.RetryConfig{
					Attempts: (*attempts),
					MinWait:  time.Nanosecond,
					MaxWait:  time.Nanosecond,
				},
			},
		)
		done <- err
	}()
	select {
	case <-streaming:
	case err := <-done:
		t.Fatalf("fetch ended before cancellation: %v", err)
	}
	cancel()
	{
		err := <-done
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled fetch: %v", err)
	}
	<-canceled
	if sequences["canceled"].Load() != 1 {
		t.Fatalf("canceled producer sequences=%d, want 1", sequences["canceled"].Load())
	}
}

func testFetcherTransientStatusRecoveryCase(t *testing.T, tc *struct {
	status  int
	code    string
	message string
},
) {
	t.Helper()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if calls.Add(1) == 1 {
			w.WriteHeader(tc.status)
			_, _ = fmt.Fprintf(
				w,
				"<Error><Code>%s</Code><Message>%s</Message></Error>", tc.
					code, tc.
					message,
			)
			return
		}
		_, _ = io.WriteString(
			w,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
		)
	}))
	t.Cleanup(srv.Close)
	c := cachetest.New(t, "", cache.New)
	f := &bucket.Fetcher{Network: newTestNetwork(t), Cache: c}
	_, err := f.Fetch(t.Context(), &manifest.Bucket{
		Name: "retry", Namespace: "test", BucketName: "data",
		Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
	}, (testInputs{}).readSet(&manifest.Bucket{
		Name: "retry", Namespace: "test", BucketName: "data",
		Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
	}), cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond}})
	if tc.status == http.StatusForbidden {
		cause, ok := errors.AsType[minio.ErrorResponse](err)
		response, typed := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || !typed || response.StatusCode != tc.status || response.Op != http.MethodGet ||
			!errors.Is(response.Err, cause) ||
			cause.StatusCode != tc.status ||
			cause.Code != tc.code ||
			cause.Message != tc.message ||
			!errors.Is(err, manifest.ErrMissingSecret) ||
			err.Error() != "machine credential acquisition failed" ||
			calls.Load() != 1 {
			t.Fatalf("permanent failure: %v, cause=%+v, requests=%d", err, cause, calls.Load())
		}
		return
	}
	if err != nil || calls.Load() != 2 {
		t.Fatalf("recovery: %v, requests=%d; want success after 2 requests", err, calls.Load())
	}
}

func testFetcherTimeoutRecoveryCase(t *testing.T, reopen *bool,
) {
	t.Helper()

	var blocked atomic.Bool
	blocked.Store(true)
	var listings, objects atomic.Int32
	bodyStarted, bodyCanceled := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			listings.Add(1)
			_, _ = io.WriteString(
				w,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>hello.txt</Key><Size>2</Size><ETag>"a"</ETag></Contents></ListBucketResult>`,
			)
			return
		}
		objects.Add(1)
		w.Header().Set("Content-Length", "2")
		w.Header().Set("ETag", `"a"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		_, _ = io.WriteString(w, "h")
		if blocked.Load() {
			w.(http.Flusher).Flush()
			close(bodyStarted)
			<-r.Context().Done()
			close(bodyCanceled)
			return
		}
		_, _ = io.WriteString(w, "i")
	}))
	t.Cleanup(srv.Close)
	root := t.TempDir()
	f := &bucket.Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
	b := &manifest.Bucket{
		Name: "timeout", Namespace: "test", BucketName: "data", Endpoint: srv.URL,
		Insecure: true, Region: "us-east-1", Timeout: &metav1.Duration{Duration: time.Second},
	}
	policy := cache.Policy{MaxAge: time.Hour}
	artifact, err := f.Fetch(t.Context(), b, (testInputs{}).readSet(b), policy)
	assert.FatalIf(
		t,
		!errors.Is(err, context.DeadlineExceeded) || artifact != nil,
		"body timeout: artifact=%v error=%v",
		artifact,
		err,
	)
	select {
	case <-bodyStarted:
	default:
		t.Fatal("source timed out before the object body was blocked")
	}
	<-bodyCanceled
	if *reopen {
		{
			closeErr := f.Cache.Close()
			assert.NoError(t, closeErr)
		}

		f.Cache = cachetest.New(t, root, cache.New)
	}
	blocked.Store(false)
	longer := *b
	longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
	artifact, err = f.Fetch(t.Context(), &longer, (testInputs{}).readSet(&longer), policy)
	assert.FatalIf(t, err != nil, "longer policy reused timeout: %v", err)
	{
		data, err := fs.ReadFile(artifact.FS, "hello.txt")
		assert.FatalIf(t, err != nil || string(data) != "hi", "recovered content=%q error=%v", data, err)
	}
	if listings.Load() != 2 || objects.Load() != 2 {
		t.Fatalf("recovery listings=%d objects=%d, want 2 each", listings.Load(), objects.Load())
	}
}

func testFetcherTerminalStatusReplayCase(t *testing.T, status *int,
) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader((*status))
		_, _ = io.WriteString(
			w,
			`<Error><Code>SlowDown</Code><Message>temporary failure fixture-secret</Message></Error>`,
		)
	}))
	t.Cleanup(srv.Close)
	root := t.TempDir()
	c, err := cache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	f := &bucket.Fetcher{Cache: c, Network: newTestNetwork(t)}
	b := &manifest.Bucket{
		Name: "terminal", Namespace: "test", BucketName: "data",
		Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
	}
	retry := cache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond}
	_, err = f.Fetch(
		t.Context(),
		b,
		(testInputs{}).readSet(b),
		cache.Policy{MaxAge: b.SourceInterval(), Retry: retry},
	)
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	cause, typed := errors.AsType[*fetcherr.ResponseError](err)
	if !ok || !terminal.Attempted || !typed || cause.StatusCode != (*status) ||
		cause.Op != http.MethodGet ||
		cause.Error() != "temporary failure fixture-secret" ||
		calls.Load() != 3 {
		t.Fatalf("terminal failure: %v, requests=%d", err, calls.Load())
	}
	{
		closeErr2 := c.Close()
		assert.NoError(t, closeErr2)
	}

	readRecord := func() (ocispec.Descriptor, []byte) {
		t.Helper()
		data, readFileErr := os.ReadFile(filepath.Join(root, "oci", "index.json"))
		assert.NoError(t, readFileErr)

		var index ocispec.Index
		{
			unmarshalErr := json.Unmarshal(data, &index)
			assert.FatalIf(
				t,
				unmarshalErr != nil || len(index.Manifests) != 1,
				"persisted outcome index: %s, %v",
				data,
				unmarshalErr,
			)
		}
		desc := index.Manifests[0]
		data, readFileErr = os.ReadFile(
			filepath.Join(root, "oci", "blobs", desc.Digest.Algorithm().String(), desc.Digest.Encoded()),
		)
		assert.NoError(t, readFileErr)

		var m ocispec.Manifest
		{
			unmarshalErr2 := json.Unmarshal(data, &m)
			assert.FatalIf(
				t,
				unmarshalErr2 != nil || m.ArtifactType != "application/vnd.flate.failure.v1",
				"persisted outcome manifest: %s, %v",
				data,
				unmarshalErr2,
			)
		}
		data, readFileErr = os.ReadFile(
			filepath.Join(
				root,
				"oci",
				"blobs",
				m.Config.Digest.Algorithm().String(),
				m.Config.Digest.Encoded(),
			),
		)
		assert.NoError(t, readFileErr)

		return desc, data
	}
	initial, record := readRecord()
	assert.FatalIf(t, strings.Contains(string(record), "fixture-secret") ||
		strings.Contains(string(record), "temporary failure"), "persisted upstream diagnostic: %s", record)
	var timing struct {
		Completed time.Time `json:"completed"`
		Count     uint32    `json:"count"`
	}
	{
		unmarshalErr3 := json.Unmarshal(record, &timing)
		assert.FatalIf(
			t,
			unmarshalErr3 != nil || timing.Completed.IsZero() || timing.Count != 1,
			"persisted timing: %s, %v",
			record,
			unmarshalErr3,
		)
	}
	deadline := timing.Completed.Add(750 * time.Millisecond)
	reopened, err := cache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = reopened.Close() })
	f.Cache = reopened
	_, err = f.Fetch(
		t.Context(),
		b,
		(testInputs{}).readSet(b),
		cache.Policy{MaxAge: b.SourceInterval(), Retry: retry},
	)
	terminal, ok = errors.AsType[*cache.TerminalError](err)
	cause, typed = errors.AsType[*fetcherr.ResponseError](err)
	if !ok || terminal.Attempted || !typed || cause.StatusCode != (*status) ||
		cause.Op != http.MethodGet ||
		cause.Err != nil ||
		cause.Message != "" ||
		calls.Load() != 3 {
		t.Fatalf("replayed failure: %v, requests=%d", err, calls.Load())
	}
	assert.FatalIf(t, !time.Now().Before(deadline), "%s", "replay did not finish before the recorded deadline")

	unchanged, replayed := readRecord()
	assert.FatalIf(t, unchanged.Digest != initial.Digest ||
		unchanged.Annotations[ocispec.AnnotationRefName] != initial.Annotations[ocispec.AnnotationRefName] ||
		string(
			replayed,
		) != string(
			record,
		), "replay changed persisted record or deadline: %s -> %s", record, replayed)
}

func testFetcherNetworkRoutesCase(t *testing.T, route *string,
) {
	t.Helper()
	var network source.Network
	var lists, objects, dials atomic.Int32
	var cause error
	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			dials.Add(1)
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	var target string
	latch := func() error {
		failing.Store(true)
		tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
		if err != nil {
			return err
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		_, err = (&http.Client{Transport: tr}).Do(req)
		failing.Store(false)
		terminal, ok := errors.AsType[*cache.TerminalError](err)
		if !ok || !terminal.Attempted {
			return fmt.Errorf("seed: %w", err)
		}
		cause = terminal.Err
		return nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "" {
			objects.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		lists.Add(1)
		if err := latch(); err != nil {
			t.Error(err)
		}
		_, _ = io.WriteString(
			w,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>app.yaml</Key><Size>1</Size><ETag>"a"</ETag></Contents></ListBucketResult>`,
		)
	}))
	t.Cleanup(srv.Close)
	target = strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	c := cachetest.New(t, "", cache.New)
	f := &bucket.Fetcher{Cache: c, Network: &network}
	if (*route) == "list" {
		{
			err := latch()
			assert.NoError(t, err)
		}
	}
	_, err := f.Fetch(
		t.Context(),
		&manifest.Bucket{
			Name:       "network",
			Namespace:  "test",
			BucketName: "data",
			Endpoint:   target,
			Insecure:   true,
			Region:     "us-east-1",
		},
		(testInputs{}).readSet(
			&manifest.Bucket{
				Name:       "network",
				Namespace:  "test",
				BucketName: "data",
				Endpoint:   target,
				Insecure:   true,
				Region:     "us-east-1",
			},
		),
		cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Hour}},
	)
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	wantLists := int32(0)
	if (*route) == "object" {
		wantLists = 1
	}
	if !ok || terminal.Attempted || !errors.Is(err, cause) || dials.Load() == 0 || lists.Load() != wantLists ||
		objects.Load() != 0 {
		t.Fatalf(
			"error=%v terminal=%+v dials=%d lists=%d objects=%d",
			err,
			terminal,
			dials.Load(),
			lists.Load(),
			objects.Load(),
		)
	}
}

type testFetcherArchiveIsolationAndWarmHitsCaseCallbackInputs struct {
	listings  *atomic.Int32
	sequences *map[string]*atomic.Int32
	streaming *chan struct{}
	canceled  *chan struct{}
}

func testFetcherArchiveIsolationAndWarmHitsCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherArchiveIsolationAndWarmHitsCaseCallbackInputs,
) {
	namespace := "first"
	for _, name := range []string{"second", "canceled"} {
		if strings.Contains(r.Header.Get("Authorization"), "Credential="+name+"/") {
			namespace = name
		}
	}
	sequence := (*testCaseInput.sequences)[namespace]
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
		testCaseInput.listings.
			Add(1)
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
			close((*testCaseInput.streaming))
			<-r.Context().Done()
			close((*testCaseInput.canceled))
			return
		}
		if sequence.Load() == 1 {
			_, _ = fmt.Fprint(w, data[:2])
			return
		}
	}
	_, _ = fmt.Fprint(w, data)
}

type testFetcherArchiveIsolationAndWarmHitsCaseCallback2Inputs struct {
	t         **testing.T
	attempts  **int
	srv       **httptest.Server
	fInputs   *testInputs
	f         **bucket.Fetcher
	namespace *string
}

func testFetcherArchiveIsolationAndWarmHitsCaseCallback2(
	testCaseInput testFetcherArchiveIsolationAndWarmHitsCaseCallback2Inputs,
) {
	b := &manifest.Bucket{
		Name:       "source",
		Namespace:  (*testCaseInput.namespace),
		BucketName: "data",
		Prefix:     "prefix/",
		Provider:   sourcev1.BucketProviderAmazon,
		Endpoint:   (*testCaseInput.srv).URL,
		Insecure:   true,
		Region:     "us-east-1",
		Interval:   metav1.Duration{Duration: time.Hour},
		SecretRef:  &manifest.LocalObjectReference{Name: "creds"},
	}
	var family string
	for range 2 {
		art, err := (*testCaseInput.f).Fetch((*testCaseInput.t).
			Context(), b, testCaseInput.fInputs.
			readSet(b), cache.Policy{
			MaxAge: b.SourceInterval(),
			Retry: cache.RetryConfig{
				Attempts: (*(*testCaseInput.attempts)),
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			},
		},
		)
		if (*(*testCaseInput.attempts)) == 1 {
			assert.ErrorIf((*testCaseInput.t), err == nil || art != nil,
				"%s retry-off artifact=%v, error=%v", (*testCaseInput.namespace), art,
				err,
			)
			return
		}
		if err != nil {
			(*testCaseInput.t).
				Error(err)
			return
		}
		assert.ErrorIf((*testCaseInput.t), art.Content == "" || art.Content != cache.AcquisitionIdentity(art.FS),
			"content generation = %q", art.Content)
		assert.ErrorIf((*testCaseInput.t), art.Key == "" || family != "" && art.Key != family,
			"warm acquisition family = %q, first = %q", art.Key, family)
		family = art.Key
		want := (*testCaseInput.namespace)
		if (*testCaseInput.namespace) == "second" {
			want = "other"
		}
		{
			data, err := fs.ReadFile(art.FS, "dir/file.yaml")
			assert.ErrorIf((*testCaseInput.t), err != nil || string(data) != want,
				"%s content=%q err=%v", (*testCaseInput.namespace), data,
				err,
			)
		}
		{
			_, err := fs.Stat(art.FS, "attempt-only.yaml")
			assert.ErrorIf((*testCaseInput.t), !errors.Is(err, fs.ErrNotExist),
				"failed acquisition leaked a file: %v",
				err,
			)
		}
		{
			_, err := fs.Stat(art.FS, "dir/drop.yaml")
			assert.ErrorIf((*testCaseInput.t), !errors.Is(err, fs.ErrNotExist), "ignored file: %v", err)
		}
		{
			data, err := fs.ReadFile(art.FS, "image.png")
			assert.ErrorIf((*testCaseInput.t), err != nil || string(data) != "image",
				"complete image=%q, error=%v",
				data,
				err,
			)
		}
		{
			data, err := fs.ReadFile(art.FS, ".git/config")
			assert.ErrorIf((*testCaseInput.t), err != nil || string(data) != "git",
				"bucket flavor excluded git content: %q %v",
				data,
				err,
			)
		}
		assert.ErrorIf((*testCaseInput.t), art.Metadata["objectCount"] != "5" || art.Revision == "",
			"bucket metadata: %v", art.Metadata)
	}
}
