package helmchart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetchIndex_WithoutTemporaryStorage(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		for _, restored := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restored=%t", backing, restored), func(t *testing.T) {
				testFetchIndexWithoutTemporaryStorageCase(
					t,
					&backing, &restored,
				)
			},
			)
		}
	}
}

//nolint:paralleltest // The test changes the process logging destination.
//nolint:paralleltest // The test changes process logging state.
func TestFetchIndex_Diagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		invalid    bool
	}{
		{"malformed", "apiVersion: [", true},
		{"partial", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n  - version: 2.0.0\n  - null\n", false},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { testFetchIndexDiagnosticsCase(t, &tc) },
		)
	}
}

func TestLoadIndex_FluxSemantics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		err        error
	}{
		{"entries", `apiVersion: v1
entries:
  chart:
    - name: chart
      version: 1.0.0
      urls: [chart.tgz]
    - version: 2.0.0
    - name: chart
      version: invalid
    - null
    - name: chart
      version: 3.0.0
      dependencies:
        - name: dependency
          version: 1.0.0
        - name: dependency
          version: 1.0.0
`, nil},
		{"json", `{"apiVersion":"v1","entries":{"chart":[{"name":"chart","version":"1.0.0"},null]}}`, nil},
		{"empty", "", repo.ErrEmptyIndexYaml},
		{"no api version", "entries: {}\n", repo.ErrNoAPIVersion},
		{"ordinary metadata", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    apiVersion: v2\n    appVersion: app-v1\n    version: 1.0.0\n    urls: [chart.tgz]\n", nil},
		{"scalar coercion", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    digest: 1234\n    urls: [yes]\n", nil},
		{"case insensitive fields", "APIVERSION: v1\nentries: {}\n", nil},
		{"unknown field", "apiVersion: v1\nunknown: true\nentries: {}\n", nil},
		{"duplicate fields", "apiVersion: v1\napiVersion: v2\nentries: {}\n", nil},
		{"empty and null entries", "apiVersion: v1\nentries:\n  chart: [null, {}]\n", nil},
		{"numeric annotation", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    annotations: {number: 1234}\n", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "index.yaml", tc.data)
			want, wantErr := repo.LoadIndexFile(filepath.Join(root, "index.yaml"))
			f := New(nil, Config{Concurrency: 256})
			got, _, gotErr := f.loadIndex(
				t.Context(),
				"https://charts.test/index.yaml",
				func(w io.Writer) (ocispec.Descriptor, error) {
					_, err := io.WriteString(w, tc.data)
					return ocispec.Descriptor{Digest: digest.FromString(tc.data), Size: int64(len(tc.data))}, err
				},
			)
			if (wantErr == nil) != (gotErr == nil) ||
				tc.err != nil && (!errors.Is(wantErr, tc.err) || !errors.Is(gotErr, tc.err)) {
				t.Fatalf("upstream=%v FS=%v, want %v", wantErr, gotErr, tc.err)
			}
			if wantErr == nil {
				{
					diff := cmp.Diff(want, got)
					assert.FatalIf(t, diff != "", "index (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFetchIndex_DecodeBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		restored, canceled bool
	}{
		{"download", false, false},
		{"restored", true, false},
		{"canceled download", false, true},
		{"canceled restored", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetchIndexDecodeBudgetCase(t, &tc)
		},
		)
	}
}

func testFetchIndexWithoutTemporaryStorageCase(
	t *testing.T, backing *string,
	restored *bool,
) {
	t.Helper()
	root := t.TempDir()
	cacheRoot := ""
	if (*backing) == "disk" {
		cacheRoot = filepath.Join(root, "cache")
	}
	owner := cachetest.New(t, cacheRoot, sourcecache.New)
	cfg := Config{Cache: owner, Concurrency: 512}
	f := New(nil, cfg)
	r := &httpRepository{
		HelmRepository: httpRepo("https://charts.test/owner"),
		client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(helmRepoIndex(""))),
			}, nil
		})},
	}
	if *restored {
		{
			_, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{})
			assert.NoError(t, err)
		}

		f = New(nil, cfg)
	}
	testutil.WriteFile(t, root, "blocked-tmp", "not a directory")
	t.Setenv("TMPDIR", filepath.Join(root, "blocked-tmp"))
	idx, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{})
	assert.NoError(t, err)
	{
		_, err := idx.Get("app-template", "1.0.0")
		assert.NoError(t, err)
	}
}

func testFetchIndexDiagnosticsCase(t *testing.T, tc *struct {
	name    string
	data    string
	invalid bool
},
) {
	t.Helper()
	var previous string
	for _, restored := range []bool{false, true} {
		t.Run(fmt.Sprintf("restored=%t", restored), func(t *testing.T) {
			testFetchIndexDiagnosticsCaseCase(
				t,
				testFetchIndexDiagnosticsCaseCaseInputs{tc: &tc, previous: &previous, restored: &restored},
			)
		},
		)
	}
}

func testFetchIndexDecodeBudgetCase(t *testing.T, tc *struct {
	name     string
	restored bool
	canceled bool
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testFetchIndexDecodeBudgetCaseCallback(t, testFetchIndexDecodeBudgetCaseCallbackInputs{tc: &tc})
	},
	)
}

type testFetchIndexDiagnosticsCaseCaseInputs struct {
	tc **struct {
		name string
		data string

		invalid bool
	}
	previous *string
	restored *bool
}

func testFetchIndexDiagnosticsCaseCase(t *testing.T, testCaseInput testFetchIndexDiagnosticsCaseCaseInputs) {
	t.Helper()
	for range 2 {
		owner := cachetest.New(t, t.TempDir(), sourcecache.New)
		r := &httpRepository{
			HelmRepository: httpRepo("https://charts.test/diagnostics"),
			client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader((*testCaseInput.tc).data)),
				}, nil
			})},
		}
		if *testCaseInput.restored {
			key := sourcecache.BlobKey{
				Content: struct{ Repository, Transport any }{r.Content(), r.material},
			}
			_, _, _, err := owner.Blob(
				t.Context(),
				key,
				sourcecache.Policy{MaxAge: sourcecache.Forever},
				func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					_, err := io.WriteString(w, (*testCaseInput.tc).data)
					return sourcecache.ProducerResult{
						Digest: digest.FromString((*testCaseInput.tc).data),
						Size:   int64(len((*testCaseInput.tc).data)),
					}, err
				},
			)
			assert.NoError(t, err)
		}
		var output bytes.Buffer
		saved := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
		idx, err := New(
			nil,
			Config{Cache: owner, Concurrency: 512},
		).fetchIndex(t.Context(), r, sourcecache.Policy{})
		slog.SetDefault(saved)
		if (*testCaseInput.tc).invalid {
			assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux) ||
				!strings.Contains(err.Error(), "parse https://charts.test/diagnostics/index.yaml") ||
				strings.Contains(err.Error(), "flate-index-"), "parse diagnostic: %v", err)
			assert.FatalIf(
				t, (*testCaseInput.previous) !=
					"" && err.Error() != (*testCaseInput.previous), "unstable parse diagnostic: %q, previously %q",
				err, (*testCaseInput.previous),
			)
			(*testCaseInput.previous) = err.Error()
		} else {
			assert.FatalIf(
				t,
				err != nil || len(idx.Entries["chart"]) != 1,
				"partial index: %v, %v",
				idx,
				err,
			)
		}
		if output.Len() != 0 {
			t.Fatalf("index loader emitted diagnostics: %s", output.String())
		}
	}
}

type testFetchIndexDecodeBudgetCaseCallbackInputs struct {
	tc **struct {
		name     string
		restored bool
		canceled bool
	}
}

func testFetchIndexDecodeBudgetCaseCallback(t *testing.T, testCaseInput testFetchIndexDecodeBudgetCaseCallbackInputs) {
	t.Helper()
	owner := cachetest.New(t, "", sourcecache.New)
	cfg := Config{Cache: owner, Concurrency: 256}
	f := New(nil, cfg)
	data := map[string]string{
		"/one/index.yaml": helmRepoIndex(""),
		"/two/index.yaml": strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0"),
	}
	var calls atomic.Int32
	transfer := make(chan struct{})
	client := &http.Client{Transport: chartTransport(func(req *http.Request) (*http.Response, error) {
		body, index := data[req.URL.Path]
		if index {
			calls.Add(1)
			if !(*testCaseInput.tc).restored {
				select {
				case <-transfer:
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
		} else {
			body = "chart"
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	repositories := []*httpRepository{
		{HelmRepository: httpRepo("https://charts.test/one"), client: client},
		{HelmRepository: httpRepo("https://charts.test/two"), client: client},
	}
	if (*testCaseInput.tc).restored {
		for _, r := range repositories {
			{
				_, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{})
				assert.NoError(t, err)
			}
		}
		f = New(nil, cfg)
	}
	calls.Store(0)
	if cap(f.indexDecodes) != 2 {
		t.Fatalf("decode budget = %d, want 2 from 256 task slots", cap(f.indexDecodes))
	}
	for range cap(f.indexDecodes) {
		f.indexDecodes <- struct{}{}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, len(repositories))
	for i, r := range repositories {
		go func() {
			_, err := f.fetchHTTPChart(
				ctx,
				r,
				helmChart(r.Name, "app-template", fmt.Sprintf("%d.0.0", i+1)),
				sourcecache.Policy{},
			)
			done <- err
		}()
	}
	testFetchIndexDecodeBudgetCaseCallbackCompletion(
		t,
		&testFetchIndexDecodeBudgetCaseCallbackCompletionInputs{
			done:          &done,
			calls:         &calls,
			testCaseInput: &testCaseInput,
			cancel:        &cancel,
			repositories:  &repositories,
			data:          &data,
			f:             &f,
			transfer:      &transfer,
		},
	)
}

func testFetchIndexDecodeBudgetCaseCallbackCompletion(
	t *testing.T,
	captures *testFetchIndexDecodeBudgetCaseCallbackCompletionInputs,
) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-(*captures.done):
		t.Fatalf("index decoded while all permits were borrowed: %v", err)
	default:
	}
	assert.FatalIf(
		t,
		captures.calls.Load() != 0,
		"%s",
		"waiting indexes acquired raw responses outside the decode budget",
	)

	if (*captures.testCaseInput.tc).canceled {
		(*captures.cancel)()
		synctest.Wait()
		for range *captures.repositories {
			{
				err := <-(*captures.done)
				assert.FatalIf(t, !errors.Is(err, context.Canceled), "blocked decode cancellation: %v", err)
			}
		}
		for _, body := range *captures.data {
			{
				_, ok := (*captures.f).indexCache.Load("sha256:" + chartDigest([]byte(body)))
				assert.FatalIf(t, ok, "%s", "canceled decode published an index")
			}
		}
	}
	for range cap((*captures.f).indexDecodes) {
		<-(*captures.f).indexDecodes
	}
	if (*captures.testCaseInput.tc).canceled {
		close((*captures.transfer))
		for i, r := range *captures.repositories {
			{
				_, err := (*captures.f).fetchHTTPChart(
					t.Context(),
					r,
					helmChart(r.Name, "app-template", fmt.Sprintf("%d.0.0", i+1)),
					sourcecache.Policy{},
				)
				assert.NoError(t, err)
			}
		}
	} else {
		synctest.Wait()
		if !(*captures.testCaseInput.tc).restored &&
			(captures.calls.Load() != 2 || len((*captures.f).indexDecodes) != 2) {
			t.Errorf(
				"two permits did not admit two indexes: calls=%d permits=%d", captures.calls.
					Load(), len((*captures.f).indexDecodes),
			)
		}
		close((*captures.transfer))
		for range *captures.repositories {
			{
				err := <-(*captures.done)
				assert.NoError(t, err)
			}
		}
	}
	assert.FatalIf(t, len((*captures.f).indexDecodes) != 0, "%s", "decode did not return its borrowed permit")
}

type testFetchIndexDecodeBudgetCaseCallbackCompletionInputs struct {
	done          *chan error
	calls         *atomic.Int32
	testCaseInput *testFetchIndexDecodeBudgetCaseCallbackInputs
	cancel        *context.CancelFunc
	repositories  *[]*httpRepository
	data          *map[string]string
	f             **Fetcher
	transfer      *chan struct{}
}
