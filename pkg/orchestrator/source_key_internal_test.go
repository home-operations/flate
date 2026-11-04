package orchestrator

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_SourceEffectiveWarmCache(t *testing.T) {
	const mediaType = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"
	layer := testutil.TarGz(t, map[string]string{
		"app-template/Chart.yaml":        "apiVersion: v2\nname: app-template\nversion: 4.4.0\n",
		"app-template/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: effective-output}\ndata: {value: accepted}\n",
	}, 0o644, gzip.DefaultCompression)
	descriptor := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(layer), Size: int64(len(layer))}
	image := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest, Layers: []ocispec.Descriptor{descriptor},
		SchemaVersion: 2,
	}
	body, err := json.Marshal(image)
	assert.NoError(t, err)
	var requests atomic.Int64
	var blocked atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if blocked.Load() {
			http.Error(w, "attributable app-template request blocked", http.StatusForbidden)
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
			_, _ = w.Write(body)
		case strings.HasSuffix(r.URL.Path, descriptor.Digest.String()):
			_, _ = w.Write(layer)
		default:
			http.NotFound(w, r)
		}
	}))
	testDAGSourceEffectiveWarmCacheTail(
		t,
		&testDAGSourceEffectiveWarmCacheTailInputs{
			server:    &server,
			mediaType: mediaType,
			requests:  &requests,
			blocked:   &blocked,
		},
	)
}

func testDAGSourceEffectiveWarmCacheTail(t *testing.T, tailCaseInput *testDAGSourceEffectiveWarmCacheTailInputs) {
	t.Helper()
	t.Cleanup((*tailCaseInput.server).Close)
	root := t.TempDir()
	effective := &manifest.OCIRepository{
		Name:          "app-template",
		Namespace:     "apps",
		URL:           "oci://" + strings.TrimPrefix((*tailCaseInput.server).URL, "https://") + "/app-template",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "4.4.0"},
		Interval:      &metav1.Duration{Duration: time.Hour},
		LayerSelector: &sourcev1.OCILayerSelector{MediaType: tailCaseInput.mediaType, Operation: sourcev1.OCILayerCopy},
	}
	warm, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)
	network := &source.Network{}
	t.Cleanup(network.Close)
	fetcher := &oci.Fetcher{Cache: warm, Network: network}
	warmStore := store.New()
	warmStore.AddObject(effective)
	var reads store.ReadSet
	reads.Init(warmStore, effective.Named(), nil)
	defer reads.Retire()
	artifact, err := fetcher.Fetch(
		t.Context(),
		effective,
		&reads,
		sourcecache.Policy{MaxAge: effective.Interval.Duration},
	)
	assert.NoError(t, err)
	before, err := fs.ReadFile(artifact.FS, source.OCICopiedLayerFilename)
	assert.NoError(t, err)
	assert.NoError(t, warm.Close())
	assert.FatalIf(
		t,
		tailCaseInput.requests.Swap(0) == 0,
		"%v",
		"cold control did not exercise the attributable endpoint",
	)
	tailCaseInput.blocked.
		Store(true)
	var attempts atomic.Int64
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{GetConn: func(string) { attempts.Add(1) }})
	var outputs [2][]byte
	for run := range 2 {
		dir := t.TempDir()
		o, localErr := newTestOrchestrator(t, Tree{Path: dir, RepoRoot: dir}, Config{CacheDir: root, Concurrency: 2})
		assert.NoError(t, localErr)
		t.Cleanup(func() { o.stop() })
		o.src.Fetchers[manifest.KindOCIRepository] = source.Wrap(
			manifest.KindOCIRepository,
			&oci.Fetcher{Cache: o.cache, Network: o.cfg.Network},
		)
		s := o.store
		parent := &manifest.Kustomization{Name: "materializer", Namespace: effective.Namespace}
		raw := *effective
		raw.Interval, raw.LayerSelector = &metav1.Duration{}, nil
		hr := &manifest.HelmRelease{
			Name:      "consumer",
			Namespace: effective.Namespace,
			ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: effective.Name},
		}

		s.AddObjects([]manifest.BaseManifest{parent, &raw, hr})
		s.UpdateStatus(
			parent.Named(),
			store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"},
		)
		o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			raw.Named(): {
				Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}},
			},
		}}
		s.SetDiscoveryIndex(o.index)
		o.configureControllers()
		o.src.StartLifecycle()
		o.hrc.StartLifecycle()
		parked := make(chan struct{})
		var once sync.Once
		d := dagDispatcher{o}
		dispatch := fixtureDispatcher{
			store: s,
			dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				if id == parent.Named() {
					o.tasks.YieldSlot(func() {
						select {
						case <-parked:
						case <-ctx.Done():
						}
					})
					assert.ErrorIf(
						t,
						attempts.Load() != 0 || tailCaseInput.requests.Load() != 0,
						"%v",
						"covered raw source attempted acquisition",
					)
					s.RecordEmitted(
						id,
						captureEmission(
							s,
							id,
							&store.Emission{Children: []store.Child{{Obj: effective}}, Complete: false},
						),
						nil,
					)
					return schedule.Result{}
				}
				out := d.Dispatch(ctx, id, level)
				if id == effective.Named() {
					once.Do(func() { close(parked) })
				}
				return out
			},
		}
		scheduler := schedule.New(o.tasks, dispatch)
		unwatch := o.watchDAG(scheduler)
		scheduler.Seed(o.seedNodes())
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		localErr = scheduler.Run(runCtx)
		cancel()
		unwatch()
		assert.NoError(t, localErr)
		art, _ := s.GetArtifact(effective.Named(), store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, art == nil, "%v", "accepted effective source did not acquire its warmed layer")
		acquired, localErr := fs.ReadFile(art.FS, source.OCICopiedLayerFilename)
		assert.NoError(t, localErr)
		assert.FatalIf(t, !bytes.Equal(before, acquired), "%v", "warm cache changed accepted layer bytes")
		docs := outputByParent(o.collectManifests())[hr.Named()]
		assert.FatalIf(t, len(docs) != 1, "accepted source did not render its chart: %v", docs)
		var stdout bytes.Buffer
		assert.NoError(t, (output.Encoder{Writer: &stdout, Format: output.FormatYAML}).Documents(docs))
		outputs[run] = stdout.Bytes()
		t.Logf("warm run=%d stdout=%q", run, outputs[run])
		if o.tasks.Failures() != 0 || len(s.FailedResources()) != 0 || attempts.Load() != 0 ||
			tailCaseInput.requests.Load() != 0 {
			t.Fatalf(
				"warm source: attempts=%d requests=%d failures=%v",
				attempts.Load(), tailCaseInput.requests.
					Load(), s.FailedResources(),
			)
		}
		o.stop()
	}
	if !bytes.Equal(outputs[0], outputs[1]) || !bytes.Contains(outputs[0], []byte("effective-output")) {
		t.Fatalf("warm blocked source changed stdout: %q %q", outputs[0], outputs[1])
	}
	t.Logf(
		"source=%s interval=%s layerSelector=%+v attempts=%d requests=%d stdout=%q",
		effective.Named(),
		effective.Interval.Duration,
		effective.LayerSelector,
		attempts.Load(), tailCaseInput.requests.
			Load(), outputs[0],
	)
	control := *effective
	control.Interval = &metav1.Duration{}
	owner := cachetest.New(t, root, sourcecache.New)
	_, err = (&oci.Fetcher{Cache: owner, Network: network}).Fetch(ctx, &control, &reads, sourcecache.Policy{})
	if err == nil || attempts.Load() == 0 || tailCaseInput.requests.Load() == 0 {
		t.Fatalf(
			"blocked control failed to detect a request: err=%v attempts=%d requests=%d",
			err,
			attempts.Load(), tailCaseInput.requests.
				Load(),
		)
	}
	t.Logf("blocked control attempts=%d requests=%d error=%v", attempts.Load(), tailCaseInput.requests.Load(), err)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAGDispatch_CoveredArrival(t *testing.T) {
	for _, order := range []string{"before parking", "after parking", "excluded then refire"} {
		for _, admission := range []string{"seed", "event"} {
			t.Run(order+"/"+admission, func(t *testing.T) {
				testDAGDispatchCoveredArrivalCase(
					t,
					testDAGDispatchCoveredArrivalCaseInputs{order: &order, admission: &admission},
				)
			},
			)
		}
	}
}

type testDAGDispatchCoveredArrivalCaseInputs struct {
	order     *string
	admission *string
}

func testDAGDispatchCoveredArrivalCase(t *testing.T, testCaseInput testDAGDispatchCoveredArrivalCaseInputs) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testCoveredArrivalBubble(t, testCaseInput)
	})
}

type testDAGSourceEffectiveWarmCacheTailInputs struct {
	server    **httptest.Server
	mediaType string
	requests  *atomic.Int64
	blocked   *atomic.Bool
}

func testCoveredArrivalBubble(t *testing.T, testCaseInput testDAGDispatchCoveredArrivalCaseInputs) {
	t.Helper()
	dir := t.TempDir()
	o, err := newTestOrchestrator(t, Tree{Path: dir, RepoRoot: dir}, Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	s := o.store
	parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps", URL: "oci://raw.example"}
	accepted := *raw
	accepted.URL = "oci://accepted.example"
	s.AddObject(parent)
	s.UpdateStatus(
		parent.Named(),
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"},
	)
	production := manifest.Production{Owner: parent.Named(), Source: manifest.BootstrapSourceID}
	if (*testCaseInput.order) == "excluded then refire" {
		production.Authored = raw.Named()
		production.Path = filepath.Join(dir, "unchanged.yaml")
		production.File = "unchanged.yaml"
	}
	o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		parent.Named(): {File: "changed.yaml"},
		raw.Named():    {File: "unchanged.yaml", Productions: []manifest.Production{production}},
	}}
	if (*testCaseInput.order) == "excluded then refire" {
		o.loader = admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet([]string{"changed.yaml"}),
				Index:     o.index,
				Inventory: s,
				Root:      "",
			},
		)
		admissiontest.Commit(t, o.loader, nil)
		o.filter = o.loader.Options.Filter
		assert.FatalIf(
			t,
			o.filter.ShouldReconcile(raw.Named()),
			"%v",
			"fixture already selects covered source",
		)
	}
	f := &sharedRepositoryFetcher{calls: map[manifest.NamedResource]int{}}
	o.src.Fetchers[manifest.KindOCIRepository] = f
	s.SetDiscoveryIndex(o.index)
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	testCaseInput.admit(s, raw, "seed")
	entered, published := make(chan struct{}), make(chan struct{})
	publishPermit, extendPermit := make(chan struct{}), make(chan struct{})
	excluded := make(chan struct{})
	var once sync.Once
	var dispatches atomic.Int64
	var initial schedule.Kind
	d := dagDispatcher{o}
	dispatch := fixtureDispatcher{
		store: s,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			if id == parent.Named() {
				if (*testCaseInput.order) == "excluded then refire" {
					once.Do(func() { close(entered) })
				}
				o.tasks.YieldSlot(func() {
					waitSharedRepositoryBarrier(ctx, publishPermit)
				})
				f.mu.Lock()
				assert.ErrorIf(
					t,
					f.calls[raw.Named()] != 0,
					"%v",
					"raw covered source acquired before publication",
				)
				f.mu.Unlock()
				if (*testCaseInput.order) == "excluded then refire" {
					info, exists := s.GetStatus(raw.Named())
					assert.ErrorIf(
						t,
						exists || dispatches.Load() != 0 || len(s.FailedResources()) != 0,
						"excluded source created work/failure: %+v",
						info,
					)
					s.RecordEmitted(
						id,
						captureEmission(
							s,
							id,
							&store.Emission{Children: []store.Child{{Obj: &accepted}}, Complete: false},
						),
						nil,
					)
					close(excluded)
					o.tasks.YieldSlot(func() {
						waitSharedRepositoryBarrier(ctx, extendPermit)
					})
					o.ksc.KeepEmitted(t.Context(), id, store.Child{Obj: &accepted}, nil)
				} else {
					s.RecordEmitted(
						id,
						captureEmission(
							s,
							id,
							&store.Emission{Children: []store.Child{{Obj: &accepted}}, Complete: false},
						),
						nil,
					)
				}
				close(published)
				return schedule.Result{}
			}
			call := dispatches.Add(1)
			out := d.Dispatch(ctx, id, level)
			if call == 1 {
				initial = out.Assessment.Outcome()
				signalCoveredArrival(testCaseInput, &once, entered)
			}

			if call == 1 && (*testCaseInput.order) == "before parking" {
				once.Do(func() { close(entered) })
				o.tasks.YieldSlot(func() { <-published })
			}
			return out
		},
	}
	scheduler := schedule.New(o.tasks, dispatch)
	unwatch := o.watchDAG(scheduler)
	defer unwatch()

	scheduler.Seed(o.seedNodes())
	testCaseInput.admit(s, raw, "event")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	await := func(signal <-chan struct{}) {
		t.Helper()
		awaitCoveredArrival(t, signal, done)
	}
	await(entered)
	synctest.Wait()
	close(publishPermit)
	if (*testCaseInput.order) == "excluded then refire" {
		await(excluded)
		synctest.Wait()
		close(extendPermit)
	}
	assert.NoError(t, <-done)
	want, wantDispatches := expectedCoveredArrival(testCaseInput)
	assert.FatalIf(t, initial != want, "initial outcome=%v want=%v", initial, want)
	f.mu.Lock()
	calls := f.calls[raw.Named()]
	f.mu.Unlock()
	if calls != 1 || o.tasks.Failures() != 0 || len(s.FailedResources()) != 0 ||
		dispatches.Load() != wantDispatches {
		t.Fatalf(
			"publication lost scheduler progress: fetches=%d dispatches=%d failures=%v",
			calls,
			dispatches.Load(),
			s.FailedResources(),
		)
	}
	info, _ := s.GetStatus(parent.Named())
	assert.FatalIf(t, info.Status == store.StatusReady, "%v", "child waited for its parent to settle")
	t.Logf(
		"%s/%s: dispatches=%d effective fetches=%d failures=%d",
		(*testCaseInput.order),
		(*testCaseInput.admission),
		dispatches.Load(),
		calls,
		len(s.FailedResources()),
	)
}

func awaitCoveredArrival(t *testing.T, signal <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-signal:
	case err := <-done:
		t.Fatalf("scheduler stopped before publication barrier: %v", err)
	}
}

func signalCoveredArrival(
	testCaseInput testDAGDispatchCoveredArrivalCaseInputs,
	once *sync.Once,
	entered chan struct{},
) {
	if (*testCaseInput.order) != "before parking" {
		once.Do(func() { close(entered) })
	}
}

func expectedCoveredArrival(testCaseInput testDAGDispatchCoveredArrivalCaseInputs) (schedule.Kind, int64) {
	want := schedule.Blocked
	wantDispatches := int64(2)
	if (*testCaseInput.order) == "excluded then refire" {
		want = schedule.Terminal
		wantDispatches = 1
	}
	return want, wantDispatches
}

func (in testDAGDispatchCoveredArrivalCaseInputs) admit(s *store.Store, raw *manifest.OCIRepository, phase string) {
	if (*in.admission) == phase {
		s.AddObject(raw)
	}
}
