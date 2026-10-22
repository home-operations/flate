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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDAG_SourceEffectiveWarmCache(t *testing.T) {
	t.Parallel()
	const mediaType = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"
	layer := testutil.TarGz(t, map[string]string{
		"app-template/Chart.yaml":        "apiVersion: v2\nname: app-template\nversion: 4.4.0\n",
		"app-template/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: effective-output}\ndata: {value: accepted}\n",
	}, 0o644, gzip.DefaultCompression)
	descriptor := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(layer), Size: int64(len(layer))}
	image := ocispec.Manifest{MediaType: ocispec.MediaTypeImageManifest, Layers: []ocispec.Descriptor{descriptor},
		SchemaVersion: 2}
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
	t.Cleanup(server.Close)
	root := t.TempDir()
	effective := &manifest.OCIRepository{
		Name: "app-template", Namespace: "apps", URL: "oci://" + strings.TrimPrefix(server.URL, "https://") + "/app-template",
		Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Tag: "4.4.0"},
		Interval:      metav1.Duration{Duration: time.Hour},
		LayerSelector: &sourcev1.OCILayerSelector{MediaType: mediaType, Operation: sourcev1.OCILayerCopy},
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
	artifact, err := fetcher.Fetch(t.Context(), effective, &reads, sourcecache.Policy{MaxAge: effective.Interval.Duration})
	assert.NoError(t, err)
	before, err := fs.ReadFile(artifact.FS, source.OCICopiedLayerFilename)
	assert.NoError(t, err)
	assert.NoError(t, warm.Close())
	if requests.Swap(0) == 0 {
		t.Fatal("cold control did not exercise the attributable endpoint")
	}
	blocked.Store(true)
	var attempts atomic.Int64
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{GetConn: func(string) { attempts.Add(1) }})
	var outputs [2][]byte
	for run := range 2 {
		dir := t.TempDir()
		o, err := New(ctx, Config{Path: dir, RepoRoot: dir, CacheDir: root, Concurrency: 2})
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, o.Stop()) })
		o.WithFetcher(manifest.KindOCIRepository, source.Wrap(manifest.KindOCIRepository, &oci.Fetcher{Cache: o.cache, Network: o.cfg.Network}))
		s := o.store
		parent := &manifest.Kustomization{Name: "materializer", Namespace: effective.Namespace}
		raw := *effective
		raw.Interval, raw.LayerSelector = metav1.Duration{}, nil
		hr := &manifest.HelmRelease{Name: "consumer", Namespace: effective.Namespace, Chart: manifest.HelmChart{
			Name: "app-template", Source: effective.Named(),
		}}
		s.AddObjects([]manifest.BaseManifest{parent, &raw, hr})
		s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"})
		o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			raw.Named(): {Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}},
		}}
		s.SetDiscoveryIndex(o.index)
		o.configureControllers()
		o.src.StartLifecycle()
		o.hrc.StartLifecycle()
		parked := make(chan struct{})
		var once sync.Once
		d := dagDispatcher{o}
		dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			if id == parent.Named() {
				o.tasks.YieldSlot(func() {
					select {
					case <-parked:
					case <-ctx.Done():
					}
				})
				if attempts.Load() != 0 || requests.Load() != 0 {
					t.Error("covered raw source attempted acquisition")
				}
				s.RecordEmitted(id, captureEmission(s, id, &store.Emission{Children: []store.Child{{Obj: effective}}, Complete: false}), nil)
				return schedule.Result{}
			}
			out := d.Dispatch(ctx, id, level)
			if id == effective.Named() {
				once.Do(func() { close(parked) })
			}
			return out
		}}
		scheduler := schedule.New(o.tasks, dispatch)
		unwatch := o.watchDAG(scheduler)
		scheduler.Seed(o.seedNodes())
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = scheduler.Run(runCtx)
		cancel()
		unwatch()
		assert.NoError(t, err)
		art, _ := s.GetArtifact(effective.Named()).(*store.SourceArtifact)
		if art == nil {
			t.Fatal("accepted effective source did not acquire its warmed layer")
		}
		acquired, err := fs.ReadFile(art.FS, source.OCICopiedLayerFilename)
		assert.NoError(t, err)
		if !bytes.Equal(before, acquired) {
			t.Fatal("warm cache changed accepted layer bytes")
		}
		docs := outputByParent(o.collectManifests())[hr.Named()]
		if len(docs) != 1 {
			t.Fatalf("accepted source did not render its chart: %v", docs)
		}
		var stdout bytes.Buffer
		assert.NoError(t, (output.Encoder{Writer: &stdout, Format: output.FormatYAML}).Documents(docs))
		outputs[run] = stdout.Bytes()
		t.Logf("warm run=%d stdout=%q", run, outputs[run])
		if o.tasks.Failures() != 0 || len(s.FailedResources()) != 0 || attempts.Load() != 0 || requests.Load() != 0 {
			t.Fatalf("warm source: attempts=%d requests=%d failures=%v", attempts.Load(), requests.Load(), s.FailedResources())
		}
		assert.NoError(t, o.Stop())
	}
	if !bytes.Equal(outputs[0], outputs[1]) || !bytes.Contains(outputs[0], []byte("effective-output")) {
		t.Fatalf("warm blocked source changed stdout: %q %q", outputs[0], outputs[1])
	}
	t.Logf("source=%s interval=%s layerSelector=%+v attempts=%d requests=%d stdout=%q", effective.Named(), effective.Interval.Duration, effective.LayerSelector, attempts.Load(), requests.Load(), outputs[0])
	control := *effective
	control.Interval = metav1.Duration{}
	owner := cachetest.New(t, root, sourcecache.New)
	_, err = (&oci.Fetcher{Cache: owner, Network: network}).Fetch(ctx, &control, &reads, sourcecache.Policy{})
	if err == nil || attempts.Load() == 0 || requests.Load() == 0 {
		t.Fatalf("blocked control failed to detect a request: err=%v attempts=%d requests=%d", err, attempts.Load(), requests.Load())
	}
	t.Logf("blocked control attempts=%d requests=%d error=%v", attempts.Load(), requests.Load(), err)
}

func TestDAGDispatch_CoveredArrival(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"before parking", "after parking", "excluded then refire"} {
		for _, admission := range []string{"seed", "event"} {
			t.Run(order+"/"+admission, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					dir := t.TempDir()
					o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, Concurrency: 2})
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, o.Stop()) })
					s := o.store
					parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
					raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps", URL: "oci://raw.example"}
					accepted := *raw
					accepted.URL = "oci://accepted.example"
					s.AddObject(parent)
					s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"})
					o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						parent.Named(): {File: "changed.yaml"},
						raw.Named():    {File: "unchanged.yaml", Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}},
					}}
					if order == "excluded then refire" {
						o.filter = change.NewFilter(change.NewSet([]string{"changed.yaml"}), o.index, s)
						if o.filter.ShouldReconcile(raw.Named()) {
							t.Fatal("fixture already selects covered source")
						}
					}
					f := &sharedRepositoryFetcher{calls: map[manifest.NamedResource]int{}}
					o.WithFetcher(manifest.KindOCIRepository, f)
					s.SetDiscoveryIndex(o.index)
					o.configureControllers()
					o.src.StartLifecycle()
					o.ksc.StartLifecycle()
					if admission == "seed" {
						s.AddObject(raw)
					}
					entered, published := make(chan struct{}), make(chan struct{})
					publishPermit, extendPermit := make(chan struct{}), make(chan struct{})
					excluded := make(chan struct{})
					var once sync.Once
					var dispatches atomic.Int64
					var initial schedule.Kind
					d := dagDispatcher{o}
					dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
						if id == parent.Named() {
							o.tasks.YieldSlot(func() {
								select {
								case <-publishPermit:
								case <-ctx.Done():
								}
							})
							f.mu.Lock()
							if f.calls[raw.Named()] != 0 {
								t.Error("raw covered source acquired before publication")
							}
							f.mu.Unlock()
							if order == "excluded then refire" {
								info, _ := s.GetStatus(raw.Named())
								if info.Reason != store.ReasonUnchanged || len(s.FailedResources()) != 0 {
									t.Errorf("excluded source created work/failure: %+v", info)
								}
								s.RecordEmitted(id, captureEmission(s, id, &store.Emission{Children: []store.Child{{Obj: &accepted}}, Complete: false}), nil)
								o.tasks.YieldSlot(func() {
									select {
									case <-extendPermit:
									case <-ctx.Done():
									}
								})
								o.ksc.KeepEmitted(id, &accepted, nil)
							} else {
								s.RecordEmitted(id, captureEmission(s, id, &store.Emission{Children: []store.Child{{Obj: &accepted}}, Complete: false}), nil)
							}
							close(published)
							return schedule.Result{}
						}
						call := dispatches.Add(1)
						out := d.Dispatch(ctx, id, level)
						if call == 1 {
							initial = out.Assessment.Outcome()
							if order != "before parking" {
								once.Do(func() { close(entered) })
							}
						}
						if call == 2 && order == "excluded then refire" {
							if out.Assessment.Outcome() != schedule.Terminal {
								t.Errorf("accepted excluded source added dependency work: %v", out)
							}
							close(excluded)
						}
						if call == 1 && order == "before parking" {
							once.Do(func() { close(entered) })
							o.tasks.YieldSlot(func() { <-published })
						}
						return out
					}}
					scheduler := schedule.New(o.tasks, dispatch)
					unwatch := o.watchDAG(scheduler)
					defer unwatch()

					scheduler.Seed(o.seedNodes())
					if admission == "event" {
						s.AddObject(raw)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					done := make(chan error, 1)
					go func() { done <- scheduler.Run(ctx) }()
					await := func(signal <-chan struct{}) {
						t.Helper()
						select {
						case <-signal:
						case err := <-done:
							t.Fatalf("scheduler stopped before publication barrier: %v", err)
						}
					}
					await(entered)
					synctest.Wait()
					close(publishPermit)
					if order == "excluded then refire" {
						await(excluded)
						synctest.Wait()
						close(extendPermit)
					}
					assert.NoError(t, <-done)
					want := schedule.Blocked
					wantDispatches := int64(2)
					if order == "excluded then refire" {
						want = schedule.Terminal
						wantDispatches = 3
					}
					if initial != want {
						t.Fatalf("initial outcome=%v want=%v", initial, want)
					}
					f.mu.Lock()
					calls := f.calls[raw.Named()]
					f.mu.Unlock()
					if calls != 1 || o.tasks.Failures() != 0 || len(s.FailedResources()) != 0 || dispatches.Load() != wantDispatches {
						t.Fatalf("publication lost scheduler progress: fetches=%d dispatches=%d failures=%v", calls, dispatches.Load(), s.FailedResources())
					}
					info, _ := s.GetStatus(parent.Named())
					if info.Status == store.StatusReady {
						t.Fatal("child waited for its parent to settle")
					}
					t.Logf("%s/%s: dispatches=%d effective fetches=%d failures=%d", order, admission, dispatches.Load(), calls, len(s.FailedResources()))
				})
			})
		}
	}
}
