package source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_ArtifactMaterializers(t *testing.T) {
	t.Parallel()
	for _, state := range []store.Status{store.StatusPending, store.StatusFailed, store.StatusReady} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			testControllerArtifactMaterializersCase(t, &state)
		},
		)
	}
}

type materializerFetcher struct{ calls atomic.Int64 }

func (f *materializerFetcher) Fetch(
	context.Context,
	manifest.BaseManifest,
	*store.ReadSet,
	sourcecache.Policy,
) (*store.SourceArtifact, error) {
	f.calls.Add(1)
	return nil, manifest.NewError(manifest.ErrInput, nil, "content unavailable")
}

func TestController_ArtifactMaterializersProduction(t *testing.T) {
	t.Parallel()
	for _, declarationPresent := range []bool{false, true} {
		t.Run(fmt.Sprintf("declaration=%t", declarationPresent), func(t *testing.T) {
			t.Parallel()
			fetcher := &materializerFetcher{}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			generator := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1",
				Kind:       "ArtifactGenerator",
				Name:       "generator",
				Namespace:  "apps",
			}
			obj := &manifest.ExternalArtifact{Name: "generated", Namespace: "apps"}
			st.AddObject(owner)
			st.AddObject(obj)
			materializer := owner.Named()
			if declarationPresent {
				st.AddObject(generator)
				materializer = generator.Named()
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				obj.Named(): {
					Productions: []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}},
				},
			}})
			st.UpdateStatus(
				materializer,
				store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "progressing"},
			)
			var blocked []manifest.NamedResource
			c.Tasks.Go(t.Context(), "predicted", func(ctx context.Context) {
				run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
				blocked = run.Blocked
			})
			c.Tasks.BlockTillDone()
			if len(blocked) != 1 || blocked[0] != materializer || fetcher.calls.Load() != 0 {
				t.Fatalf(
					"progressing materializer = %v, want %s; fetches=%d",
					blocked,
					materializer,
					fetcher.calls.Load(),
				)
			}
		})
	}
}

func TestController_ArtifactMaterializersCompleteEmpty(t *testing.T) {
	t.Parallel()
	fetcher := &materializerFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
	generator := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
	}
	st.AddObject(generator)
	doc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       manifest.KindExternalArtifact,
		"metadata":   map[string]any{"name": "generated", "namespace": "apps"},
		"spec": map[string]any{
			"sourceRef": map[string]any{
				"apiVersion": generator.APIVersion,
				"kind":       generator.Kind,
				"name":       generator.Name,
			},
		},
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parsed.Object, parsed.Document
	assert.FatalIf(t, err != nil, "%v", err)
	st.AddObject(obj, authoritative)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		obj.Named(): {
			Productions: []manifest.Production{
				{Owner: generator.Named(), Declaration: generator.Named(), Source: manifest.BootstrapSourceID},
			},
		},
	}})
	assert.FatalIf(t, len(st.Productions(obj.Named(), nil)) != 1, "%v", "fixture has no bound output prediction")
	_, _, empty := st.GetEmission(generator.Named())
	defer empty.Finish()
	empty.Complete = true
	st.RecordEmitted(generator.Named(), &empty, nil)
	st.UpdateStatus(
		generator.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "complete empty"},
	)
	assert.FatalIf(t, len(st.Productions(obj.Named(), nil)) != 0 || st.GetObject(obj.Named()) != nil ||
		st.GetArtifact(
			obj.Named(),
			store.ArtifactContent,
		) != nil, "%v", "complete empty inventory retained predicted output authority")
	st.AddObject(obj, authoritative)
	assert.FatalIf(
		t,
		len(st.Productions(obj.Named(), nil)) != 0,
		"%v",
		"independent declaration revived a completed producer prediction",
	)
	var blocked []manifest.NamedResource
	c.Tasks.Go(t.Context(), "complete-empty", func(ctx context.Context) {
		run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
		blocked = run.Blocked
	})
	c.Tasks.BlockTillDone()
	status, _ := st.GetStatus(obj.Named())
	if len(blocked) != 0 || status.Status != store.StatusFailed || status.IsDependencyFailure() ||
		fetcher.calls.Load() != 1 ||
		st.GetArtifact(obj.Named(), store.ArtifactContent) != nil {
		t.Fatalf("complete empty source = %+v, blocked=%v, fetches=%d", status, blocked, fetcher.calls.Load())
	}
}

type materializerDispatcher struct {
	controller *Controller
	drains     [3]atomic.Int64
}

func (d *materializerDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
) schedule.Result {
	d.drains[level].Add(1)
	return schedule.Result{
		Assessment: &materializerAssessment{Execution: d.controller.ReconcileNode(ctx, id, level), drains: &d.drains},
	}
}

func (d *materializerDispatcher) Sweep(_ context.Context, _ bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

type materializerAssessment struct {
	*base.Execution

	drains *[3]atomic.Int64
}

func (a *materializerAssessment) Settle(ctx context.Context, level manifest.DrainLevel) {
	a.drains[level].Add(1)
	a.Execution.Settle(ctx, level)
}

func TestController_ArtifactMaterializersCycleDrains(t *testing.T) {
	t.Parallel()
	fetcher := &materializerFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
	ids := []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: "a", Namespace: "apps"},
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: "b", Namespace: "apps"},
	}
	for i, id := range ids {
		doc := map[string]any{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       id.Kind,
			"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
			"spec": map[string]any{
				"sourceRef": map[string]any{
					"apiVersion": "source.toolkit.fluxcd.io/v1",
					"kind":       id.Kind,
					"name":       ids[1-i].Name,
				},
			},
		}
		parsed2, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		obj, authoritative := parsed2.Object, parsed2.Document
		assert.FatalIf(t, err != nil, "%v", err)
		st.AddObject(obj, authoritative)
	}
	dispatcher := &materializerDispatcher{controller: c}
	scheduler := schedule.New(c.Tasks, dispatcher)
	defer st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) {
		scheduler.OnPublication()
	}, false)()
	scheduler.Seed(ids)
	{
		err := scheduler.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	if dispatcher.drains[manifest.DrainForce].Load() == 0 || fetcher.calls.Load() != 0 || c.Tasks.Failures() != 0 {
		t.Fatalf(
			"cycle force passes=%d, fetches=%d, task failures=%d",
			dispatcher.drains[manifest.DrainForce].Load(),
			fetcher.calls.Load(),
			c.Tasks.Failures(),
		)
	}
	for i, id := range ids {
		status, _ := st.GetStatus(id)
		blocked := status.BlockedBy()
		assert.FatalIf(
			t,
			status.Status != store.StatusFailed || !status.IsDependencyFailure() || len(blocked) != 1 ||
				blocked[0] != ids[1-i] ||
				st.GetArtifact(
					id,
					store.ArtifactContent,
				) != nil,
			"contentless cycle %s = %+v, blocked=%v",
			id,
			status,
			blocked,
		)
	}
}

func TestController_AmbientSettlementExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, secret := range []string{"auth", ""} {
		for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade} {
			t.Run(fmt.Sprintf("secret=%q/level=%d", secret, level), func(t *testing.T) {
				t.Parallel()
				repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "ssh://git@fixture/repo"}
				missing, _ := errors.AsType[*src.MissingSecretError](
					src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), secret, "credentials unavailable"),
				)
				calls := 0
				missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
					calls++
					return &store.SourceArtifact{Kind: manifest.KindGitRepository}, nil
				}
				f := &fakeFetcher{err: missing}
				c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
				st.AddObject(repo)
				dispatch := controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}
				done := controllertest.Reconcile(t.Context(), t, dispatch, level)
				if secret != "" && level == manifest.DrainNone {
					assert.FatalIf(t, done || calls != 0, "%v", "ambient tier ran before repository settlement")
					done = controllertest.Reconcile(t.Context(), t, dispatch, manifest.DrainCascade)
				}
				assert.FatalIf(
					t,
					!done || calls != 1 || st.GetArtifact(repo.Named(), store.ArtifactContent) == nil,
					"settlement: done=%t calls=%d",
					done,
					calls,
				)
			})
		}
	}
}

func TestController_SyntheticCredentialSkip(t *testing.T) {
	t.Parallel()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	repo := chart.OCIRepository(
		&manifest.HelmRepository{
			URL:       "oci://registry.example/charts",
			SecretRef: &manifest.LocalObjectReference{Name: "auth"},
		},
	)
	failure := src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "auth", "not found")
	fetcher := &fakeFetcher{err: failure, secret: "auth"}
	controller, st := newConfiguredController(
		t,
		map[string]src.Fetcher{manifest.KindOCIRepository: fetcher},
		FetchOptions{AllowMissingSecrets: true},
	)
	st.AddObject(repo)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{
			Tasks:         controller.Tasks,
			Store:         st,
			ID:            repo.Named(),
			ReconcileNode: controller.ReconcileNode,
		},
	)
	warnings := st.Warnings()
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "sources", Name: "auth"}
	assert.FatalIf(t, info.Outcome() != store.OutcomeSkipped || info.String() != "secret sources/auth not found" ||
		len(warnings) != 1, "skip lost display or outcome: %+v %+v", info, warnings)
	assert.FatalIf(t, warnings[0].Resource != repo.Named() || warnings[0].Subject.Resource != chart.Named() ||
		!slices.Equal(warnings[0].Related, []manifest.NamedResource{secret}) ||
		st.GetArtifact(repo.Named(), store.ArtifactContent) != nil, "skip lost raw provenance: %+v", warnings)
}

func TestController_AmbientFailureRetainsCause(t *testing.T) {
	t.Parallel()
	for _, rejected := range []bool{false, true} {
		t.Run(strconv.FormatBool(rejected), func(t *testing.T) {
			t.Parallel()
			repo := &manifest.GitRepository{Name: "kubernetes-secrets", Namespace: "flux-system"}
			missing, _ := errors.AsType[*src.MissingSecretError](
				src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), repo.Name, "not found"),
			)
			var cause error = missing
			if rejected {
				cause = manifest.NewError(manifest.ErrFlux, &fetcherr.ResponseError{StatusCode: 401}, "")
			}
			missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
				if !rejected {
					return nil, src.MissingSecretErr(
						manifest.DiagnosticFor(repo.Named(), repo),
						"",
						"SSH agent unavailable",
					)
				}
				return nil, cause
			}
			controller, st := newConfiguredController(t, nil, FetchOptions{AllowMissingSecrets: true})
			st.AddObject(repo)
			c := &execution{Controller: controller}
			c.run.Init(controller.Controller, repo.Named())
			defer c.run.Close()
			acquisition := base.Acquisition{
				AllowMissingSecrets: true,
				Fetch:               func(context.Context) (*store.SourceArtifact, error) { return nil, missing },
				Complete: func(_ context.Context, artifact *store.SourceArtifact, unavailable error, _ manifest.DrainLevel) error {
					return c.completeFetch(repo, artifact, unavailable)
				},
			}
			var err error
			c.Tasks.Go(
				t.Context(),
				"settle",
				func(ctx context.Context) { err = c.run.Acquire(ctx, repo, acquisition, manifest.DrainCascade) },
			)
			c.Tasks.BlockTillDone()
			assert.FatalIf(t, err == nil || err.Error() != "secret flux-system/kubernetes-secrets not found" ||
				!errors.Is(err, cause) ||
				errors.Is(err, manifest.ErrSourceSkipped) == rejected, "ambient classification: %v", err)
			assert.FatalIf(
				t,
				!strings.Contains(manifest.ErrorDetails(err), cause.Error()),
				"%v",
				"lost ambient diagnostic",
			)
			{
				response, ok := errors.AsType[*fetcherr.ResponseError](
					err,
				)
				assert.FatalIf(t, rejected &&
					(!ok || response.StatusCode != http.StatusUnauthorized), "%v", "lost typed rejection")
			}
		})
	}
}

func TestController_DeclaredNonGitWithAdmission(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindHelmRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testControllerDeclaredNonGitWithAdmissionCase(
				t,
				&kind,
			)
		},
		)
	}
}

func testControllerArtifactMaterializersCase(
	t *testing.T,
	testCaseInput *store.Status,
) {
	t.Helper()

	fetcher := &fakeFetcher{err: manifest.NewError(manifest.ErrInput, nil, "content unavailable")}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
	generator := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
	}
	st.AddObject(generator)
	st.UpdateStatus(
		generator.Named(),
		store.StatusInfo{Status: (*testCaseInput), Reason: "", Message: "materializer state"},
	)
	doc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       "ExternalArtifact",
		"metadata":   map[string]any{"name": "generated", "namespace": "apps"},
		"spec": map[string]any{
			"sourceRef": map[string]any{
				"apiVersion": generator.APIVersion,
				"kind":       generator.Kind,
				"name":       generator.Name,
			},
		},
	}
	parsed3, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parsed3.Object, parsed3.Document
	assert.FatalIf(t, err != nil, "%v", err)
	st.AddObject(obj, authoritative)
	var blocked []manifest.NamedResource
	var outcome schedule.Kind
	c.Tasks.Go(t.Context(), "materializer", func(ctx context.Context) {
		run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
		outcome, blocked = run.Outcome(), run.Blocked
		if outcome == schedule.DependencyFailed {
			status, _ := st.GetStatus(obj.Named())
			blocked = status.BlockedBy()
		}
	})
	c.Tasks.BlockTillDone()
	switch *testCaseInput {
	case store.StatusPending:
		if outcome != schedule.Blocked || len(blocked) != 1 || blocked[0] != generator.Named() ||
			fetcher.calls != 0 {
			t.Fatalf("materializer wait = %v, calls=%d", blocked, fetcher.calls)
		}
	case store.StatusFailed:
		status, _ := st.GetStatus(obj.Named())
		if outcome != schedule.DependencyFailed || len(blocked) != 1 || blocked[0] != generator.Named() ||
			status.Status != store.StatusFailed ||
			fetcher.calls != 0 {
			t.Fatalf(
				"failed materializer result = %+v, %+v, %v, calls=%d",
				outcome,
				status,
				blocked,
				fetcher.calls,
			)
		}
	case store.StatusReady:
		status, _ := st.GetStatus(obj.Named())
		assert.FatalIf(
			t,
			len(blocked) != 0 || status.Status != store.StatusFailed,
			"contentless result = %+v, %v",
			status,
			blocked,
		)
	default:
		status, _ := st.GetStatus(obj.Named())
		assert.FatalIf(
			t,
			len(blocked) != 0 || status.Status != store.StatusFailed,
			"contentless result = %+v, %v",
			status,
			blocked,
		)
	}
	payload := &store.SourceArtifact{
		Kind: manifest.KindExternalArtifact,
		Key:  "generated",
		FS:   fstest.MapFS{"file": {Data: []byte("available")}},
	}
	st.RecordEmitted(
		generator.Named(),
		&store.Emission{Children: []store.Child{{Obj: obj, Doc: authoritative}}},
		map[manifest.NamedResource]*store.SourceArtifact{obj.Named(): payload},
	)
	before := fetcher.calls
	st.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
	c.Tasks.Go(t.Context(), "accepted", func(ctx context.Context) {
		run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
		blocked = run.Blocked
	})
	c.Tasks.BlockTillDone()
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(
		t,
		len(blocked) != 0 || status.Status != store.StatusReady || fetcher.calls != before,
		"accepted bytes waited for %s producer: %+v, %v", (*testCaseInput), status,
		blocked,
	)
}

func testControllerDeclaredNonGitWithAdmissionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{
		LocalPath: root,
		LocalRoot: root,
		FS:        fstest.MapFS{"decoy": {Data: []byte("local")}},
	}
	result, err := discovery.Run(t.Context(), discovery.Config{Input: input, Store: st})
	assert.NoError(t, err)
	spec := map[string]any{"url": "oci://fixture.invalid/artifact"}
	if (*testCaseInput) == manifest.KindHelmRepository {
		spec["url"] = "https://fixture.invalid/charts"
	}
	if (*testCaseInput) == manifest.KindBucket {
		spec = map[string]any{"endpoint": "fixture.invalid", "bucketName": "published"}
	}
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1",
		"kind":       (*testCaseInput),
		"metadata":   map[string]any{"name": "declared", "namespace": "sources"},
		"spec":       spec,
	}
	parsed4, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj := parsed4.Object
	assert.NoError(t, err)
	before := manifest.Fingerprint(obj)
	f := &fakeFetcher{
		artifact: &store.SourceArtifact{
			Kind: (*testCaseInput),
			URL:  "remote",
			FS:   fstest.MapFS{"payload": {Data: []byte("acquired")}},
		},
	}
	c, _ := newConfiguredController(
		t,
		map[string]src.Fetcher{(*testCaseInput): f},
		FetchOptions{Admission: result.Admission, Loader: loader.New(st)},
	)
	st.AddObject(obj, doc)
	status := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: obj.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, status.Status, store.StatusReady)
	if (*testCaseInput) == manifest.KindHelmRepository {
		assert.Equal(t, f.calls, 0)
		assert.Equal(t, st.GetObject(obj.Named()), obj)
		assert.Equal(t, manifest.Fingerprint(obj), before)
		return
	}
	if f.calls != 1 || len(f.objects) != 1 || f.objects[0] != obj {
		t.Fatalf("fetcher did not receive authored declaration: calls=%d objects=%v", f.calls, f.objects)
	}
	assert.Equal(t, manifest.Fingerprint(obj), before)
	artifact := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.SourceArtifact)
	data, err := fs.ReadFile(artifact.FS, "payload")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "acquired")
}
