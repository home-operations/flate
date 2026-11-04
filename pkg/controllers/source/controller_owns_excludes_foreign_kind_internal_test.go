package source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// TestController_OwnsExcludesForeignKind pins issue #872: a CRD from another
// API group that reuses a source kind name (seaweed.seaweedfs.com Bucket)
// parses to a RawObject, so the kind's Fetcher would reject it with
// "unexpected payload". Owns keeps the scheduler from ever dispatching it.
func TestController_OwnsExcludesForeignKind(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindBucket}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindBucket: f})

	foreign := &manifest.RawObject{
		Kind:       manifest.KindBucket,
		APIVersion: "seaweed.seaweedfs.com/v1",
		Name:       "greptimedb", Namespace: "observability",
	}
	st.AddObject(foreign)
	if c.Owns(foreign.Named()) {
		t.Fatalf("Owns(%s) = true for a foreign %s CRD", foreign.Named(), foreign.APIVersion)
	}

	flux := &manifest.Bucket{
		Name: foreign.Name, Namespace: foreign.Namespace,
		BucketName: "b", Endpoint: "minio:9000",
	}
	st.AddObject(flux)
	if !c.Owns(flux.Named()) {
		t.Errorf("Owns(%s) = false for the Flux Bucket", flux.Named())
	}
	for _, group := range []string{"", "example.test"} {
		id := flux.Named()
		id.Group = group
		assert.ErrorIf(t, c.Owns(id), "Owns(%s) = true for an absent lookalike", id)
	}
	assert.FatalIf(t, c.Owns(foreign.Named()), "%v", "same-name Flux source admitted a foreign object")
	// An id the store never saw stays ours — the vanished-object report is
	// the controller's to write.
	assert.ErrorIf(t, !c.Owns(
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindBucket,
			Namespace: "ns",
			Name:      "gone",
		},
	), "Owns = false for an absent id of a registered kind")
	assert.ErrorIf(
		t,
		c.Owns(manifest.NamedResource{Group: "apps", Kind: "Deployment", Namespace: "ns", Name: "app"}),
		"Owns = true for a kind with no fetcher",
	)
}

func TestController_MissingSecretWaitsForRender(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		allowMissing bool
	}{
		{name: "strict"},
		{name: "allow missing", allowMissing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeFetcher{
				err: src.MissingSecretErr(
					manifest.DiagnosticFor(
						manifest.NamedResource{
							Group:     manifest.SourceDomain,
							Kind:      manifest.KindOCIRepository,
							Namespace: "ns",
							Name:      "r",
						},
						nil,
					),
					"ca",
					"not found",
				),
			}
			c, st := newConfiguredController(t,
				map[string]src.Fetcher{manifest.KindOCIRepository: f},
				FetchOptions{AllowMissingSecrets: tt.allowMissing})
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns"}
			st.AddObject(repo)

			assert.FatalIf(t, controllertest.Reconcile(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
				0,
			), "%v", "missing Secret must block while renders can still produce it")
			{
				info, _ := st.GetStatus(repo.Named())
				assert.FatalIf(t, info.Status != store.StatusPending, "status = %+v, want Pending", info)
			}

			st.AddObject(&manifest.Secret{Name: "ca", Namespace: "ns"})
			f.err = nil
			f.artifact = &store.SourceArtifact{Kind: manifest.KindOCIRepository}
			assert.FatalIf(t, !controllertest.Reconcile(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
				0,
			), "%v", "source must finish after the Secret arrives")
			{
				info, _ := st.GetStatus(
					repo.Named(),
				)
				assert.FatalIf(t, info.Status != store.StatusReady ||
					info.Outcome() == store.OutcomeSkipped, "status = %+v, want Ready without a skip", info)
			}
			assert.FatalIf(
				t,
				st.GetArtifact(repo.Named(), store.ArtifactContent) == nil,
				"%v",
				"source artifact missing after retry",
			)
		})
	}
}

func TestController_UnavailableSecretAtFixpoint(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		present      bool
		allowMissing bool
		producer     bool
		plain        bool
		malformed    bool
		secret       string
		wantSkip     bool
	}{
		{name: "missing", secret: "auth"},
		{name: "missing allowed", secret: "auth", allowMissing: true, wantSkip: true},
		{name: "missing producer backed", secret: "auth", producer: true, wantSkip: true},
		{name: "missing keys", secret: "auth", present: true},
		{name: "missing keys allowed", secret: "auth", present: true, allowMissing: true, wantSkip: true},
		{name: "missing keys producer backed", secret: "auth", present: true, producer: true, wantSkip: true},
		{name: "plain missing allowed", plain: true, secret: "ghcr-creds", allowMissing: true, wantSkip: true},
		{name: "plain missing strict", plain: true, secret: "ghcr-creds"},
		{name: "missing auth producer backed", secret: "ghcr-creds", producer: true, wantSkip: true},
		{name: "missing auth no producer", secret: "ghcr-creds"},
		{name: "malformed allowed", secret: "auth", present: true, malformed: true, allowMissing: true},
		{name: "malformed producer backed", secret: "auth", present: true, malformed: true, producer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testControllerUnavailableSecretAtFixpointCase(
				t,
				&tt,
			)
		},
		)
	}
}

// fallbackFetcher fails with a missing Secret until rendered is set, and
// carries a registry-fallback retry.
type fallbackFetcher struct {
	rendered      bool
	fallbackCalls int
	retry         sourcecache.Policy
	fallbackRetry sourcecache.Policy
	fallbackErr   error
}

func (f *fallbackFetcher) Fetch(
	_ context.Context,
	_ manifest.BaseManifest,
	_ *store.ReadSet,
	policy *sourcecache.Policy,
) (*store.SourceArtifact, error) {
	f.retry = *policy
	if f.rendered {
		return &store.SourceArtifact{Kind: manifest.KindOCIRepository}, nil
	}
	missing, _ := errors.AsType[*src.MissingSecretError](
		src.MissingSecretErr(
			manifest.DiagnosticFor(
				manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindOCIRepository,
					Namespace: "ns",
					Name:      "r",
				},
				nil,
			),
			"ghcr-creds",
			"not found",
		),
	)
	missing.RetryAmbient = func(_ context.Context) (*store.SourceArtifact, error) {
		f.fallbackCalls++
		f.fallbackRetry = *policy
		if f.fallbackErr != nil {
			return nil, f.fallbackErr
		}
		return &store.SourceArtifact{Kind: manifest.KindOCIRepository}, nil
	}
	return nil, missing
}

func TestController_MissingSecretRegistryFallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		rendered     bool
		wantFallback int
	}{
		{name: "rendered Secret takes precedence", rendered: true},
		{name: "unrendered Secret falls back at fixpoint", wantFallback: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testControllerMissingSecretRegistryFallbackCase(
				t,
				&tt,
			)
		},
		)
	}
}

func TestController_AmbientPolicy(t *testing.T) {
	t.Parallel()
	for _, repo := range []manifest.BaseManifest{
		&manifest.GitRepository{Name: "r", Namespace: "ns", URL: "ssh://git@fixture/repo"},
		&manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://fixture/image"},
		&manifest.Bucket{Name: "r", Namespace: "ns", Endpoint: "https://bucket.invalid", BucketName: "data"},
		&manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "r"}},
	} {
		for _, tt := range []struct {
			name            string
			producer, allow bool
			failure         error
		}{
			{name: "producer", producer: true},
			{name: "producer allowed", producer: true, allow: true},
			{name: "success"},
			{name: "success allowed", allow: true},
			{name: "rejected", failure: &fetcherr.ResponseError{StatusCode: 401}},
			{name: "rejected allowed", allow: true, failure: &fetcherr.ResponseError{StatusCode: 401}},
			{name: "unavailable", failure: src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "no machine credentials")},
			{name: "unavailable allowed", allow: true, failure: src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "no machine credentials")},
			{name: "acquisition error", failure: &fs.PathError{Op: "open", Path: "repositories.yaml", Err: fs.ErrPermission}},
			{name: "acquisition error allowed", allow: true, failure: &fs.PathError{Op: "open", Path: "repositories.yaml", Err: fs.ErrPermission}},
			{name: "timeout", failure: context.DeadlineExceeded},
			{name: "timeout allowed", allow: true, failure: context.DeadlineExceeded},
			{name: "canceled", failure: context.Canceled},
			{name: "canceled allowed", allow: true, failure: context.Canceled},
		} {
			t.Run(repo.Named().Kind+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				testControllerAmbientPolicyCase(t, testControllerAmbientPolicyCaseInputs{repo: &repo, tt: &tt})
			},
			)
		}
	}
}

func TestController_AmbientContextError(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
			failure := errors.Join(
				src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "unavailable"),
				cause,
			)
			missing, _ := errors.AsType[*src.MissingSecretError](
				src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "auth", "not found"),
			)
			missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) { return nil, failure }
			controller, st := newConfiguredController(t, nil, FetchOptions{AllowMissingSecrets: true})
			st.AddObject(repo)
			c := &execution{Controller: controller}
			c.run.Init(controller.Controller, repo.Named())
			defer c.run.Close()
			var err error
			c.Tasks.Go(t.Context(), "settle", func(ctx context.Context) {
				err = c.run.Acquire(
					ctx,
					repo,
					base.Acquisition{
						AllowMissingSecrets: true,
						Fetch: func(context.Context) (*store.SourceArtifact, error) {
							return nil, missing
						},
						Complete: func(_ context.Context, artifact *store.SourceArtifact, unavailable error, _ manifest.DrainLevel) error {
							return c.completeFetch(repo, artifact, unavailable)
						},
					},
					manifest.DrainCascade)
			})
			c.Tasks.BlockTillDone()
			assert.FatalIf(t, !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) ||
				errors.Is(
					err,
					manifest.ErrSourceSkipped,
				), "default settlement error = %v; want %v and ErrFlux", err, cause)
		})
	}
}

func TestController_ExistenceOnly_NoTransientPendingOnReemit(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{}
	c, st := newController(t, map[string]src.Fetcher{
		manifest.KindHelmRepository: f,
	})
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		URL: "https://charts.example", Type: "default",
	}
	st.AddObject(repo)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != repo.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	reemit := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		URL: "https://charts.example", Type: "default",
		Interval: metav1.Duration{Duration: time.Hour},
	}
	st.AddObject(reemit)
	controllertest.Reconcile(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
		0,
	)
	if f.calls != 0 {
		t.Errorf("presence-only acquisitions = %d, want 0", f.calls)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.ErrorIf(
		t,
		sawPending,
		"%v",
		"presence-only source transiently downgraded Ready to Pending on a no-op re-run (the quiescence-race window)",
	)
	{
		info, _ := st.GetStatus(repo.Named())
		assert.ErrorIf(t, info.Status != store.StatusReady, "source not Ready after no-op re-run: %+v", info)
	}
}

func testControllerUnavailableSecretAtFixpointCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		present      bool
		allowMissing bool
		producer     bool
		plain        bool
		malformed    bool
		secret       string
		wantSkip     bool
	},
) {
	t.Helper()

	secret := &manifest.Secret{Name: testCaseInput.secret, Namespace: "ns"}
	detail := "not found"
	if testCaseInput.present {
		detail = "missing username/password"
	}
	repo := &manifest.OCIRepository{
		Name:      "r",
		Namespace: "ns",
		URL:       "oci://example/img",
		Interval:  &metav1.Duration{Duration: time.Hour},
	}
	fetchErr := src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), secret.Name, detail)
	if testCaseInput.plain {
		fetchErr = manifest.NewError(
			manifest.ErrMissingSecret,
			nil,
			fmt.Sprintf("OCIRepository ns/r: secret ns/%s %s", secret.Name, detail),
		)
	}
	if testCaseInput.malformed {
		detail = "invalid credential encoding"
		fetchErr = manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("OCIRepository ns/r: secret ns/%s %s", secret.Name, detail),
		)
	}
	ambientCalls := 0
	if testCaseInput.producer && !testCaseInput.malformed {
		missing, _ := errors.AsType[*src.MissingSecretError](fetchErr)
		missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
			ambientCalls++
			return &store.SourceArtifact{Kind: repo.Named().Kind}, nil
		}
	}
	f := &fakeFetcher{err: fetchErr, secret: secret.Name}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	if testCaseInput.producer {
		addDeclaration(
			index,
			secret.Named(),
			manifest.NamedResource{
				Group:     "external-secrets.io",
				Kind:      "ExternalSecret",
				Namespace: "ns",
				Name:      "auth",
			},
		)
	}
	c, st := newConfiguredController(t,
		map[string]src.Fetcher{manifest.KindOCIRepository: f},
		FetchOptions{AllowMissingSecrets: testCaseInput.allowMissing})
	st.SetDiscoveryIndex(index)
	if testCaseInput.present {
		st.AddObject(secret)
	}
	st.AddObject(repo)
	var info store.StatusInfo
	if testCaseInput.plain || testCaseInput.malformed {
		info = controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         st,
				ID:            repo.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
	} else {
		assert.FatalIf(t, controllertest.Reconcile(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
			manifest.DrainNone,
		), "%v", "must wait while renders can still supply Secret contents")
		{
			pending, _ := st.GetStatus(repo.Named())
			assert.FatalIf(t, pending.Status != store.StatusPending, "status = %+v, want Pending", pending)
		}
		assert.FatalIf(t, !controllertest.Reconcile(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
			manifest.DrainCascade,
		), "%v", "unavailable Secret must terminalize at the fixpoint")
		info, _ = st.GetStatus(repo.Named())
		if f.calls != 2 {
			t.Errorf("fetch calls = %d, want 2", f.calls)
		}
	}
	wantStatus := store.StatusFailed
	if testCaseInput.wantSkip {
		wantStatus = store.StatusReady
	}
	if info.Status != wantStatus || (info.Outcome() == store.OutcomeSkipped) != testCaseInput.wantSkip ||
		!strings.Contains(info.String(), detail) ||
		!strings.Contains(info.String(), secret.Name) {
		t.Fatalf(
			"status = %+v, want %v (skip=%t) preserving %q and %q",
			info,
			wantStatus, testCaseInput.wantSkip, detail,
			secret.Name,
		)
	}
	assert.FatalIf(
		t,
		ambientCalls != 0,
		"index-only producer invoked ambient continuation %d times",
		ambientCalls,
	)
	assert.FatalIf(
		t,
		st.GetArtifact(repo.Named(), store.ArtifactContent) != nil,
		"%v",
		"unavailable or malformed source must not produce an artifact",
	)
}

func testControllerMissingSecretRegistryFallbackCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		rendered     bool
		wantFallback int
	},
) {
	t.Helper()

	f := &fallbackFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: f})
	repo := &manifest.OCIRepository{
		Name:      "r",
		Namespace: "ns",
		URL:       "oci://example/img",
		Interval:  &metav1.Duration{Duration: time.Hour},
	}
	st.AddObject(repo)

	assert.FatalIf(t, controllertest.Reconcile(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
		0,
	), "%v", "missing Secret must block before any registry fallback")
	if f.fallbackCalls != 0 {
		t.Fatalf("fallback calls = %d while renders can still produce the Secret, want 0", f.fallbackCalls)
	}

	if testCaseInput.rendered {
		st.AddObject(&manifest.Secret{Name: "ghcr-creds", Namespace: "ns"})
		f.rendered = true
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         st,
				ID:            repo.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady ||
			info.Outcome() == store.OutcomeSkipped, "status = %+v, want Ready without a skip", info)
	}
	assert.ErrorIf(
		t,
		st.GetArtifact(repo.Named(), store.ArtifactContent) == nil,
		"%v",
		"fetch must store its artifact",
	)
	policies := []sourcecache.Policy{f.retry}
	if testCaseInput.wantFallback > 0 {
		policies = append(policies, f.fallbackRetry)
	}
	for _, policy := range policies {
		if policy.MaxAge != time.Hour || policy.Retry != (sourcecache.RetryConfig{Attempts: 3}) ||
			policy.Timeout != nil ||
			policy.Admit != nil {
			t.Fatalf("normal/fallback policies=%+v/%+v", f.retry, f.fallbackRetry)
		}
	}
	if f.fallbackCalls != testCaseInput.wantFallback {
		t.Errorf("fallback calls = %d, want %d", f.fallbackCalls, testCaseInput.wantFallback)
	}
}

type testControllerAmbientPolicyCaseInputs struct {
	repo *manifest.BaseManifest
	tt   *struct {
		name string

		producer bool
		allow    bool
		failure  error
	}
}

func testControllerAmbientPolicyCase(t *testing.T, testCaseInput testControllerAmbientPolicyCaseInputs) {
	t.Helper()

	missing, _ := errors.AsType[*src.MissingSecretError](
		src.MissingSecretErr(
			manifest.DiagnosticFor((*testCaseInput.repo).Named(), (*testCaseInput.repo)),
			"auth",
			"not found",
		),
	)
	calls := 0
	failure := testCaseInput.tt.failure
	if failure != nil {
		failure = manifest.NewError(manifest.ErrFlux, fmt.Errorf("ambient acquisition: %w", failure), "")
	}
	missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
		calls++
		if failure != nil {
			return nil, failure
		}
		return &store.SourceArtifact{Kind: (*testCaseInput.repo).Named().Kind, FS: fstest.MapFS{}}, nil
	}
	f := &fakeFetcher{err: missing, secret: "auth"}
	c, st := newConfiguredController(
		t,
		map[string]src.Fetcher{(*testCaseInput.repo).Named().Kind: f},
		FetchOptions{AllowMissingSecrets: testCaseInput.tt.allow},
	)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	if testCaseInput.tt.producer {
		addDeclaration(
			index,
			missing.Secret,
			manifest.NamedResource{
				Group:     "external-secrets.io",
				Kind:      "ExternalSecret",
				Namespace: "ns",
				Name:      "producer",
			},
		)
	}
	st.SetDiscoveryIndex(index)
	repository := &manifest.HelmRepository{Name: "r", Namespace: "ns", URL: "https://charts.invalid"}
	st.AddObject(repository)
	st.UpdateStatus(repository.Named(), store.StatusInfo{Status: store.StatusReady})
	st.AddObject((*testCaseInput.repo))
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{
			Tasks:         c.Tasks,
			Store:         st,
			ID:            (*testCaseInput.repo).Named(),
			ReconcileNode: c.ReconcileNode,
		},
	)
	wantCalls := 1
	if testCaseInput.tt.producer {
		wantCalls = 0
	}
	wantSkip := testCaseInput.tt.producer ||
		testCaseInput.tt.allow && errors.Is(failure, manifest.ErrMissingSecret) && !errors.Is(failure, context.Canceled) &&
			!errors.Is(failure, context.DeadlineExceeded)
	wantStatus := store.StatusReady
	if failure != nil && !wantSkip {
		wantStatus = store.StatusFailed
	}
	assert.FatalIf(t, calls != wantCalls || info.Status != wantStatus ||
		(info.Outcome() == store.OutcomeSkipped) != wantSkip, "status=%+v calls=%d; want status=%v skip=%t calls=%d",
		info,
		calls,
		wantStatus,
		wantSkip,
		wantCalls,
	)
	assert.FatalIf(
		t,
		failure != nil && !wantSkip && info.Message != "secret ns/auth not found",
		"settlement message = %q",
		info.Message,
	)
	assert.FatalIf(
		t,
		failure != nil && !wantSkip && !strings.Contains(info.Message, "secret ns/auth not found"),
		"settlement lost unavailable-material context: %+v",
		info,
	)
	assert.FatalIf(
		t,
		(st.GetArtifact((*testCaseInput.repo).Named(), store.ArtifactContent) != nil) != (!wantSkip && failure == nil),
		"%v",
		"ambient settlement published the wrong content outcome",
	)
	if !wantSkip && failure == nil {
		controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         st,
				ID:            (*testCaseInput.repo).Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, calls != wantCalls, "%v", "accepted source artifact was acquired again")
		assert.Equal(t, len(st.Warnings()), 0)
	}
	assert.FatalIf(t, wantSkip && !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Resource == (*testCaseInput.repo).Named() &&
			w.Category == manifest.WarnSourceCredentialsUnavailable
	}), "%v", "missing-material settlement lost its warning")
}
