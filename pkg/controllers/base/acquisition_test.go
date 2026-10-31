package base

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAcquire_Outcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                        string
		missing, anonymous, producer, allow, policy bool
		failure                                     error
		unavailable, hard                           bool
	}{
		{name: "acquired"},
		{name: "anonymous unavailable", anonymous: true, policy: true, unavailable: true},
		{name: "anonymous denied without policy", anonymous: true, hard: true},
		{name: "explicit missing", missing: true, policy: true, hard: true},
		{name: "explicit missing allowed", missing: true, allow: true, unavailable: true},
		{name: "producer unavailable", missing: true, producer: true, unavailable: true},
		{name: "supplied machine credential rejected", policy: true, failure: cache.MachineCredentialErr(&fetcherr.ResponseError{StatusCode: http.StatusUnauthorized}), hard: true},
		{name: "malformed material", allow: true, failure: fmt.Errorf("%w: malformed credentials", manifest.ErrInput), hard: true},
		{name: "registry failure", allow: true, failure: &fetcherr.ResponseError{StatusCode: http.StatusServiceUnavailable}, hard: true},
		{name: "canceled", anonymous: true, allow: true, policy: true, failure: context.Canceled, hard: true},
		{name: "deadline", anonymous: true, allow: true, policy: true, failure: context.DeadlineExceeded, hard: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			t.Cleanup(tasks.BlockTillDone)
			owner := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
			provider := &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "registry"}
			st.AddObjects([]manifest.BaseManifest{owner, provider})
			missing := &source.MissingSecretError{Owner: provider.Named(), Subject: manifest.DiagnosticFor(provider.Named(), provider), Detail: "registry credentials unavailable", AnonymousDenied: tc.anonymous}
			if tc.missing {
				missing.Secret = manifest.NamedResource{Kind: manifest.KindSecret, Namespace: provider.Namespace, Name: "auth"}
			}
			if tc.producer {
				declaration := manifest.NamedResource{Kind: "ExternalSecret", Namespace: provider.Namespace, Name: "auth"}
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{missing.Secret: {Productions: []manifest.Production{{Declaration: declaration}}}}})
			}
			artifact := &store.SourceArtifact{Kind: manifest.KindResourceSetInputProvider, FS: fstest.MapFS{}}
			failure := tc.failure
			if tc.missing || tc.anonymous {
				missing.Cause = failure
				failure = missing
			}
			run := gateExecution(New(st, tasks), owner.Named())
			defer run.Close()
			completed := 0
			var got error
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
				run.Dispatch(ctx, manifest.DrainCascade, t, func(t *testing.T, ctx context.Context, _ *manifest.ResourceSet, level manifest.DrainLevel) error {
					t.Helper()
					run.Reads.GetObject(provider.Named())
					got = run.Acquire(ctx, provider, Acquisition{
						AllowMissingSecrets: tc.allow, AllowAnonymousDenied: tc.policy,
						Fetch: func(context.Context) (*store.SourceArtifact, error) {
							if tc.missing {
								source.SecretLookup(&run.Reads)(provider.Namespace, missing.Secret.Name)
							}
							if failure != nil {
								return nil, failure
							}
							return artifact, nil
						},
						Complete: func(_ context.Context, acquired *store.SourceArtifact, unavailable error, completedLevel manifest.DrainLevel) error {
							completed++
							if (unavailable != nil) != tc.unavailable || completedLevel != level {
								t.Errorf("completion unavailable=%v level=%d", unavailable, completedLevel)
							}
							if !tc.unavailable && acquired != artifact {
								t.Error("completion lost acquired content")
							}
							return nil
						},
					}, level)
					return got
				})
			})
			tasks.BlockTillDone()
			if (got != nil) != tc.hard || completed != map[bool]int{true: 0, false: 1}[tc.hard] {
				t.Fatalf("error=%v completions=%d, want hard=%t", got, completed, tc.hard)
			}
			if tc.hard && !errors.Is(got, failure) {
				t.Fatalf("hard failure lost its cause: %v", got)
			}
			warnings := st.Warnings()
			if !tc.unavailable {
				if len(warnings) != 0 {
					t.Fatalf("unexpected warnings: %+v", warnings)
				}
				return
			}
			if len(warnings) != 1 || warnings[0].Resource != provider.Named() || warnings[0].Subject != manifest.DiagnosticFor(provider.Named(), provider) || warnings[0].Category != manifest.WarnSourceCredentialsUnavailable {
				t.Fatalf("provider diagnostic=%+v", warnings)
			}
			var related []manifest.NamedResource
			if tc.missing {
				related = []manifest.NamedResource{missing.Secret}
			}
			if !slices.Equal(warnings[0].Related, related) {
				t.Fatalf("related=%v, want %v", warnings[0].Related, related)
			}
			if info, _ := st.GetStatus(owner.Named()); info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
				t.Fatalf("provider unavailability suppressed the consumer: %+v", info)
			}
		})
	}
}

func TestAcquire_SettlementContinuation(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	t.Cleanup(tasks.BlockTillDone)
	owner := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	provider := &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "registry"}
	st.AddObjects([]manifest.BaseManifest{owner, provider})
	run := gateExecution(New(st, tasks), owner.Named())
	defer run.Close()
	missing := &source.MissingSecretError{Owner: provider.Named(), Subject: manifest.DiagnosticFor(provider.Named(), provider), Secret: manifest.NamedResource{Kind: manifest.KindSecret, Namespace: provider.Namespace, Name: "auth"}, Detail: "not found"}
	artifact := &store.SourceArtifact{Kind: manifest.KindResourceSetInputProvider, FS: fstest.MapFS{"inputs.json": {Data: []byte(`[]`)}}}
	var fetches, retries, completions int
	missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
		retries++
		return artifact, nil
	}
	acquisition := Acquisition{
		Fetch: func(context.Context) (*store.SourceArtifact, error) {
			fetches++
			source.SecretLookup(&run.Reads)(provider.Namespace, missing.Secret.Name)
			return nil, missing
		},
		Complete: func(_ context.Context, acquired *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
			completions++
			if acquired != artifact || unavailable != nil || level != manifest.DrainCascade {
				t.Errorf("completion artifact=%v unavailable=%v level=%d", acquired, unavailable, level)
			}
			return nil
		},
	}
	tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
		run.Dispatch(ctx, manifest.DrainNone, run, func(run *Execution, ctx context.Context, _ *manifest.ResourceSet, level manifest.DrainLevel) error {
			return run.Acquire(ctx, provider, acquisition, level)
		})
	})
	tasks.BlockTillDone()
	if run.Outcome() != schedule.Blocked || !slices.Equal(run.Blocked, []manifest.NamedResource{missing.Secret}) || fetches != 1 || retries != 0 || completions != 0 {
		t.Fatalf("initial outcome=%v blocked=%v fetches=%d retries=%d completions=%d", run.Outcome(), run.Blocked, fetches, retries, completions)
	}
	tasks.Go(t.Context(), "settle", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
	tasks.BlockTillDone()
	if run.Outcome() != schedule.Terminal || fetches != 1 || retries != 1 || completions != 1 {
		t.Fatalf("settled outcome=%v fetches=%d retries=%d completions=%d", run.Outcome(), fetches, retries, completions)
	}
	if st.GetArtifact(provider.Named(), store.ArtifactContent) != nil || st.GetArtifact(owner.Named(), store.ArtifactContent) != nil || len(st.Warnings()) != 0 {
		t.Fatal("shared acquisition published source content or treated an evaluated empty result as unavailable")
	}
}

func TestAcquire_CanceledMaterialDoesNotPark(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			t.Cleanup(tasks.BlockTillDone)
			owner := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
			provider := &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "registry"}
			st.AddObjects([]manifest.BaseManifest{owner, provider})
			run := gateExecution(New(st, tasks), owner.Named())
			defer run.Close()
			missing := &source.MissingSecretError{Owner: provider.Named(), Secret: manifest.NamedResource{Kind: manifest.KindSecret, Namespace: provider.Namespace, Name: "auth"}, Detail: "not found", Cause: cause}
			var got error
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
				got = run.Acquire(ctx, provider, Acquisition{
					AllowMissingSecrets: true,
					Fetch: func(context.Context) (*store.SourceArtifact, error) {
						return nil, missing
					},
					Complete: func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error {
						t.Error("canceled acquisition completed")
						return nil
					},
				}, manifest.DrainNone)
			})
			tasks.BlockTillDone()
			if !errors.Is(got, cause) || len(st.Warnings()) != 0 {
				t.Fatalf("canceled acquisition error=%v warnings=%+v", got, st.Warnings())
			}
			if pending, ok := got.(interface {
				Settle(context.Context, manifest.DrainLevel) error
			}); ok {
				t.Fatalf("canceled material parked: %v", pending)
			}
		})
	}
}
