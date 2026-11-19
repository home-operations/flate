package base

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/fetcherr"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/pkg/manifest"
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
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testAcquireOutcomesCase(t, &tc) },
		)
	}
}

//nolint:paralleltest // The test changes the process-wide logger.
func TestAcquire_UnavailableCredentialsLog(t *testing.T) {
	var logs bytes.Buffer
	prior, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	})))
	t.Cleanup(func() {
		slog.SetDefault(prior)
		log.SetOutput(writer)
		log.SetFlags(flags)
	})

	st := store.New()
	tasks := task.NewBounded(2)
	t.Cleanup(tasks.BlockTillDone)
	repo := &manifest.GitRepository{Name: "external", Namespace: "flux-system"}
	st.AddObject(repo)
	run := gateExecution(New(st, tasks), repo.Named())
	run.Missing = depwait.NewMissingPolicy([]string{manifest.KindSecret})
	defer run.Close()
	missing := source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "unavailable-auth", "not found")
	var got error
	completed := 0
	tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
		run.Dispatch(ctx, manifest.DrainCascade, t,
			func(t *testing.T, ctx context.Context, current *manifest.GitRepository, level manifest.DrainLevel) error {
				t.Helper()
				got = run.Acquire(ctx, current, Acquisition{
					Fetch: func(context.Context) (source.PreparedFetch, error) {
						run.Reads.GetObject(repo.Named())
						source.SecretLookup(&run.Reads)(repo.Namespace, "unavailable-auth")
						return source.PreparedFetch{}, missing
					},
					Complete: func(_ context.Context, artifact *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
						completed++
						if artifact != nil || !errors.Is(unavailable, missing) || level != manifest.DrainCascade {
							t.Errorf("completion artifact=%v unavailable=%v level=%d", artifact, unavailable, level)
						}
						return nil
					},
				}, level)
				return got
			})
	})
	tasks.BlockTillDone()
	if got != nil || completed != 1 {
		t.Fatalf("acquisition error=%v completions=%d", got, completed)
	}
	want := `{"level":"INFO","msg":"base: credentials unavailable",` +
		`"id":"GitRepository.source.toolkit.fluxcd.io/flux-system/external",` +
		`"err":"secret flux-system/unavailable-auth not found"}` + "\n"
	if logs.String() != want {
		t.Errorf("log = %q, want %q", logs.String(), want)
	}
	warnings := st.Warnings()
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnSourceCredentialsUnavailable ||
		warnings[0].Resource != repo.Named() || warnings[0].Message != missing.Error() {
		t.Errorf("credential warnings = %+v", warnings)
	}
}

func TestAcquire_SettlementContinuation(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	t.Cleanup(tasks.BlockTillDone)
	owner := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	provider := &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "registry"}
	for _, object := range []manifest.BaseManifest{owner, provider} {
		st.AddObject(object)
	}
	run := gateExecution(New(st, tasks), owner.Named())
	defer run.Close()
	missing := &source.MissingSecretError{
		Owner:   provider.Named(),
		Subject: manifest.DiagnosticFor(provider.Named(), provider),
		Secret:  manifest.NamedResource{Kind: manifest.KindSecret, Namespace: provider.Namespace, Name: "auth"},
		Detail:  "not found",
	}
	artifact := &store.SourceArtifact{
		Kind: manifest.KindResourceSetInputProvider,
		FS:   fstest.MapFS{"inputs.json": {Data: []byte(`[]`)}},
	}
	var fetches, retries, completions, closed, children int
	missing.RetryAmbient = func(context.Context) (source.PreparedFetch, error) {
		retries++
		prepared, err := fetchtest.Prepared(artifact, nil)
		prepared.Close = func() { closed++ }
		return prepared, err
	}
	acquisition := Acquisition{
		Fetch: func(context.Context) (source.PreparedFetch, error) {
			fetches++
			missing.Input.Facts = source.SecretLookup(&run.Reads)(provider.Namespace, missing.Secret.Name)
			return source.PreparedFetch{}, missing
		},
		Complete: func(_ context.Context, acquired *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
			completions++

			assert.ErrorIf(
				t,
				acquired != artifact || unavailable != nil || level != manifest.DrainCascade,
				"completion artifact=%v unavailable=%v level=%d",
				acquired,
				unavailable,
				level,
			)

			assert.Equal(t, closed, 0)
			return run.Acquire(t.Context(), provider, Acquisition{
				Fetch: func(context.Context) (source.PreparedFetch, error) {
					prepared, err := fetchtest.Prepared(artifact, nil)
					prepared.Close = func() { closed++ }
					return prepared, err
				},
				Complete: func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error {
					children++
					assert.Equal(t, closed, 0)
					return nil
				},
			}, level)
		},
	}
	tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
		run.Dispatch(
			ctx,
			manifest.DrainNone,
			run,
			func(run *Execution, ctx context.Context, _ *manifest.ResourceSet, level manifest.DrainLevel) error {
				return run.Acquire(ctx, provider, acquisition, level)
			},
		)
	})
	tasks.BlockTillDone()
	if run.Outcome() != schedule.Blocked || !slices.Equal(run.Blocked, []manifest.NamedResource{missing.Secret}) ||
		fetches != 1 ||
		retries != 0 ||
		completions != 0 {
		t.Fatalf(
			"initial outcome=%v blocked=%v fetches=%d retries=%d completions=%d",
			run.Outcome(),
			run.Blocked,
			fetches,
			retries,
			completions,
		)
	}
	tasks.Go(t.Context(), "settle", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
	tasks.BlockTillDone()
	if run.Outcome() != schedule.Terminal || fetches != 1 || retries != 1 || completions != 1 {
		t.Fatalf(
			"settled outcome=%v fetches=%d retries=%d completions=%d",
			run.Outcome(),
			fetches,
			retries,
			completions,
		)
	}

	assert.FatalIf(t, st.GetArtifact(provider.Named(), store.ArtifactContent) != nil ||
		st.GetArtifact(owner.Named(), store.ArtifactContent) != nil ||
		len(
			st.Warnings(),
		) != 0, "%v", "shared acquisition published source content or treated an evaluated empty result as unavailable")
	assert.Equal(t, children, 1)
	assert.Equal(t, closed, 2)
	run.Close()
	assert.Equal(t, closed, 2)
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
			for _, object := range []manifest.BaseManifest{owner, provider} {
				st.AddObject(object)
			}
			run := gateExecution(New(st, tasks), owner.Named())
			run.Missing = depwait.NewMissingPolicy([]string{manifest.KindSecret})
			defer run.Close()
			missing := &source.MissingSecretError{
				Owner:  provider.Named(),
				Secret: manifest.NamedResource{Kind: manifest.KindSecret, Namespace: provider.Namespace, Name: "auth"},
				Detail: "not found",
				Cause:  cause,
			}
			var got error
			closed := 0
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
				assert.NoError(t, run.Acquire(ctx, provider, Acquisition{
					Fetch: func(context.Context) (source.PreparedFetch, error) {
						prepared, err := fetchtest.Prepared(&store.SourceArtifact{}, nil)
						prepared.Close = func() { closed++ }
						return prepared, err
					},
					Complete: func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error { return nil },
				}, manifest.DrainNone))
				assert.Equal(t, closed, 1)
				got = run.Acquire(
					ctx,
					provider,
					Acquisition{
						Fetch: func(context.Context) (source.PreparedFetch, error) {
							return source.PreparedFetch{}, missing
						},
						Complete: func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error {
							t.Error("canceled acquisition completed")
							return nil
						},
					},
					manifest.DrainNone)
			})
			tasks.BlockTillDone()
			if !errors.Is(got, cause) || len(st.Warnings()) != 0 {
				t.Fatalf("canceled acquisition error=%v warnings=%+v", got, st.Warnings())
			}
			run.Close()
			assert.Equal(t, closed, 1)

			{
				pending, ok := got.(interface {
					Settle(context.Context, manifest.DrainLevel) error
				})
				assert.FatalIf(t, ok, "canceled material parked: %v", pending)
			}
		})
	}
}

func testAcquireOutcomesCase(t *testing.T, testCaseInput *struct {
	name string

	missing     bool
	anonymous   bool
	producer    bool
	allow       bool
	policy      bool
	failure     error
	unavailable bool
	hard        bool
},
) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded(2)
	t.Cleanup(tasks.BlockTillDone)
	owner := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	provider := &manifest.ResourceSetInputProvider{Name: "tags", Namespace: "registry"}
	for _, object := range []manifest.BaseManifest{owner, provider} {
		st.AddObject(object)
	}
	missing := &source.MissingSecretError{
		Owner:           provider.Named(),
		Subject:         manifest.DiagnosticFor(provider.Named(), provider),
		Detail:          "registry credentials unavailable",
		AnonymousDenied: testCaseInput.anonymous,
	}
	if testCaseInput.missing {
		missing.Secret = manifest.NamedResource{
			Kind:      manifest.KindSecret,
			Namespace: provider.Namespace,
			Name:      "auth",
		}
	}
	if testCaseInput.producer {
		declaration := manifest.NamedResource{
			Kind:      "ExternalSecret",
			Namespace: provider.Namespace,
			Name:      "auth",
		}
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					missing.Secret: {Productions: []manifest.Production{{Declaration: declaration}}},
				},
			},
		)
	}
	artifact := &store.SourceArtifact{Kind: manifest.KindResourceSetInputProvider, FS: fstest.MapFS{}}
	failure := testCaseInput.failure
	if testCaseInput.missing || testCaseInput.anonymous {
		missing.Cause = failure
		failure = missing
	}
	run := gateExecution(New(st, tasks), owner.Named())
	if testCaseInput.allow {
		run.Missing = depwait.NewMissingPolicy([]string{manifest.KindSecret})
	}
	defer run.Close()
	completed := 0
	var got error
	tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
		run.Dispatch(
			ctx,
			manifest.DrainCascade,
			t,
			func(t *testing.T, ctx context.Context, _ *manifest.ResourceSet, level manifest.DrainLevel) error {
				t.Helper()
				run.Reads.GetObject(provider.Named())
				got = run.Acquire(
					ctx,
					provider,
					Acquisition{
						ProviderOnly: testCaseInput.policy,
						Fetch: func(context.Context) (source.PreparedFetch, error) {
							if testCaseInput.missing {
								missing.Input.Facts = source.SecretLookup(
									&run.Reads,
								)(
									provider.Namespace,
									missing.Secret.Name,
								)
							}
							if failure != nil {
								return source.PreparedFetch{}, failure
							}
							return fetchtest.Prepared(artifact, nil)
						},
						Complete: func(_ context.Context, acquired *store.SourceArtifact, unavailable error, completedLevel manifest.DrainLevel) error {
							completed++

							assert.ErrorIf(
								t,
								(unavailable != nil) != testCaseInput.unavailable || completedLevel != level,
								"completion unavailable=%v level=%d",
								unavailable,
								completedLevel,
							)

							assert.ErrorIf(
								t,
								!testCaseInput.unavailable && acquired != artifact,
								"%v",
								"completion lost acquired content",
							)

							return nil
						},
					},
					level)
				return got
			},
		)
	})
	shouldReturn := testAcquireOutcomesCaseTail(
		t,
		&testAcquireOutcomesCaseTailInputs{
			tasks:         &tasks,
			got:           &got,
			testCaseInput: &testCaseInput,
			completed:     &completed,
			failure:       &failure,
			st:            &st,
			provider:      &provider,
			missing:       &missing,
			owner:         &owner,
		},
	)
	if shouldReturn {
		return
	}
}

func testAcquireOutcomesCaseTail(t *testing.T, tailCaseInput *testAcquireOutcomesCaseTailInputs) bool {
	t.Helper()
	(*tailCaseInput.tasks).
		BlockTillDone()

	assert.FatalIf(
		t,
		((*tailCaseInput.got) != nil) != (*tailCaseInput.testCaseInput).hard || (*tailCaseInput.completed) !=
			map[bool]int{true: 0, false: 1}[(*tailCaseInput.testCaseInput).hard],
		"error=%v completions=%d, want hard=%t",
		(*tailCaseInput.got),
		(*tailCaseInput.completed),
		(*tailCaseInput.testCaseInput).hard,
	)

	assert.FatalIf(
		t,
		(*tailCaseInput.testCaseInput).hard && !errors.Is((*tailCaseInput.got), (*tailCaseInput.failure)),
		"hard failure lost its cause: %v",
		(*tailCaseInput.got),
	)

	warnings := (*tailCaseInput.st).Warnings()
	if !(*tailCaseInput.testCaseInput).unavailable {
		assert.FatalIf(t, len(warnings) != 0, "unexpected warnings: %+v", warnings)

		return true
	}

	assert.FatalIf(t, len(warnings) != 1 || warnings[0].Resource != (*tailCaseInput.provider).Named() ||
		warnings[0].Subject != manifest.DiagnosticFor(
			(*tailCaseInput.provider).Named(),
			(*tailCaseInput.provider),
		) ||
		warnings[0].Category != manifest.WarnSourceCredentialsUnavailable, "provider diagnostic=%+v", warnings)

	var related []manifest.NamedResource
	if (*tailCaseInput.testCaseInput).missing {
		related = []manifest.NamedResource{(*tailCaseInput.missing).Secret}
	}
	if !slices.Equal(warnings[0].Related, related) {
		t.Fatalf("related=%v, want %v", warnings[0].Related, related)
	}

	{
		info, _ := (*tailCaseInput.st).GetStatus((*tailCaseInput.owner).
			Named(),
		)
		assert.FatalIf(t, info.Status != store.StatusReady ||
			info.Outcome() == store.OutcomeSkipped, "provider unavailability suppressed the consumer: %+v", info)
	}

	return false
}

type testAcquireOutcomesCaseTailInputs struct {
	tasks **task.
		Service

	got *error

	testCaseInput **struct {
		name string

		missing bool

		anonymous bool

		producer bool

		allow       bool
		policy      bool
		failure     error
		unavailable bool
		hard        bool
	}

	completed *int

	failure *error

	st **store.
		Store

	provider **manifest.
			ResourceSetInputProvider

	missing **source.
		MissingSecretError

	owner **manifest.
		ResourceSet
}

func TestAcquire_CapturedEvidence(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"unknown production", "encrypted production", "missing key production", "private preparation"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			t.Cleanup(tasks.BlockTillDone)
			owner := &manifest.GitRepository{Name: "repo", Namespace: "apps"}
			st.AddObject(owner)
			run := gateExecution(New(st, tasks), owner.Named())
			defer run.Close()
			facts := store.ReferenceFacts{Generated: true}
			if name != "unknown production" {
				secret := &manifest.Secret{StringData: map[string]any{"password": "readable"}}
				if name == "encrypted production" {
					secret.StringData["username"] = "ENC[AES256_GCM,data:opaque,iv:x,tag:y,type:str]"
				}
				facts.Data, facts.Original = secret, secret
			}
			if name == "private preparation" {
				facts.Private, facts.Prepared = true, true
			}
			_, _, err := source.BasicAuthFromSecret(facts, manifest.DiagnosticFor(owner.Named(), owner), "auth")
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok, "unavailable material: %v", err)
			closed, retries, completions := 0, 0, 0
			artifact := &store.SourceArtifact{}
			missing.RetryAmbient = func(context.Context) (source.PreparedFetch, error) {
				retries++
				return fetchtest.Prepared(artifact, nil)
			}
			acquisition := Acquisition{
				Fetch: func(context.Context) (source.PreparedFetch, error) {
					return source.PreparedFetch{Close: func() { closed++ }}, missing
				},
				Complete: func(_ context.Context, got *store.SourceArtifact, unavailable error, _ manifest.DrainLevel) error {
					completions++
					if name == "private preparation" {
						assert.Equal(t, got, artifact)
						assert.NoError(t, unavailable)
					} else {
						assert.Equal(t, got == nil, true)
						assert.Equal(t, unavailable, error(missing))
					}
					return nil
				},
			}
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
				run.Dispatch(
					ctx,
					manifest.DrainNone,
					owner,
					func(owner *manifest.GitRepository, ctx context.Context, _ manifest.BaseManifest, level manifest.DrainLevel) error {
						return run.Acquire(ctx, owner, acquisition, level)
					},
				)
			})
			tasks.BlockTillDone()
			assert.Equal(t, run.Outcome(), schedule.Blocked)
			assert.Diff(t, run.Blocked, []manifest.NamedResource{missing.Secret})
			assert.Equal(t, retries+closed+completions, 0)
			tasks.Go(t.Context(), "settle", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
			tasks.BlockTillDone()
			assert.Equal(t, run.Outcome(), schedule.Terminal)
			assert.Equal(t, completions, 1)
			assert.Equal(t, closed, 1)
			wantRetries, wantWarnings := 0, 1
			if name == "private preparation" {
				wantRetries, wantWarnings = 1, 0
			}
			assert.Equal(t, retries, wantRetries)
			assert.Equal(t, len(st.Warnings()), wantWarnings)
			run.Close()
			assert.Equal(t, closed, 1)
		})
	}
}
