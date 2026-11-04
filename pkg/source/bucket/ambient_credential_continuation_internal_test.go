package bucket

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type testFetcherMachineContinuationCaseInputs struct {
	requests *atomic.Int32
	access   *string
	rejected *bool
	tier     *string
}

func testFetcherMachineContinuationCase(t *testing.T, testCaseInput testFetcherMachineContinuationCaseInputs) {
	t.Helper()
	b := ambientBucket()
	st := store.New()
	(*testCaseInput.rejected) = (*testCaseInput.tier) == "rejected"
	if (*testCaseInput.tier) == "STS rejected" {
		clearMachineEnvironment(t)
		token := filepath.Join(t.TempDir(), "web-identity")
		testutil.WriteFileAt(t, token, "synthetic-token")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
	}
	switch {
	case (*testCaseInput.tier) == "public":
		b.SecretRef = nil
		(*testCaseInput.access) = ""
	case (*testCaseInput.tier) != "machine" && (*testCaseInput.tier) != "STS rejected":
		(*testCaseInput.access) = "repository"
		sec := &manifest.Secret{
			Name:       "auth",
			Namespace:  "apps",
			StringData: map[string]any{"accesskey": (*testCaseInput.access), "secretkey": "explicit-secret"},
		}
		if (*testCaseInput.tier) != "supply" {
			if (*testCaseInput.tier) == "partial supply" {
				sec.StringData["secretkey"] = manifest.ValuePlaceholder("secretkey")
			}
			st.AddObject(sec)
		}
		if (*testCaseInput.tier) == "supply" || (*testCaseInput.tier) == "partial supply" {
			supplied := &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{
				"accesskey": "supply", "secretkey": "supply-secret",
			}}
			st.AddDeclaration(
				store.Declaration{
					Obj:    supplied,
					Doc:    testutil.ObjectDocument(supplied),
					Origin: manifest.SuppliedDeclaration,
				},
			)
			if (*testCaseInput.tier) == "supply" {
				(*testCaseInput.access) = "supply"
			}
		}
	default:
		(*testCaseInput.access) = "machine-canary"
	}
	st.AddObject(b)
	var reads store.ReadSet
	reads.Init(st, b.Named(), nil)
	defer reads.Close()
	root := ""
	if (*testCaseInput.tier) == "machine" {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
	initialRequests := testCaseInput.requests.Load()
	artifact, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
	if (*testCaseInput.tier) == "rejected" {
		assert.FatalIf(
			t,
			err == nil || artifact != nil || !errors.Is(err, manifest.ErrMissingSecret) || testCaseInput.requests.
				Load() != initialRequests+1,
			"explicit rejection advanced credential tiers: %v",
			err,
		)
		return
	}
	if (*testCaseInput.tier) == "STS rejected" {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
		artifact, err = missing.RetryAmbient(t.Context())
		rejection, rejected := errors.AsType[credentials.ErrorResponse](err)
		assert.FatalIf(t, artifact != nil || !rejected || rejection.STSError.Code != "AccessDenied" ||
			errors.Is(err, manifest.ErrMissingSecret) ||
			strings.Contains(err.Error(), "canary") || testCaseInput.requests.
			Load() == initialRequests, "machine STS rejection lost its cause or exposed credentials: %v", err)
		return
	}
	testFetcherMachineContinuationCaseCompletion(
		t,
		&testFetcherMachineContinuationCaseCompletionInputs{
			testCaseInput: &testCaseInput,
			err:           &err,
			artifact:      &artifact,
			f:             &f,
			reads:         &reads,
			b:             &b,
			root:          &root,
		},
	)
}

func testFetcherMachineContinuationCaseCompletion(
	t *testing.T,
	captures *testFetcherMachineContinuationCaseCompletionInputs,
) {
	t.Helper()
	if (*captures.testCaseInput.tier) == "machine" {
		missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err))
		assert.FatalIf(
			t,
			!ok || missing.RetryAmbient == nil || (*captures.artifact) != nil,
			"missing auth did not retain continuation: %v", (*captures.err),
		)
		before, prepErr := (&operation{Fetcher: (*captures.f), secrets: source.SecretLookup(captures.reads)}).prepare(
			(*captures.b),
		)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-before-first-acquisition")
		after, prepErr := (&operation{Fetcher: (*captures.f), secrets: source.SecretLookup(captures.reads)}).prepare(
			(*captures.b),
		)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		assert.FatalIf(t, before.material != nil || after.material != nil ||
			before.options.Transport != after.options.Transport, "%s", "machine values changed transport identity")
		(*captures.artifact), (*captures.err) = missing.RetryAmbient(t.Context())
	}
	assert.NoError(t, (*captures.err))
	assert.FatalIf(t, (*captures.artifact) == nil, "%s", "acquisition returned no source")

	if len((*captures.artifact).Warnings) != 0 {
		t.Fatalf("advisory count=%d, want %d", len((*captures.artifact).Warnings), 0)
	}
	var data []byte

	data, (*captures.err) = fs.ReadFile((*captures.artifact).FS, "file.yaml")
	assert.NoError(t, (*captures.err))
	assert.Equal(t, string(data), "data")
	assert.FatalIf(t, strings.Contains(
		fmt.Sprint(
			(*captures.artifact).Key,
			(*captures.artifact).Content,
			(*captures.artifact).Metadata,
			(*captures.artifact).Warnings,
		),
		"canary",
	), "%s", "machine credentials entered artifact identity or output")

	before := captures.testCaseInput.requests.Load()
	var warm *store.SourceArtifact

	warm, (*captures.err) = (*captures.f).Fetch(
		t.Context(),
		(*captures.b),
		captures.reads,
		cache.Policy{MaxAge: time.Hour},
	)
	if missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err)); ok && missing.RetryAmbient != nil {
		warm, (*captures.err) = missing.RetryAmbient(t.Context())
	}
	assert.NoError(t, (*captures.err))
	assert.FatalIf(
		t,
		warm.Key != (*captures.artifact).Key || warm.Content != (*captures.artifact).Content || len(warm.Warnings) != 0 ||
			captures.testCaseInput.
				requests.
				Load() != before,
		"%s",
		"warm acquisition consulted credentials, changed identity or repeated success information",
	)

	if (*captures.testCaseInput.tier) == "machine" {
		(*captures.testCaseInput.access) = "rotated-canary"
		t.Setenv("AWS_ACCESS_KEY_ID", (*captures.testCaseInput.access))
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret-canary")
		_, fetchErr := (*captures.f).Fetch(t.Context(), (*captures.b), captures.reads, cache.Policy{MaxAge: time.Hour})
		missing, ok := errors.AsType[*source.MissingSecretError](fetchErr)
		if !ok {
			t.Fatal(fetchErr)
		}
		rotated, fetchErr := missing.RetryMachine(t.Context(), captures.reads)
		assert.NoError(t, fetchErr)
		assert.FatalIf(
			t,
			rotated.Key == warm.Key || captures.testCaseInput.requests.Load() == before,
			"%s",
			"rotation reused credential identity or skipped signing",
		)

		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		testutil.WriteFileAt(t, os.Getenv("AWS_CONFIG_FILE"), "[default]\naws_access_key_id = partial\n")
		beforeFailure := captures.testCaseInput.requests.Load()
		failed, fetchErr := missing.RetryMachine(t.Context(), captures.reads)
		assert.FatalIf(
			t,
			failed != nil || fetchErr == nil || errors.Is(fetchErr, manifest.ErrMissingSecret) ||
				captures.testCaseInput.
					requests.
					Load() != beforeFailure,
			"failed preparation reused warm content: %v",
			fetchErr,
		)
	}
	var warmData []byte

	warmData, (*captures.err) = fs.ReadFile(warm.FS, "file.yaml")
	assert.NoError(t, (*captures.err))
	assert.Equal(t, string(warmData), string(data))
	if (*captures.root) != "" {
		assert.NoError(t, filepath.WalkDir((*captures.root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			payload, err := os.ReadFile(path)
			if strings.Contains(string(payload), "canary") {
				t.Error("machine credentials or success provenance entered persistent cache data")
			}
			return err
		}))
	}
}

type testFetcherMachineContinuationCaseCompletionInputs struct {
	testCaseInput *testFetcherMachineContinuationCaseInputs
	err           *error
	artifact      **store.SourceArtifact
	f             **Fetcher
	reads         *store.ReadSet
	b             **manifest.Bucket
	root          *string
}
