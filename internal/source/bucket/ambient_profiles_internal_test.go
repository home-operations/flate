package bucket

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testFetcherMachineProfilesCase(t *testing.T, shape *string,
) {
	t.Helper()
	clearMachineEnvironment(t)
	root := t.TempDir()
	marker := filepath.Join(root, "executed")
	program := filepath.Join(root, "credential-process")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + filepath.Join(
		root,
		"args",
	) + "'\n/bin/cat > '" + filepath.Join(
		root,
		"stdin",
	) + "'\n/usr/bin/env > '" + filepath.Join(
		root,
		"environment",
	) + "'\nprintf called > '" + marker + "'\nprintf '%s' '{\"Version\":1,\"AccessKeyId\":\"file-canary\",\"SecretAccessKey\":\"file-secret\"}'\n"
	if (*shape) == "failing process" {
		script = "#!/bin/sh\nprintf '%s' 'credential-canary' >&2\nexit 1\n"
	}
	testutil.WriteFileAt(t, program, script)
	assert.NoError(t, os.Chmod(program, 0o700))
	profile := "[default]\ncredential_process = " + program + " argument\n"
	switch *shape {
	case "static before process":
		profile += "aws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
	case "environment before files":
		t.Setenv("AWS_ACCESS_KEY_ID", "environment")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		profile = "invalid INI ["
	case "partial":
		profile += "aws_access_key_id = partial\n"
	case "missing executable":
		profile = "[default]\ncredential_process = " + filepath.Join(root, "absent") + " argument\n"
	case "missing profile":
		t.Setenv("AWS_PROFILE", "absent")
	case "no default profile":
		profile = "[profile other]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
	case "selected profile":
		t.Setenv("AWS_PROFILE", "selected")
		profile = "[profile selected]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
	case "shared credentials":
		profile = "[default]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
	case "malformed INI":
		profile = "[unterminated"
	case "absent SSO token":
		profile = "[default]\nsso_role_name = role\nsso_account_id = 123456789012\nsso_start_url = https://sso.invalid/start\nsso_region = us-east-1\n"
	}
	path := os.Getenv("AWS_CONFIG_FILE")
	if (*shape) == "shared credentials" {
		path = os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	}
	if (*shape) != "no files" {
		testutil.WriteFileAt(t, path, profile)
	}
	var remote, iam atomic.Int32
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		iam.Add(1)
		_, _ = io.WriteString(w, `{"Code":"Success","Expiration":"2099-01-01T00:00:00Z"}`)
	}))
	t.Cleanup(metadata.Close)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
	var signer atomic.Value
	signer.Store("file-canary")
	switch *shape {
	case "environment before files":
		signer.Store("environment")
	case "no files":
		signer.Store("")
	}
	testFetcherMachineProfilesCaseCompletion(
		t,
		&testFetcherMachineProfilesCaseCompletionInputs{
			remote:  &remote,
			signer:  &signer,
			shape:   &shape,
			iam:     &iam,
			marker:  &marker,
			root:    &root,
			path:    &path,
			profile: &profile,
			program: &program,
			script:  &script,
		},
	)
}

func testFetcherMachineProfilesCaseCompletion(t *testing.T, captures *testFetcherMachineProfilesCaseCompletionInputs) {
	t.Helper()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captures.remote.
			Add(1)
		access := captures.signer.Load().(string)
		if access == "" && r.Header.Get("Authorization") != "" ||
			access != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+access+"/") {
			t.Error("profile acquisition used a different signer")
		}
		_, _ = io.WriteString(
			w,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
		)
	}))
	t.Cleanup(endpoint.Close)
	b := ambientBucket()
	b.Endpoint, b.Insecure = endpoint.URL, true
	inputs := testInputs{}
	ca, proxyAddress := "", ""
	if (*(*captures.shape)) == "process" {
		t.Setenv("FLATE_PROCESS_MACHINE", "machine-environment")
		ca = testutil.SelfSignedCA(t)
		proxy := httptest.NewServer(endpoint.Config.Handler)
		t.Cleanup(proxy.Close)
		proxyAddress = proxy.URL
		b.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
		b.CertSecretRef = b.ProxySecretRef
		inputs.secrets = func(string, string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{
				"address":  proxy.URL,
				"username": "repository-proxy-user",
				"password": "repository-proxy-password",
				"ca.crt":   ca,
			}}
		}
	}
	reads := inputs.readSet(b)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{MaxAge: time.Hour}, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(
		t,
		!ok || missing.RetryAmbient == nil || artifact != nil || captures.remote.Load() != 0 ||
			captures.iam.Load() != 0,
		"profile lookup ran before continuation: %v",
		err,
	)
	artifact, err = fetchtest.Machine(t.Context(), missing, reads)
	success := (*(*captures.shape)) == "process" || (*(*captures.shape)) == "static before process" ||
		(*(*captures.shape)) == "environment before files" ||
		(*(*captures.shape)) ==
			"selected profile" ||
		(*(*captures.shape)) ==
			"shared credentials"
	unavailable := (*(*captures.shape)) == "no files"
	var wantRemote, wantIAM int32
	if success || unavailable {
		wantRemote = 1
	}
	if unavailable {
		wantIAM = 1
	}
	testFetcherMachineProfilesCaseCompletionTail(
		t,
		&testFetcherMachineProfilesCaseCompletionTailState{
			captures:     &captures,
			ca:           &ca,
			proxyAddress: &proxyAddress,
			reads:        &reads,
			artifact:     &artifact,
			err:          &err,
			missing:      &missing,
			success:      &success,
			unavailable:  &unavailable,
			wantRemote:   &wantRemote,
			wantIAM:      &wantIAM,
		},
	)
}

type testFetcherMachineProfilesCaseCompletionInputs struct {
	remote  *atomic.Int32
	signer  *atomic.Value
	shape   **string
	iam     *atomic.Int32
	marker  *string
	root    *string
	path    *string
	profile *string
	program *string
	script  *string
}

type testFetcherMachineProfilesCaseCompletionTailState struct {
	captures     **testFetcherMachineProfilesCaseCompletionInputs
	ca           *string
	proxyAddress *string
	reads        **store.ReadSet
	artifact     **store.SourceArtifact
	err          *error
	missing      **source.MissingSecretError
	success      *bool
	unavailable  *bool
	wantRemote   *int32
	wantIAM      *int32
}

func testFetcherMachineProfilesCaseCompletionTail(
	t *testing.T,
	scope *testFetcherMachineProfilesCaseCompletionTailState,
) {
	t.Helper()
	if ((*scope.err) == nil) != ((*scope.success) || (*scope.unavailable)) ||
		((*scope.artifact) != nil) != ((*scope.success) || (*scope.unavailable)) ||
		errors.Is((*scope.err), manifest.ErrMissingSecret) ||
		(*scope.captures).remote.
			Load() != (*scope.wantRemote) ||
		(*scope.captures).iam.
			Load() != (*scope.wantIAM) {
		t.Fatalf(
			"profile selection: %v remote=%d IAM=%d",
			(*scope.err), (*scope.captures).remote.Load(), (*scope.captures).iam.Load(),
		)
	}
	_, markerErr := os.Stat((*(*scope.captures).marker))
	assert.FatalIf(
		t,
		(markerErr == nil) != ((*(*(*scope.captures).shape)) == "process"),
		"credential_process precedence: %v",
		markerErr,
	)
	if (*(*(*scope.captures).shape)) == "process" {
		args, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "args"))
		assert.NoError(t, err)
		assert.Equal(t, string(args), "argument\n")
		stdin, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "stdin"))
		assert.NoError(t, err)
		assert.Equal(t, string(stdin), "")
		environment, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "environment"))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!strings.Contains(string(environment), "FLATE_PROCESS_MACHINE=machine-environment"),
			"%s",
			"credential process lost the machine environment",
		)

		for _, canary := range []string{(*scope.proxyAddress), "repository-proxy-user", "repository-proxy-password", (*scope.ca), "-----BEGIN CERTIFICATE-----"} {
			assert.FatalIf(
				t,
				strings.Contains(string(args)+string(stdin)+string(environment), canary),
				"%s",
				"repository transport material reached credential process inputs",
			)
		}
	}
	if *scope.success {
		warm, err := fetchtest.Machine(t.Context(), (*scope.missing), (*scope.reads))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			warm.Key != (*scope.artifact).Key || (*scope.captures).remote.Load() != 1,
			"%s",
			"unchanged profile missed warm content",
		)
		(*scope.captures).signer.
			Store("rotated-canary")
		if (*(*(*scope.captures).shape)) == "environment before files" {
			t.Setenv("AWS_ACCESS_KEY_ID", "rotated-canary")
		} else {
			testutil.WriteFileAt(
				t,
				(*(*scope.captures).path),
				strings.ReplaceAll((*(*scope.captures).profile), "file-canary", "rotated-canary"),
			)
			testutil.WriteFileAt(
				t,
				(*(*scope.captures).program),
				strings.ReplaceAll((*(*scope.captures).script), "file-canary", "rotated-canary"),
			)
		}
		rotated, err := fetchtest.Machine(t.Context(), (*scope.missing), (*scope.reads))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			rotated.Key == warm.Key || (*scope.captures).remote.Load() != 2 || (*scope.captures).iam.Load() != 0,
			"%s",
			"rotated profile reused credential identity or skipped signing",
		)
	}
}
