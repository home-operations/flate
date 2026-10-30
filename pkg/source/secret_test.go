package source

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/home-operations/flate/pkg/source/cache"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSecretLookup_References(t *testing.T) {
	t.Parallel()
	if SecretLookup(nil) != nil {
		t.Fatal("unwired capture became wired")
	}
	ca := testutil.SelfSignedCA(t)
	first := &manifest.Secret{Name: "shared", Namespace: "ns", StringData: map[string]any{
		"username": "alice", "password": "first", "ca.crt": ca, "address": "http://proxy.invalid",
	}}
	s := store.New()
	s.AddObject(first)
	var reads store.ReadSet
	owner := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"}
	reads.Init(s, owner, nil)
	secrets := SecretLookup(&reads)
	assert.Equal(t, secrets("ns", "shared"), first)
	assert.Equal(t, secrets("ns", "missing"), (*manifest.Secret)(nil))
	current := &manifest.Secret{Name: "shared", Namespace: "ns", StringData: map[string]any{"username": "rotated"}}
	s.AddObject(current)
	s.AddObject(&manifest.Secret{Name: "missing", Namespace: "ns"})
	assert.Equal(t, secrets("ns", "shared"), first)
	assert.Equal(t, secrets("ns", "missing"), (*manifest.Secret)(nil))
	other := *current
	other.Namespace = "other"
	s.AddObject(&other)
	assert.Equal(t, secrets("other", "shared"), &other)
	username, password, err := BasicAuthFromSecret(secrets("ns", "shared"), manifest.DiagnosticFor(owner, nil), "shared")
	assert.NoError(t, err)
	assert.Equal(t, username, "alice")
	assert.Equal(t, password, "first")
	ref := &manifest.LocalObjectReference{Name: "shared"}
	material, err := ResolveTransport(secrets, manifest.DiagnosticFor(owner, nil), []TransportInput{
		{Reference: ref, Use: TransportCert}, {Reference: ref, Use: TransportProxy},
	})
	assert.NoError(t, err)
	assert.Equal(t, material.Certificate()[2], ca)
	var next store.ReadSet
	next.Init(s, owner, nil)
	assert.Equal(t, SecretLookup(&next)("ns", "shared"), current)
	if SecretLookup(&next)("ns", "missing") == nil {
		t.Fatal("next execution retained the preceding absence")
	}
}

func TestMissingSecretErr(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, secret, want string }{
		{name: "Secret material", secret: "ghcr-creds", want: "secret ns/ghcr-creds not found"},
		{name: "source material", want: "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := manifest.NamedResource{Group: "input.example", Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}
			err := MissingSecretErr(manifest.DiagnosticFor(owner, nil), tc.secret, "not found")
			if got := err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
			if !errors.Is(err, manifest.ErrMissingSecret) {
				t.Error("errors.Is(err, ErrMissingSecret) = false, want true")
			}
			mse, ok := errors.AsType[*MissingSecretError](fmt.Errorf("git clone: %w", err))
			if !ok {
				t.Fatal("errors.AsType did not recover *MissingSecretError through a wrap")
			}
			var wantSecret manifest.NamedResource
			if tc.secret != "" {
				wantSecret = manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: tc.secret}
			}
			if mse.Secret != wantSecret {
				t.Errorf("recovered Secret = %v, want %v", mse.Secret, wantSecret)
			}
			wantOwner := manifest.NamedResource{Group: owner.Group, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}
			if mse.Owner != wantOwner || owner.Group != "input.example" {
				t.Errorf("recovered Owner = %v, want %v; input = %v", mse.Owner, wantOwner, owner)
			}
		})
	}
}

func TestBasicAuthFromSecret_UnavailableMaterial(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	encodedCipher := base64.StdEncoding.EncodeToString([]byte(cipher))
	owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "ns", Name: "repo"}
	for _, tc := range []struct {
		name               string
		data, stringData   map[string]any
		field              string
		username, password string
		metadata           bool
	}{
		{name: "absent"},
		{name: "empty username", stringData: map[string]any{"username": "", "password": "plain"}},
		{name: "empty password", stringData: map[string]any{"username": "user", "password": ""}},
		{name: "wiped username", stringData: map[string]any{"username": "..PLACEHOLDER_username..", "password": "plain"}},
		{name: "wiped password", stringData: map[string]any{"username": "user", "password": "..PLACEHOLDER_password.."}},
		{name: "encoded wiped username", data: map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_username.."))}, stringData: map[string]any{"password": "plain"}},
		{name: "encoded wiped password", data: map[string]any{"password": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_password.."))}, stringData: map[string]any{"username": "user"}},
		{name: "malformed username", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"password": "plain"}, field: "username"},
		{name: "malformed password", data: map[string]any{"password": "credential-payload!"}, stringData: map[string]any{"username": "user"}, field: "password"},
		{name: "nonscalar username", stringData: map[string]any{"username": []any{"credential-payload"}, "password": "plain"}, field: "username"},
		{name: "nonscalar password", stringData: map[string]any{"username": "user", "password": map[string]any{"secret": "credential-payload"}}, field: "password"},
		{name: "missing username malformed password", data: map[string]any{"password": "credential-payload!"}, field: "password"},
		{name: "wiped username malformed password", data: map[string]any{"password": "credential-payload!"}, stringData: map[string]any{"username": "..PLACEHOLDER_username.."}, field: "password"},
		{name: "malformed username wiped password", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"password": "..PLACEHOLDER_password.."}, field: "username"},
		{name: "both malformed choose username", data: map[string]any{"username": "credential-payload!", "password": "credential-payload!"}, field: "username"},
		{name: "raw placeholder is malformed", data: map[string]any{"username": "..PLACEHOLDER_username.."}, stringData: map[string]any{"password": "plain"}, field: "username"},
		{name: "valid", stringData: map[string]any{"username": "user", "password": "plain"}, username: "user", password: "plain"},
		{name: "valid encoded", data: map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("user")), "password": base64.StdEncoding.EncodeToString([]byte("plain"))}, username: "user", password: "plain"},
		{name: "shadowed malformed data", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"username": "user", "password": "plain"}, username: "user", password: "plain"},
		{name: "clear metadata credentials", stringData: map[string]any{"username": "user", "password": "plain"}, username: "user", password: "plain", metadata: true},
		{name: "encoded clear metadata credentials", data: map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("user")), "password": base64.StdEncoding.EncodeToString([]byte("plain"))}, username: "user", password: "plain", metadata: true},
		{name: "ciphertext username", stringData: map[string]any{"username": cipher, "password": "plain"}, metadata: true},
		{name: "ciphertext password", stringData: map[string]any{"username": "user", "password": cipher}},
		{name: "encoded ciphertext username", data: map[string]any{"username": encodedCipher}, stringData: map[string]any{"password": "plain"}, metadata: true},
		{name: "partial decode beside wiped password", data: map[string]any{"username": encodedCipher + "!"}, stringData: map[string]any{"password": cipher}, field: "username", metadata: true},
		{name: "raw ciphertext encoded field", data: map[string]any{"username": cipher}, stringData: map[string]any{"password": cipher}, metadata: true},
		{name: "unsupported scalar beside wiped password", stringData: map[string]any{"username": []any{"credential-payload"}, "password": cipher}, field: "username", metadata: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"apiVersion": "v1", "kind": manifest.KindSecret, "metadata": map[string]any{"name": "metadata-secret", "namespace": "elsewhere"}, "data": tc.data, "stringData": tc.stringData}
			if tc.metadata {
				doc["sops"] = map[string]any{"version": "3.13.3", "mac": "metadata"}
			}
			obj, _, _, parseErr := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: true})
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			sec := obj.(*manifest.Secret)
			username, password, err := BasicAuthFromSecret(sec, manifest.DiagnosticFor(owner, nil), "creds")
			if tc.username != "" {
				if err != nil || username != tc.username || password != tc.password {
					t.Fatal("valid credentials were not preserved")
				}
				return
			}
			if username != "" || password != "" {
				t.Fatal("credential failure returned partial credentials")
			}
			if tc.field != "" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrMissingSecret) {
					t.Fatalf("expected hard input failure, got %v", err)
				}
				for _, context := range []string{"secret ns/creds", "[" + tc.field + "]"} {
					if !strings.Contains(err.Error(), context) {
						t.Errorf("input error missing context %q: %v", context, err)
					}
				}
				if strings.Contains(err.Error(), "credential-payload") {
					t.Error("input error exposed credential payload")
				}
				return
			}
			missing, ok := errors.AsType[*MissingSecretError](err)
			if !ok || !errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("expected MissingSecretError, got %v", err)
			}
			wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}
			if missing.Owner != owner || missing.Secret != wantSecret {
				t.Fatalf("credential identities = %+v", missing)
			}
		})
	}
}

func TestSecretLookup_SuppliedAuthTLSAndProxy(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	supplied := &manifest.Secret{Name: "material", Namespace: "apps", StringData: map[string]any{"username": "supplied", "password": "filled", "ca.crt": ca, "address": "http://proxy.invalid"}}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: supplied}})
	assert.NoError(t, err)
	s := store.New()
	s.SetSupplied(snapshot)
	repository := &manifest.Secret{Name: supplied.Name, Namespace: supplied.Namespace, StringData: map[string]any{"username": "repository", "password": manifest.ValuePlaceholder("password")}}
	s.AddObject(repository)
	owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
	var reads store.ReadSet
	reads.Init(s, owner, nil)
	defer reads.Close()
	lookup := SecretLookup(&reads)
	effective := lookup(owner.Namespace, supplied.Name)
	username, password, err := BasicAuthFromSecret(effective, manifest.DiagnosticFor(owner, nil), supplied.Name)
	assert.NoError(t, err)
	assert.Equal(t, username, "repository")
	assert.Equal(t, password, "filled")
	ref := &manifest.LocalObjectReference{Name: supplied.Name}
	material, err := ResolveTransport(lookup, manifest.DiagnosticFor(owner, nil), []TransportInput{{Reference: ref, Use: TransportSecret, OptionalTLS: true, CAFallback: true}, {Reference: ref, Use: TransportProxy}})
	assert.NoError(t, err)
	assert.Equal(t, material.Certificate()[2], ca)
	assert.Diff(t, material.ProxyComponents(), [3]string{"http://proxy.invalid", "repository", "filled"})
	if s.GetObject(supplied.Named()) != repository || repository.StringData["password"] != manifest.ValuePlaceholder("password") {
		t.Fatal("effective credential material entered canonical state")
	}
}

func TestAcquireMachine_Lifecycle(t *testing.T) {
	t.Parallel()
	cause := &fs.PathError{Op: "open", Path: "credential-canary", Err: fs.ErrPermission}
	for _, phase := range []string{"pre-canceled", "unavailable", "prepare failure", "expired preparation", "success", "unbounded", "acquire failure"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if phase == "pre-canceled" {
				cancel()
			}
			timeout := time.Hour
			if phase == "expired preparation" {
				timeout = -1
			}
			policy := cache.Policy{Timeout: &timeout, MaxAge: time.Hour, Retry: cache.RetryConfig{Attempts: 3}}
			if phase == "unbounded" {
				policy.Timeout = nil
			}
			prepared, acquired := 0, 0
			var preparation context.Context
			partial := &store.SourceArtifact{Warnings: []manifest.Warning{{Category: manifest.WarnSourceCredentialsUnavailable}}}
			artifact, err := AcquireMachine(ctx, policy, func(ctx context.Context) error {
				prepared++
				preparation = ctx
				if phase == "unavailable" {
					return MissingSecretErr(manifest.DiagnosticIdentity{}, "", "unavailable")
				}
				if phase == "prepare failure" {
					return cause
				}
				return nil
			}, func(got context.Context, p cache.Policy) (*store.SourceArtifact, error) {
				acquired++
				if got != ctx || got.Err() != nil || preparation.Err() == nil || !p.Machine || p.Timeout != policy.Timeout || p.MaxAge != policy.MaxAge || p.Retry != policy.Retry {
					t.Fatal("preparation cancellation or acquisition policy changed")
				}
				if phase == "acquire failure" {
					return partial, cause
				}
				return partial, nil
			})
			switch phase {
			case "pre-canceled":
				if prepared != 0 || acquired != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled lifecycle: %d/%d %v", prepared, acquired, err)
				}
			case "unavailable":
				if prepared != 1 || acquired != 0 || !errors.Is(err, manifest.ErrMissingSecret) {
					t.Fatalf("unavailable: %d/%d %v", prepared, acquired, err)
				}
			case "expired preparation":
				if acquired != 0 || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("late success admitted acquisition: %v", err)
				}
			case "prepare failure", "acquire failure":
				if !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) || strings.Contains(err.Error(), "canary") {
					t.Fatalf("classification lost cause or exposed material: %v", err)
				}
				if phase == "prepare failure" && acquired != 0 || phase == "acquire failure" && artifact != partial {
					t.Fatal("changed partial result or acquired after preparation failure")
				}
			case "success", "unbounded":
				if err != nil || artifact != partial || prepared != 1 || acquired != 1 {
					t.Fatalf("successful lifecycle: %d/%d %v", prepared, acquired, err)
				}
			}
		})
	}
}

func TestMissingSecretError_RetryMachine(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"nil reads", "source only", "producer", "unavailable", "canceled", "no callback"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			owner := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "source"}
			err := MissingSecretErr(manifest.DiagnosticFor(owner, nil), "auth", "not found")
			missing, _ := errors.AsType[*MissingSecretError](err)
			calls := 0
			missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
				calls++
				if shape == "unavailable" {
					return nil, MissingSecretErr(manifest.DiagnosticFor(owner, nil), "", "unavailable")
				}
				return &store.SourceArtifact{}, nil
			}
			var reads *store.ReadSet
			if shape == "source only" {
				missing.Secret = manifest.NamedResource{}
			}
			if shape == "producer" {
				s := store.New()
				declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "auth", Namespace: "ns"}
				s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{missing.Secret: {Productions: []manifest.Production{{Declaration: declaration.Named()}}}}})
				reads = &store.ReadSet{}
				reads.Init(s, owner, nil)
				defer reads.Close()
				if !missing.ProducerBacked(reads) {
					t.Fatal("missing captured declaration")
				}
			}
			if shape == "no callback" {
				missing.RetryAmbient = nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if shape == "canceled" {
				cancel()
			}
			artifact, got := missing.RetryMachine(ctx, reads)
			switch shape {
			case "producer", "no callback", "unavailable":
				if got != missing || artifact != nil || calls != map[string]int{"unavailable": 1}[shape] { //nolint:errorlint // Unavailable retries MUST retain the original error identity.
					t.Fatalf("missing identity or eligibility changed: %v calls=%d", got, calls)
				}
			case "canceled":
				if !errors.Is(got, context.Canceled) || calls != 0 {
					t.Fatalf("canceled retry: %v", got)
				}
			default:
				if got != nil || artifact == nil || calls != 1 {
					t.Fatalf("eligible retry: %v calls=%d", got, calls)
				}
			}
		})
	}
}
