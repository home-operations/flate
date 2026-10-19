package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	objectcache "github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

// TestAutoResolve_ExplicitBaseFetchedFromOrigin pins the fetch fallback:
// a checkout that carries an origin remote but none of the baseline's
// history (an agent's file dump over `git init`, a fetch-depth=1 clone
// of another branch) fetches the rev from origin instead of failing,
// whatever form the rev takes.
func TestAutoResolve_ExplicitBaseFetchedFromOrigin(t *testing.T) {
	t.Parallel()
	remote := t.TempDir()
	mainCommit := initRepoWithFile(t, remote, "a.yaml", "base")
	remoteRepo := openHelper(t, remote)
	setRef(t, remoteRepo, plumbing.NewBranchReferenceName("main"), mainCommit)
	if _, err := remoteRepo.CreateTag("v1", mainCommit, &git.CreateTagOptions{
		Tagger:  &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
		Message: "v1",
	}); err != nil {
		t.Fatal(err)
	}
	url := filepath.Join(remote, ".git")

	cases := []struct{ name, base string }{
		{"branch", "main"},
		{"remote-qualified branch", "origin/main"},
		{"tag", "v1"},
		{"full ref", "refs/heads/main"},
		{"sha", mainCommit.String()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := historylessCheckout(t, url, "a.yaml", "pr-tip")
			res, err := AutoResolve(t.Context(), dir, tc.base, cachetest.New(t, "", sourcecache.New))
			if err != nil {
				t.Fatalf("AutoResolve(--base=%s): %v", tc.base, err)
			}

			got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
			if err != nil {
				t.Fatalf("read materialized: %v", err)
			}
			if string(got) != "base" {
				t.Errorf("materialized a.yaml = %q, want %q (origin's main, not the checkout)", got, "base")
			}
			if res.BaseInput.Revision != mainCommit.String() {
				t.Errorf("Revision = %q, want %q", res.BaseInput.Revision, mainCommit.String())
			}
			if !strings.Contains(res.Source.Metadata["baseline-selection"], "fetched from origin") {
				t.Errorf("Source = %q, want it to say the rev was fetched from origin", res.Source.Metadata["baseline-selection"])
			}
			// The checkout is read-only: the fetch lands nowhere near it.
			if _, err := openHelper(t, dir).CommitObject(mainCommit); err == nil {
				t.Error("baseline commit landed in the checkout; the fetch must not write into it")
			}
		})
	}
}

// TestAutoResolve_ExplicitBaseFetchedDespiteLocalHistory: local history
// that simply lacks the rev takes the same fallback, so `--base main`
// works in a shallow clone of a feature branch.
func TestAutoResolve_ExplicitBaseFetchedDespiteLocalHistory(t *testing.T) {
	t.Parallel()
	remote := t.TempDir()
	mainCommit := initRepoWithFile(t, remote, "a.yaml", "base")
	setRef(t, openHelper(t, remote), plumbing.NewBranchReferenceName("main"), mainCommit)

	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "pr-tip")
	addOrigin(t, dir, filepath.Join(remote, ".git"))

	res, err := AutoResolve(t.Context(), dir, "main", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve(--base=main): %v", err)
	}

	if res.BaseInput.Revision != mainCommit.String() {
		t.Errorf("Revision = %q, want %q", res.BaseInput.Revision, mainCommit.String())
	}
}

// TestAutoResolve_ExplicitBaseNotOnOrigin: the fallback ran and origin
// has no such ref either; the error says where it looked.
func TestAutoResolve_ExplicitBaseNotOnOrigin(t *testing.T) {
	t.Parallel()
	remote := t.TempDir()
	initRepoWithFile(t, remote, "a.yaml", "base")
	dir := historylessCheckout(t, filepath.Join(remote, ".git"), "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "nope", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected error for a rev origin does not have")
	}
	for _, want := range []string{"nope", "origin"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

// TestAutoResolve_ExplicitBaseNoOriginRemote: nothing local and no
// origin to fetch from; the error names the missing remote.
func TestAutoResolve_ExplicitBaseNoOriginRemote(t *testing.T) {
	t.Parallel()
	dir := historylessCheckout(t, "", "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "main", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, errors.Is(err, plumbing.ErrReferenceNotFound), true)
	if err == nil {
		t.Fatal("expected error without an origin remote")
	}
	if !strings.Contains(err.Error(), "origin remote") {
		t.Errorf("error %q should say there is no origin remote to fetch from", err)
	}
}

// TestAutoResolve_UnbornHEADWithoutBase: auto-detection needs a HEAD
// commit to merge-base from, so a history-less checkout must be told
// to pass --base rather than failing on a bare ref lookup.
func TestAutoResolve_UnbornHEADWithoutBase(t *testing.T) {
	t.Parallel()
	remote := t.TempDir()
	initRepoWithFile(t, remote, "a.yaml", "base")
	dir := historylessCheckout(t, filepath.Join(remote, ".git"), "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, errors.Is(err, plumbing.ErrReferenceNotFound), true)
	if err == nil {
		t.Fatal("expected error for an unborn HEAD without --base")
	}
	for _, want := range []string{"no commits", "--base"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

// historylessCheckout builds the layout an agent runner produces: the
// files written straight to disk, `git init` over them, and an origin
// remote when url is non-empty. No commits, no objects.
func historylessCheckout(t *testing.T, url, path, content string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	testutil.GitInit(t, dir)
	if url != "" {
		addOrigin(t, dir, url)
	}
	return dir
}

func addOrigin(t *testing.T, dir, url string) {
	t.Helper()
	if _, err := openHelper(t, dir).CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}}); err != nil {
		t.Fatal(err)
	}
}

func TestBaseRemote_FetchPolicy(t *testing.T) {
	t.Parallel()
	pin := plumbing.NewHash(strings.Repeat("a", 40))
	refusal := &pktline.ErrorLine{Text: "partial fetch refused"}
	responseClose, sessionClose := errors.New("response close"), errors.New("session close")
	for _, tc := range []struct {
		name                                     string
		filter, shallow, reachable, full, bodies bool
		failures                                 []error
		wantErr                                  error
		wantFull                                 bool
		attempts                                 int
		cancel                                   bool
		smallSideband, preCanceled               bool
		closeErrors                              bool
	}{
		{name: "metadata small sideband", filter: true, shallow: true, attempts: 1, smallSideband: true},
		{name: "already canceled", filter: true, wantErr: context.Canceled, preCanceled: true},
		{name: "metadata filtered shallow", filter: true, shallow: true, attempts: 1},
		{name: "metadata filtered without shallow", filter: true, attempts: 1},
		{name: "metadata without filter", shallow: true, wantFull: true, attempts: 1},
		{name: "metadata without capabilities", wantFull: true, attempts: 1},
		{name: "metadata preserves full", filter: true, shallow: true, full: true, wantFull: true, attempts: 1},
		{name: "targeted bodies", filter: true, shallow: true, reachable: true, bodies: true, attempts: 1},
		{name: "targeted bodies preserve full", filter: true, shallow: true, reachable: true, bodies: true, full: true, wantFull: true, attempts: 1},
		{name: "missing reachable capability fetches full pin", filter: true, shallow: true, bodies: true, wantFull: true, attempts: 1},
		{name: "full missing objects", full: true, bodies: true, wantFull: true},
		{name: "refusal retries fresh full pin", filter: true, shallow: true, failures: []error{refusal}, wantFull: true, attempts: 2},
		{name: "body refusal retries fresh full pin", filter: true, shallow: true, reachable: true, bodies: true, failures: []error{refusal}, wantFull: true, attempts: 2},
		{name: "refusal retries only once", filter: true, shallow: true, failures: []error{refusal, refusal}, wantErr: refusal, wantFull: true, attempts: 2},
		{name: "authentication stops", filter: true, failures: []error{transport.ErrAuthenticationRequired}, wantErr: transport.ErrAuthenticationRequired, attempts: 1},
		{name: "authentication precedes refusal", filter: true, failures: []error{errors.Join(transport.ErrAuthenticationRequired, refusal)}, wantErr: transport.ErrAuthenticationRequired, attempts: 1},
		{name: "authorization stops", filter: true, failures: []error{transport.ErrAuthorizationFailed}, wantErr: transport.ErrAuthorizationFailed, attempts: 1},
		{name: "cancellation precedes refusal and auth", filter: true, failures: []error{errors.Join(refusal, transport.ErrAuthenticationRequired)}, wantErr: context.Canceled, attempts: 1, cancel: true},
		{name: "response and session close errors", filter: true, wantErr: responseClose, attempts: 1, closeErrors: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			caps := capability.NewList()
			sideband := capability.Sideband64k
			if tc.smallSideband {
				sideband = capability.Sideband
			}
			for _, c := range []capability.Capability{sideband, capability.MultiACKDetailed} {
				if err := caps.Set(c); err != nil {
					t.Fatal(err)
				}
			}
			for c, enabled := range map[capability.Capability]bool{capability.Filter: tc.filter, capability.Shallow: tc.shallow, capability.AllowReachableSHA1InWant: tc.reachable} {
				if enabled {
					if err := caps.Set(c); err != nil {
						t.Fatal(err)
					}
				}
			}
			originalCaps := caps.String()
			source := memory.NewStorage()
			body := fetchBlob(t, source, "requested body")
			var packed bytes.Buffer
			if _, err := packfile.NewEncoder(&packed, source, false).Encode([]plumbing.Hash{body}, 0); err != nil {
				t.Fatal(err)
			}
			fixture := &fetchPolicyTransport{pack: packed.Bytes(), failures: tc.failures}
			if tc.closeErrors {
				fixture.responseErr, fixture.sessionErr = responseClose, sessionClose
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				fixture.upload = cancel
			}
			remote := &baseRemote{client: fixture, capabilities: caps, pin: pin, full: tc.full}
			upper := memory.NewStorage()
			var wants []plumbing.Hash
			if tc.bodies {
				wants = []plumbing.Hash{body}
			}
			if tc.preCanceled {
				cancel()
			}
			err := remote.fetch(ctx, upper, wants)
			if tc.name == "full missing objects" {
				if err == nil || err.Error() != fmt.Sprintf("base %s is missing objects after full fetch", pin) {
					t.Fatalf("missing object diagnostic: %v", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("fetch error = %v, want %v", err, tc.wantErr)
			}
			if len(fixture.requests) != tc.attempts || fixture.closed != tc.attempts || remote.full != tc.wantFull || caps.String() != originalCaps {
				t.Fatalf("fetch lifecycle: requests=%d closed=%d full=%t capabilities=%q", len(fixture.requests), fixture.closed, remote.full, caps.String())
			}
			for i, request := range fixture.requests {
				bodyRequest := tc.bodies && tc.reachable && i == 0
				want := []plumbing.Hash{pin}
				depth := packp.DepthCommits(0)
				if bodyRequest {
					want = wants
				} else if tc.shallow {
					depth = packp.DepthCommits(1)
				}
				filtered := !tc.bodies && !tc.full && tc.filter && i == 0
				if !slices.Equal(request.Wants, want) || request.Depth != depth || request.Capabilities.Supports(capability.Shallow) != (depth != 0) || request.Capabilities.Supports(capability.Filter) != filtered || request.Capabilities.Supports(capability.Sideband) || request.Capabilities.Supports(capability.Sideband64k) {
					t.Fatalf("request %d: wants=%v depth=%v filter=%q caps=%q", i, request.Wants, request.Depth, request.Filter, request.Capabilities.String())
				}
				wantFilter := packp.Filter("")
				if filtered {
					wantFilter = packp.FilterBlobNone()
				}
				if request.Filter != wantFilter {
					t.Fatalf("request %d filter=%q, want %q", i, request.Filter, wantFilter)
				}
				if i > 0 && (request == fixture.requests[0] || request.Capabilities == fixture.requests[0].Capabilities) {
					t.Fatal("fallback reused a partial request")
				}
			}
			marker, markerErr := upper.Reference(fullBaseRef)
			if err == nil {
				if upper.HasEncodedObject(body) != nil || fixture.responsesClosed != 1 {
					t.Fatal("successful response was not consumed and closed")
				}
				if tc.wantFull {
					if markerErr != nil || marker.Hash() != pin {
						t.Fatalf("full marker: %v, %v", marker, markerErr)
					}
				} else if !errors.Is(markerErr, plumbing.ErrReferenceNotFound) {
					t.Fatalf("partial fetch wrote full marker: %v", marker)
				}
			} else if !errors.Is(markerErr, plumbing.ErrReferenceNotFound) {
				t.Fatalf("failed fetch wrote full marker: %v", marker)
			}
			if tc.closeErrors && (!errors.Is(err, sessionClose) || fixture.responsesClosed != 1) {
				t.Fatalf("joined close errors lost: %v", err)
			}
			if tc.cancel && (errors.Is(err, refusal) || errors.Is(err, transport.ErrAuthenticationRequired)) {
				t.Fatalf("cancellation did not take precedence: %v", err)
			}
		})
	}
}

type fetchPolicyTransport struct {
	transport.Transport
	pack                    []byte
	failures                []error
	upload                  func()
	responseErr, sessionErr error
	requests                []*packp.UploadPackRequest
	closed, responsesClosed int
}

func (f *fetchPolicyTransport) NewUploadPackSession(*transport.Endpoint, transport.AuthMethod) (transport.UploadPackSession, error) {
	return &fetchPolicySession{fixture: f}, nil
}

type fetchPolicySession struct {
	transport.UploadPackSession
	fixture *fetchPolicyTransport
}

func (s *fetchPolicySession) UploadPack(_ context.Context, request *packp.UploadPackRequest) (*packp.UploadPackResponse, error) {
	f := s.fixture
	attempt := len(f.requests)
	f.requests = append(f.requests, request)
	if f.upload != nil {
		f.upload()
	}
	if attempt < len(f.failures) && f.failures[attempt] != nil {
		return nil, f.failures[attempt]
	}
	return packp.NewUploadPackResponseWithPackfile(request, &fetchPolicyPack{Reader: bytes.NewReader(f.pack), fixture: f}), nil
}

func (s *fetchPolicySession) Close() error {
	s.fixture.closed++
	return s.fixture.sessionErr
}

type fetchPolicyPack struct {
	io.Reader
	fixture *fetchPolicyTransport
}

func (p *fetchPolicyPack) Close() error {
	p.fixture.responsesClosed++
	return p.fixture.responseErr
}

func TestAdopt_FetchedStorage(t *testing.T) {
	t.Parallel()
	pin := plumbing.NewHash(strings.Repeat("a", 40))
	for _, tc := range []struct {
		name                                               string
		layered, full, upperMarker, lowerMarker, readError bool
	}{
		{name: "standalone"},
		{name: "fetched full marker", upperMarker: true},
		{name: "absent marker retains full", full: true},
		{name: "local and fetched objects", layered: true},
		{name: "layered fetched marker", layered: true, upperMarker: true},
		{name: "local marker does not mark fetched full", layered: true, lowerMarker: true},
		{name: "layered absent marker retains full", layered: true, full: true},
		{name: "marker read error", readError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backing := osfs.New(t.TempDir())
			upper := filesystem.NewStorageWithOptions(backing, objectcache.NewObjectLRU(0), filesystem.Options{})
			upperHash := fetchBlob(t, upper, "fetched only")
			lower := memory.NewStorage()
			lowerHash := fetchBlob(t, lower, "local only")
			if tc.upperMarker {
				if err := upper.SetReference(plumbing.NewHashReference(fullBaseRef, pin)); err != nil {
					t.Fatal(err)
				}
			}
			if tc.lowerMarker {
				if err := lower.SetReference(plumbing.NewHashReference(fullBaseRef, pin)); err != nil {
					t.Fatal(err)
				}
			}
			readErr := &fs.PathError{Op: "open", Path: "packed-refs", Err: fs.ErrPermission}
			input := backing
			if tc.readError {
				input = &markerReadError{Filesystem: backing, err: readErr}
			}
			r := &resolution{Hash: pin, backing: input, remote: &baseRemote{pin: pin, full: tc.full}}
			var local storage.Storer
			if tc.layered {
				local = lower
			}
			err := r.adopt(local)
			repo := r.Repo
			if tc.readError {
				if !errors.Is(err, readErr) || repo != nil {
					t.Fatalf("marker error: repo=%v err=%v", repo, err)
				}
				return
			}
			if err != nil || repo == nil || r.remote.full != (tc.full || tc.upperMarker) || r.Hash != pin || r.remote.pin != pin {
				t.Fatalf("adopt: repo=%v err=%v full=%t", repo, err, r.remote.full)
			}
			for _, obj := range []struct {
				hash plumbing.Hash
				body string
			}{{upperHash, "fetched only"}, {lowerHash, "local only"}} {
				blob, err := repo.Storer.EncodedObject(plumbing.BlobObject, obj.hash)
				if !tc.layered && obj.hash == lowerHash {
					if !errors.Is(err, plumbing.ErrObjectNotFound) {
						t.Fatalf("standalone admitted local storage: %v", err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				reader, err := blob.Reader()
				if err != nil {
					t.Fatal(err)
				}
				data, readErr := io.ReadAll(reader)
				if err := errors.Join(readErr, reader.Close()); err != nil {
					t.Fatal(err)
				}
				if string(data) != obj.body {
					t.Fatalf("object body=%q, want %q", data, obj.body)
				}
			}
			written := fetchBlob(t, repo.Storer, "new fetched object")
			if upper.HasEncodedObject(written) != nil || !errors.Is(lower.HasEncodedObject(written), plumbing.ErrObjectNotFound) {
				t.Fatal("adopted writes did not retain fetched-upper ownership")
			}
		})
	}
}

func fetchBlob(t *testing.T, st storage.Storer, body string) plumbing.Hash {
	t.Helper()
	obj := &plumbing.MemoryObject{}
	obj.SetType(plumbing.BlobObject)
	if _, err := obj.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	hash, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

type markerReadError struct {
	billy.Filesystem
	err error
}

func (f *markerReadError) Open(name string) (billy.File, error) {
	if name == "packed-refs" {
		return nil, f.err
	}
	return f.Filesystem.Open(name)
}

func TestResolveExplicit_FetchCancellation(t *testing.T) {
	const scheme = "baseline-failure"
	original := client.Protocols[scheme]
	t.Cleanup(func() { client.InstallProtocol(scheme, original) })
	for _, tc := range []struct {
		name  string
		cause error
		input bool
	}{
		{name: "canceled", cause: context.Canceled},
		{name: "deadline", cause: context.DeadlineExceeded},
		{name: "normal fetch failure", cause: transport.ErrAuthenticationRequired, input: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("local transport: %w", tc.cause)
			client.InstallProtocol(scheme, &failureTransport{err: wrapped})
			dir := historylessCheckout(t, scheme+"://origin/repo", "a.yaml", "current")
			location, err := gitrepo.Locate(t.Context(), dir, "")
			assert.NoError(t, err)
			_, err = resolveExplicit(t.Context(), location, "main", cachetest.New(t, "", sourcecache.New))
			assert.Equal(t, errors.Is(err, wrapped), true)
			assert.Equal(t, errors.Is(err, tc.cause), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), tc.input)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), tc.input)
		})
	}
}

type failureTransport struct {
	transport.Transport
	err error
}

func (f *failureTransport) NewUploadPackSession(*transport.Endpoint, transport.AuthMethod) (transport.UploadPackSession, error) {
	return nil, f.err
}
