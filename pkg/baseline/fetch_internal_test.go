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
	{
		_, err := remoteRepo.CreateTag("v1", mainCommit, &git.CreateTagOptions{
			Tagger:  &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			Message: "v1",
		})
		assert.FatalIf(t, err != nil, "%v", err)
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
			assert.FatalIf(t, err != nil, "AutoResolve(--base=%s): %v", tc.base, err)

			got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
			assert.FatalIf(t, err != nil, "read materialized: %v", err)
			assert.ErrorIf(
				t,
				string(got) != "base",
				"materialized a.yaml = %q, want %q (origin's main, not the checkout)",
				got,
				"base",
			)
			if res.BaseInput.Revision != mainCommit.String() {
				t.Errorf("Revision = %q, want %q", res.BaseInput.Revision, mainCommit.String())
			}
			if !strings.Contains(res.Source.Metadata["baseline-selection"], "fetched from origin") {
				t.Errorf(
					"Source = %q, want it to say the rev was fetched from origin",
					res.Source.Metadata["baseline-selection"],
				)
			}
			// The checkout is read-only: the fetch lands nowhere near it.
			{
				_, err := openHelper(t, dir).CommitObject(mainCommit)
				assert.ErrorIf(
					t,
					err == nil,
					"%v",
					"baseline commit landed in the checkout; the fetch must not write into it",
				)
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
	assert.FatalIf(t, err != nil, "AutoResolve(--base=main): %v", err)

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
	assert.FatalIf(t, err == nil, "%v", "expected error for a rev origin does not have")
	for _, want := range []string{"nope", "origin"} {
		assert.ErrorIf(t, !strings.Contains(err.Error(), want), "error %q should mention %q", err, want)
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
	assert.FatalIf(t, err == nil, "%v", "expected error without an origin remote")
	assert.ErrorIf(
		t,
		!strings.Contains(err.Error(), "origin remote"),
		"error %q should say there is no origin remote to fetch from",
		err,
	)
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
	assert.FatalIf(t, err == nil, "%v", "expected error for an unborn HEAD without --base")
	for _, want := range []string{"no commits", "--base"} {
		assert.ErrorIf(t, !strings.Contains(err.Error(), want), "error %q should mention %q", err, want)
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
	{
		_, err := openHelper(
			t,
			dir,
		).CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}})
		assert.FatalIf(t, err != nil, "%v", err)
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
			testBaseRemoteFetchPolicyCase(
				t,
				testBaseRemoteFetchPolicyCaseInputs{
					pin:           &pin,
					refusal:       &refusal,
					responseClose: &responseClose,
					sessionClose:  &sessionClose,
					tc:            &tc,
				},
			)
		},
		)
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

//nolint:ireturn // The upstream interface requires this return type.
func (f *fetchPolicyTransport) NewUploadPackSession(
	*transport.Endpoint,
	transport.AuthMethod,
) (transport.UploadPackSession, error) {
	return &fetchPolicySession{fixture: f}, nil
}

type fetchPolicySession struct {
	transport.UploadPackSession

	fixture *fetchPolicyTransport
}

func (s *fetchPolicySession) UploadPack(
	_ context.Context,
	request *packp.UploadPackRequest,
) (*packp.UploadPackResponse, error) {
	f := s.fixture
	attempt := len(f.requests)
	f.requests = append(f.requests, request)
	if f.upload != nil {
		f.upload()
	}
	if attempt < len(f.failures) && f.failures[attempt] != nil {
		return nil, f.failures[attempt]
	}
	return packp.NewUploadPackResponseWithPackfile(
		request,
		&fetchPolicyPack{Reader: bytes.NewReader(f.pack), fixture: f},
	), nil
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
			testAdoptFetchedStorageCase(t, testAdoptFetchedStorageCaseInputs{pin: &pin, tc: &tc})
		},
		)
	}
}

func fetchBlob(t *testing.T, st storage.Storer, body string) plumbing.Hash {
	t.Helper()
	obj := &plumbing.MemoryObject{}
	obj.SetType(plumbing.BlobObject)
	{
		_, err := obj.Write([]byte(body))
		assert.FatalIf(t, err != nil, "%v", err)
	}
	hash, err := st.SetEncodedObject(obj)
	assert.FatalIf(t, err != nil, "%v", err)
	return hash
}

type markerReadError struct {
	billy.Filesystem

	err error
}

//nolint:ireturn // The upstream interface requires this return type.
func (f *markerReadError) Open(name string) (billy.File, error) {
	if name == "packed-refs" {
		return nil, f.err
	}
	return f.Filesystem.Open(name)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
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
			_, err = (selection{repo: location.Repository, owner: cachetest.New(t, "", sourcecache.New)}).resolveExplicit(
				t.Context(),
				location,
				"main",
			)
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

//nolint:ireturn // The upstream interface requires this return type.
func (f *failureTransport) NewUploadPackSession(
	*transport.Endpoint,
	transport.AuthMethod,
) (transport.UploadPackSession, error) {
	return nil, f.err
}

type testBaseRemoteFetchPolicyCaseInputs struct {
	pin           *plumbing.Hash
	refusal       **pktline.ErrorLine
	responseClose *error
	sessionClose  *error
	tc            *struct {
		name string

		filter    bool
		shallow   bool
		reachable bool
		full      bool
		bodies    bool
		failures  []error
		wantErr   error

		wantFull      bool
		attempts      int
		cancel        bool
		smallSideband bool
		preCanceled   bool
		closeErrors   bool
	}
}

func testBaseRemoteFetchPolicyCase(t *testing.T, testCaseInput testBaseRemoteFetchPolicyCaseInputs) {
	t.Helper()

	caps := capability.NewList()
	sideband := capability.Sideband64k
	if testCaseInput.tc.smallSideband {
		sideband = capability.Sideband
	}
	for _, c := range []capability.Capability{sideband, capability.MultiACKDetailed} {
		{
			err := caps.Set(c)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	toggles := make(map[capability.Capability]bool)
	toggles[capability.Filter] = testCaseInput.tc.filter
	toggles[capability.Shallow] = testCaseInput.tc.shallow
	toggles[capability.AllowReachableSHA1InWant] = testCaseInput.tc.reachable
	for c, enabled := range toggles {
		if enabled {
			{
				err := caps.Set(c)
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
	}
	originalCaps := caps.String()
	source := memory.NewStorage()
	body := fetchBlob(t, source, "requested body")
	var packed bytes.Buffer
	{
		_, err := packfile.NewEncoder(&packed, source, false).Encode([]plumbing.Hash{body}, 0)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	fixture := &fetchPolicyTransport{pack: packed.Bytes(), failures: testCaseInput.tc.failures}
	if testCaseInput.tc.closeErrors {
		fixture.responseErr, fixture.sessionErr = (*testCaseInput.responseClose), (*testCaseInput.sessionClose)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if testCaseInput.tc.cancel {
		fixture.upload = cancel
	}
	remote := &baseRemote{
		client:       fixture,
		capabilities: caps,
		pin:          (*testCaseInput.pin),
		full:         testCaseInput.tc.full,
	}
	upper := memory.NewStorage()
	var wants []plumbing.Hash
	if testCaseInput.tc.bodies {
		wants = []plumbing.Hash{body}
	}
	if testCaseInput.tc.preCanceled {
		cancel()
	}
	err := remote.fetch(ctx, upper, wants)
	if testCaseInput.tc.name == "full missing objects" {
		assert.FatalIf(
			t,
			err == nil ||
				err.Error() != fmt.Sprintf("base %s is missing objects after full fetch", (*testCaseInput.pin)),
			"missing object diagnostic: %v",
			err,
		)
	} else {
		assert.FatalIf(
			t,
			!errors.Is(err, testCaseInput.tc.wantErr),
			"fetch error = %v, want %v",
			err,
			testCaseInput.tc.wantErr,
		)
	}
	if len(fixture.requests) != testCaseInput.tc.attempts || fixture.closed != testCaseInput.tc.attempts ||
		remote.full != testCaseInput.tc.wantFull ||
		caps.String() != originalCaps {
		t.Fatalf(
			"fetch lifecycle: requests=%d closed=%d full=%t capabilities=%q",
			len(fixture.requests),
			fixture.closed,
			remote.full,
			caps.String(),
		)
	}
	testBaseRemoteFetchPolicyCaseSegment(t, &testCaseInput, &fixture, &wants)
	marker, markerErr := upper.Reference(fullBaseRef)
	testFetchPolicyCompletion(
		t,
		&testFetchPolicyCompletionInputs{
			testCaseInput: &testCaseInput,
			body:          &body,
			fixture:       &fixture,
			upper:         &upper,
			err:           &err,
			marker:        &marker,
			markerErr:     &markerErr,
		},
	)
}

type testAdoptFetchedStorageCaseInputs struct {
	pin *plumbing.Hash
	tc  *struct {
		name string

		layered     bool
		full        bool
		upperMarker bool
		lowerMarker bool
		readError   bool
	}
}

func testAdoptFetchedStorageCase(t *testing.T, testCaseInput testAdoptFetchedStorageCaseInputs) {
	t.Helper()

	backing := osfs.New(t.TempDir())
	upper := filesystem.NewStorageWithOptions(backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	upperHash := fetchBlob(t, upper, "fetched only")
	lower := memory.NewStorage()
	lowerHash := fetchBlob(t, lower, "local only")
	if testCaseInput.tc.upperMarker {
		{
			err := upper.SetReference(plumbing.NewHashReference(fullBaseRef, (*testCaseInput.pin)))
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	if testCaseInput.tc.lowerMarker {
		{
			err := lower.SetReference(plumbing.NewHashReference(fullBaseRef, (*testCaseInput.pin)))
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	readErr := &fs.PathError{Op: "open", Path: "packed-refs", Err: fs.ErrPermission}
	input := backing
	if testCaseInput.tc.readError {
		input = &markerReadError{Filesystem: backing, err: readErr}
	}
	r := &resolution{
		Hash:    (*testCaseInput.pin),
		backing: input,
		remote:  &baseRemote{pin: (*testCaseInput.pin), full: testCaseInput.tc.full},
	}
	var local storage.Storer
	if testCaseInput.tc.layered {
		local = lower
	}
	err := r.adopt(local)
	repo := r.Repo
	if testCaseInput.tc.readError {
		assert.FatalIf(
			t,
			!errors.Is(err, readErr) || repo != nil,
			"marker error: repo=%v err=%v",
			repo,
			err,
		)
		return
	}
	if err != nil || repo == nil || r.remote.full != (testCaseInput.tc.full || testCaseInput.tc.upperMarker) ||
		r.Hash != (*testCaseInput.pin) ||
		r.remote.pin != (*testCaseInput.pin) {
		t.Fatalf("adopt: repo=%v err=%v full=%t", repo, err, r.remote.full)
	}
	for _, obj := range []struct {
		hash plumbing.Hash
		body string
	}{{upperHash, "fetched only"}, {lowerHash, "local only"}} {
		blob, err := repo.Storer.EncodedObject(plumbing.BlobObject, obj.hash)
		if !testCaseInput.tc.layered && obj.hash == lowerHash {
			assert.FatalIf(
				t,
				!errors.Is(err, plumbing.ErrObjectNotFound),
				"standalone admitted local storage: %v",
				err,
			)
			continue
		}
		assert.FatalIf(t, err != nil, "%v", err)
		reader, err := blob.Reader()
		assert.FatalIf(t, err != nil, "%v", err)
		data, readErr := io.ReadAll(reader)
		{
			err := errors.Join(readErr, reader.Close())
			assert.FatalIf(t, err != nil, "%v", err)
		}
		assert.FatalIf(t, string(data) != obj.body, "object body=%q, want %q", data, obj.body)
	}
	written := fetchBlob(t, repo.Storer, "new fetched object")
	assert.FatalIf(t, upper.HasEncodedObject(written) != nil ||
		!errors.Is(
			lower.HasEncodedObject(written),
			plumbing.ErrObjectNotFound,
		), "%v", "adopted writes did not retain fetched-upper ownership")
}

func testBaseRemoteFetchPolicyCaseSegment(
	t *testing.T,
	testCaseInput *testBaseRemoteFetchPolicyCaseInputs,
	fixture **fetchPolicyTransport,
	wants *[]plumbing.Hash,
) {
	t.Helper()
	for i, request := range (*fixture).requests {
		bodyRequest := testCaseInput.tc.bodies && testCaseInput.tc.reachable && i == 0
		want := []plumbing.Hash{(*testCaseInput.pin)}
		depth := packp.DepthCommits(0)
		if bodyRequest {
			want = (*wants)
		} else if testCaseInput.tc.shallow {
			depth = packp.DepthCommits(1)
		}
		filtered := !testCaseInput.tc.bodies && !testCaseInput.tc.full && testCaseInput.tc.filter && i == 0
		if !slices.Equal(request.Wants, want) || request.Depth != depth ||
			request.Capabilities.Supports(capability.Shallow) != (depth != 0) ||
			request.Capabilities.Supports(capability.Filter) != filtered ||
			request.Capabilities.Supports(capability.Sideband) ||
			request.Capabilities.Supports(capability.Sideband64k) {
			t.Fatalf(
				"request %d: wants=%v depth=%v filter=%q caps=%q",
				i,
				request.Wants,
				request.Depth,
				request.Filter,
				request.Capabilities.String(),
			)
		}
		wantFilter := packp.Filter("")
		if filtered {
			wantFilter = packp.FilterBlobNone()
		}
		if request.Filter != wantFilter {
			t.Fatalf("request %d filter=%q, want %q", i, request.Filter, wantFilter)
		}
		assert.FatalIf(t, i > 0 &&
			(request == (*fixture).requests[0] || request.Capabilities == (*fixture).requests[0].Capabilities), "%v", "fallback reused a partial request")
	}
}

type testFetchPolicyCompletionInputs struct {
	testCaseInput *testBaseRemoteFetchPolicyCaseInputs
	body          *plumbing.Hash
	fixture       **fetchPolicyTransport
	upper         **memory.Storage
	err           *error
	marker        **plumbing.Reference
	markerErr     *error
}

func testFetchPolicyCompletion(t *testing.T, statementInput *testFetchPolicyCompletionInputs) {
	t.Helper()
	if (*statementInput.err) == nil {
		assert.FatalIf(
			t,
			(*statementInput.upper).HasEncodedObject((*statementInput.body)) != nil ||
				(*statementInput.fixture).responsesClosed != 1,
			"%v",
			"successful response was not consumed and closed",
		)
		if statementInput.testCaseInput.tc.wantFull {
			assert.FatalIf(
				t,
				(*statementInput.markerErr) != nil ||
					(*statementInput.marker).Hash() != (*statementInput.testCaseInput.pin),
				"full marker: %v, %v",
				(*statementInput.marker),
				(*statementInput.markerErr),
			)
		} else {
			assert.FatalIf(
				t,
				!errors.Is((*statementInput.markerErr), plumbing.ErrReferenceNotFound),
				"partial fetch wrote full marker: %v",
				(*statementInput.marker),
			)
		}
	} else {
		assert.FatalIf(
			t,
			!errors.Is((*statementInput.markerErr), plumbing.ErrReferenceNotFound),
			"failed fetch wrote full marker: %v",
			(*statementInput.marker),
		)
	}
	assert.FatalIf(
		t,
		statementInput.testCaseInput.tc.closeErrors &&
			(!errors.Is((*statementInput.err), (*statementInput.testCaseInput.sessionClose)) || (*statementInput.fixture).responsesClosed != 1),
		"joined close errors lost: %v",
		(*statementInput.err),
	)
	assert.FatalIf(
		t,
		statementInput.testCaseInput.tc.cancel &&
			(errors.Is((*statementInput.err), (*statementInput.testCaseInput.refusal)) || errors.Is((*statementInput.err), transport.ErrAuthenticationRequired)),
		"cancellation did not take precedence: %v",
		(*statementInput.err),
	)
}
