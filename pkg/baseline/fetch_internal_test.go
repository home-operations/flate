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

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	objectcache "github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/testutil/networktest"
	sourcegit "github.com/home-operations/flate/pkg/source/git"

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
			res, err := AutoResolve(
				t.Context(),
				Config{
					Network: networktest.New(t),
					Path:    dir,
					Base:    tc.base,
					Cache:   cachetest.New(t, "", sourcecache.New),
				},
			)
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

	res, err := AutoResolve(
		t.Context(),
		Config{Network: networktest.New(t), Path: dir, Base: "main", Cache: cachetest.New(t, "", sourcecache.New)},
	)
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

	_, err := AutoResolve(
		t.Context(),
		Config{Network: networktest.New(t), Path: dir, Base: "nope", Cache: cachetest.New(t, "", sourcecache.New)},
	)
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

	_, err := AutoResolve(
		t.Context(),
		Config{Network: networktest.New(t), Path: dir, Base: "main", Cache: cachetest.New(t, "", sourcecache.New)},
	)
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

	_, err := AutoResolve(
		t.Context(),
		Config{Network: networktest.New(t), Path: dir, Base: "", Cache: cachetest.New(t, "", sourcecache.New)},
	)
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
	origin := t.TempDir()
	pin := initRepoWithFile(t, origin, "a.yaml", "requested body")
	refusal := &pktline.ErrorLine{Text: "partial fetch refused"}
	responseClose, sessionClose := errors.New("response close"), errors.New("session close")
	storageError := errors.New("storage lookup")
	for _, tc := range []struct {
		name                                              string
		filter, shallow, reachable, full, bodies, noExact bool
		failures                                          []error
		wantErr                                           error
		wantFull                                          bool
		attempts                                          int
		cancel                                            bool
		smallSideband, preCanceled                        bool
		closeErrors                                       bool
		responsePins                                      []bool
		preload, wildcard                                 bool
		storageError                                      error
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
		{name: "named metadata later needs bodies", filter: true, shallow: true, bodies: true, noExact: true, wantFull: true, attempts: 1},
		{name: "full missing objects", full: true, bodies: true, wantFull: true},
		{name: "refusal retries fresh full pin", filter: true, shallow: true, failures: []error{refusal}, wantFull: true, attempts: 2},
		{name: "body refusal retries fresh full pin", filter: true, shallow: true, reachable: true, bodies: true, failures: []error{refusal}, wantFull: true, attempts: 2},
		{name: "refusal retries only once", filter: true, shallow: true, failures: []error{refusal, refusal}, wantErr: refusal, wantFull: true, attempts: 2},
		{name: "authentication stops", filter: true, failures: []error{transport.ErrAuthenticationRequired}, wantErr: transport.ErrAuthenticationRequired, attempts: 1},
		{name: "authentication precedes refusal", filter: true, failures: []error{errors.Join(transport.ErrAuthenticationRequired, refusal)}, wantErr: transport.ErrAuthenticationRequired, attempts: 1},
		{name: "authorization stops", filter: true, failures: []error{transport.ErrAuthorizationFailed}, wantErr: transport.ErrAuthorizationFailed, attempts: 1},
		{name: "cancellation precedes refusal and auth", filter: true, failures: []error{errors.Join(refusal, transport.ErrAuthenticationRequired)}, wantErr: context.Canceled, attempts: 1, cancel: true},
		{name: "response and session close errors", filter: true, wantErr: responseClose, attempts: 1, closeErrors: true},
		{name: "missing saved pin recovers history", filter: true, shallow: true, responsePins: []bool{false, true}, attempts: 2},
		{name: "history recovery preserves depth after refusal", filter: true, shallow: true, responsePins: []bool{false, false, true}, failures: []error{nil, refusal}, wantFull: true, attempts: 3},
		{name: "history recovery preserves depth after filter refusal", filter: true, shallow: true, responsePins: []bool{false, false, true}, failures: []error{nil, transport.ErrFilterNotSupported}, wantFull: true, attempts: 3},
		{name: "history recovery budget survives refusal", filter: true, shallow: true, responsePins: []bool{false, false, false}, failures: []error{nil, refusal}, wantFull: true, wantErr: plumbing.ErrObjectNotFound, attempts: 3},
		{name: "no-change response requires saved pin", filter: true, shallow: true, failures: []error{git.NoErrAlreadyUpToDate, git.NoErrAlreadyUpToDate}, wantErr: plumbing.ErrObjectNotFound, attempts: 2},
		{name: "preloaded metadata cannot certify fresh full response", full: true, noExact: true, shallow: true, preload: true, responsePins: []bool{false, false}, wantFull: true, wantErr: plumbing.ErrObjectNotFound, attempts: 2},
		{name: "unrestricted wildcard requires saved pin", filter: true, noExact: true, wildcard: true, responsePins: []bool{false}, wantErr: plumbing.ErrObjectNotFound, attempts: 1},
		{name: "storage error cannot trigger recovery", filter: true, shallow: true, noExact: true, storageError: storageError, wantErr: storageError, attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testBaseRemoteFetchPolicyCase(
				t,
				testBaseRemoteFetchPolicyCaseInputs{
					origin:        origin,
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
	capabilities                    *capability.List
	pack                            []byte
	failures                        []error
	upload                          func()
	responseErr, sessionErr         error
	requests                        []*transport.FetchRequest
	targets                         []storage.Storer
	missingPack                     []byte
	responsePins                    []bool
	opened, closed, responsesClosed int
	pin                             plumbing.Hash
}

func (f *fetchPolicyTransport) Fetch(
	_ context.Context,
	upper storage.Storer,
	_ string,
	opts *git.FetchOptions,
) error {
	f.opened++
	defer func() { f.closed++ }()
	if opts.Filter != "" && !f.capabilities.Supports(capability.Filter) {
		return transport.ErrFilterNotSupported
	}
	if opts.Depth > 0 && !f.capabilities.Supports(capability.Shallow) {
		return transport.ErrShallowNotSupported
	}
	req := &transport.FetchRequest{Depth: opts.Depth, Filter: opts.Filter, Progress: opts.Progress}
	for _, spec := range opts.RefSpecs {
		if spec.IsExactSHA1() {
			if !f.capabilities.Supports(capability.AllowReachableSHA1InWant) {
				return git.ErrExactSHA1NotSupported
			}
			req.Wants = append(req.Wants, plumbing.NewHash(spec.Src()))
		} else {
			req.Wants = append(req.Wants, f.pin)
		}
	}
	attempt := len(f.requests)
	f.requests = append(f.requests, req)
	f.targets = append(f.targets, upper)
	if f.upload != nil {
		f.upload()
	}
	if attempt < len(f.failures) && f.failures[attempt] != nil {
		return f.failures[attempt]
	}
	packed := f.pack
	if attempt < len(f.responsePins) && !f.responsePins[attempt] {
		packed = f.missingPack
	}
	err := packfile.UpdateObjectStorage(upper, bytes.NewReader(packed))
	f.responsesClosed++
	err = errors.Join(err, f.responseErr)
	if err != nil {
		return err
	}
	return f.sessionErr
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

type pinLookupError struct {
	storage.Storer

	err error
}

func (s *pinLookupError) HasEncodedObject(plumbing.Hash) error { return s.err }

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
			routes := sourcegit.Transports{
				scheme: func(context.Context, sourcegit.TransportRequest) (sourcegit.Connection, error) { return nil, wrapped },
			}
			dir := historylessCheckout(t, scheme+"://origin/repo", "a.yaml", "current")
			location, err := gitrepo.Locate(t.Context(), dir, "")
			assert.NoError(t, err)
			_, err = (&selection{repo: location.Repository, owner: cachetest.New(t, "", sourcecache.New), network: networktest.New(t), transports: routes}).resolveExplicit(
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

type testBaseRemoteFetchPolicyCaseInputs struct {
	origin        string
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
		noExact   bool
		failures  []error
		wantErr   error

		wantFull          bool
		attempts          int
		cancel            bool
		smallSideband     bool
		preCanceled       bool
		closeErrors       bool
		responsePins      []bool
		preload, wildcard bool
		storageError      error
	}
}

func testBaseRemoteFetchPolicyCase(t *testing.T, testCaseInput testBaseRemoteFetchPolicyCaseInputs) {
	t.Helper()

	caps := new(capability.List)
	sideband := capability.Sideband64k
	if testCaseInput.tc.smallSideband {
		sideband = capability.Sideband
	}
	for _, c := range []string{sideband, capability.MultiACKDetailed} {
		caps.Set(c)
	}
	toggles := make(map[string]bool)
	toggles[capability.Filter] = testCaseInput.tc.filter
	toggles[capability.Shallow] = testCaseInput.tc.shallow
	toggles[capability.AllowReachableSHA1InWant] = testCaseInput.tc.reachable
	for c, enabled := range toggles {
		if enabled {
			caps.Set(c)
		}
	}
	originalCaps := caps.String()
	source := openHelper(t, testCaseInput.origin).Storer
	commit, err := object.GetCommit(source, *testCaseInput.pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	body := tree.Entries[0].Hash
	var packed bytes.Buffer
	{
		_, encodeErr := packfile.NewEncoder(&packed, source, false).
			Encode([]plumbing.Hash{*testCaseInput.pin, tree.Hash, body}, 0)
		assert.FatalIf(t, encodeErr != nil, "%v", encodeErr)
	}
	fixture := &fetchPolicyTransport{
		pack:         packed.Bytes(),
		failures:     testCaseInput.tc.failures,
		capabilities: caps,
		pin:          *testCaseInput.pin,
	}
	var missing bytes.Buffer
	_, packErr := packfile.NewEncoder(&missing, source, false).Encode([]plumbing.Hash{body}, 0)
	assert.NoError(t, packErr)
	fixture.missingPack = missing.Bytes()
	fixture.responsePins = testCaseInput.tc.responsePins
	if testCaseInput.tc.closeErrors {
		fixture.responseErr, fixture.sessionErr = (*testCaseInput.responseClose), (*testCaseInput.sessionClose)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if testCaseInput.tc.cancel {
		fixture.upload = cancel
	}
	remote := &baseRemote{
		client:  fixture,
		pin:     (*testCaseInput.pin),
		ref:     "refs/heads/baseline",
		full:    testCaseInput.tc.full,
		noExact: testCaseInput.tc.noExact,
	}
	upper := memory.NewStorage()
	if testCaseInput.tc.preload {
		encoded, lookupErr := source.EncodedObject(plumbing.AnyObject, *testCaseInput.pin)
		assert.NoError(t, lookupErr)
		_, storeErr := upper.SetEncodedObject(encoded)
		assert.NoError(t, storeErr)
	}
	if testCaseInput.tc.wildcard {
		remote.ref = ""
	}
	var target storage.Storer = upper
	if testCaseInput.tc.storageError != nil {
		target = &pinLookupError{Storer: upper, err: testCaseInput.tc.storageError}
	}
	var wants []plumbing.Hash
	if testCaseInput.tc.bodies {
		wants = []plumbing.Hash{body}
	}
	if testCaseInput.tc.preCanceled {
		cancel()
	}
	err = remote.fetch(ctx, target, wants)
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
	if len(fixture.requests) != testCaseInput.tc.attempts || fixture.closed != fixture.opened ||
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
	if errors.Is(err, plumbing.ErrObjectNotFound) {
		assert.Equal(t, strings.Contains(err.Error(), testCaseInput.pin.String()), true)
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

//nolint:gocognit // History recovery MUST retain the existing body, filter, and request assertions in the shared policy table.
func testBaseRemoteFetchPolicyCaseSegment(
	t *testing.T,
	testCaseInput *testBaseRemoteFetchPolicyCaseInputs,
	fixture **fetchPolicyTransport,
	wants *[]plumbing.Hash,
) {
	t.Helper()
	history := len(testCaseInput.tc.responsePins) > 1 ||
		len(testCaseInput.tc.failures) > 0 && errors.Is(testCaseInput.tc.failures[0], git.NoErrAlreadyUpToDate)
	for i, request := range (*fixture).requests {
		bodyRequest := testCaseInput.tc.bodies && testCaseInput.tc.reachable && i == 0
		want := []plumbing.Hash{*testCaseInput.pin}
		if bodyRequest {
			want = *wants
		}
		if testCaseInput.tc.wildcard {
			want = []plumbing.Hash{*testCaseInput.pin, *testCaseInput.pin}
		}
		recovering := history && i > 0
		depth := 0
		if testCaseInput.tc.shallow && !bodyRequest && !testCaseInput.tc.wildcard && !recovering {
			depth = 1
		}
		filtered := !testCaseInput.tc.bodies && !testCaseInput.tc.full && testCaseInput.tc.filter
		if i > 0 {
			filtered = filtered && history && i == 1
		}
		if recovering {
			assert.Equal(t, (*fixture).targets[i] == (*fixture).targets[0], false)
			assert.Equal(t, (*fixture).targets[i] == (*fixture).targets[i-1], false)
		}
		if !slices.Equal(request.Wants, want) || request.Depth != depth ||
			(request.Depth > 0 && (*fixture).capabilities.Supports(capability.Shallow)) != (depth != 0) ||
			(request.Filter != "") != filtered ||
			request.Progress != nil {
			t.Fatalf(
				"request %d: wants=%v depth=%v filter=%q caps=%q",
				i,
				request.Wants,
				request.Depth,
				request.Filter,
				fmt.Sprintf("progress=%t", request.Progress != nil),
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
			request == (*fixture).requests[0], "%v", "fallback reused a partial request")
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
		responses := statementInput.testCaseInput.tc.attempts
		for _, failure := range statementInput.testCaseInput.tc.failures {
			if failure != nil {
				responses--
			}
		}
		assert.FatalIf(
			t,
			(*statementInput.upper).HasEncodedObject((*statementInput.body)) != nil ||
				(*statementInput.fixture).responsesClosed != responses,
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
			(errors.Is((*statementInput.err), (*statementInput.testCaseInput.sessionClose)) || (*statementInput.fixture).responsesClosed != 1),
		"unexpected close-error result: %v",
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
