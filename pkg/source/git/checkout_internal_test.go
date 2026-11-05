package git

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test replaces go-git protocol registration and restores it afterward.
//nolint:paralleltest // The test replaces process-wide go-git protocol registration.
func TestFetcher_SubmoduleSchemes(t *testing.T) {
	identity, _ := testSSHIdentity(t)
	previous := client.Protocols["ssh"]
	client.InstallProtocol(
		"ssh",
		commandTransport{Transport: server.DefaultServer, before: func(context.Context) error { return nil }},
	)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	file := &pinTransport{Transport: client.Protocols["file"]}
	client.InstallProtocol("file", file)
	t.Cleanup(func() { client.InstallProtocol("file", file.Transport) })
	for _, shape := range []string{"direct file", "nested file", "relative"} {
		t.Run(shape, func(t *testing.T) {
			testFetcherSubmoduleSchemesCase(
				t,
				&identity, &file, &shape,
			)
		},
		)
	}
}

type pinTransport struct {
	transport.Transport

	requests []*packp.UploadPackRequest
	refusal  error
}

type pinSession struct {
	transport.UploadPackSession

	owner *pinTransport
}

//nolint:ireturn // The upstream go-git protocol interface fixes this return type.
func (p *pinTransport) NewUploadPackSession(
	endpoint *transport.Endpoint,
	auth transport.AuthMethod,
) (transport.UploadPackSession, error) {
	session, err := p.Transport.NewUploadPackSession(endpoint, auth)
	return &pinSession{session, p}, err
}

func (p *pinSession) UploadPack(
	ctx context.Context,
	request *packp.UploadPackRequest,
) (*packp.UploadPackResponse, error) {
	p.owner.requests = append(p.owner.requests, request)
	if len(p.owner.requests) == 1 && p.owner.refusal != nil {
		return nil, p.owner.refusal
	}
	return p.UploadPackSession.UploadPack(ctx, request)
}

//nolint:paralleltest // The test replaces go-git protocol registration and restores it afterward.
//nolint:paralleltest // The test replaces process-wide go-git protocol registration.
func TestFetcher_ExactPinBeforeRecursion(t *testing.T) {
	module := t.TempDir()
	mustInitRepo(t, module)
	oldModule := mustSetRefToHEAD(t, module, "refs/tags/old")
	newModule := mustCommitFile(t, module, "hello.txt", "new module")
	parent := t.TempDir()
	mustInitRepoWithFiles(t, parent, map[string]string{
		"hello.txt": "parent", ".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = file://" + module + "\n",
	})
	repo, err := git.PlainOpen(parent)
	assert.NoError(t, err)

	cfg, err := repo.Storer.Config()
	assert.NoError(t, err)

	cfg.Raw.SetOption("uploadpack", "", "allowReachableSHA1InWant", "true")
	{
		setConfigErr := repo.Storer.SetConfig(cfg)
		assert.NoError(t, setConfigErr)
	}

	head, err := repo.Head()
	assert.NoError(t, err)

	commit, err := repo.CommitObject(head.Hash())
	assert.NoError(t, err)

	tree, err := commit.Tree()
	assert.NoError(t, err)

	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "lib", Mode: filemode.Submodule})
	pin := plumbing.ZeroHash
	for _, moduleHash := range []string{oldModule, newModule} {
		for i := range tree.Entries {
			if tree.Entries[i].Name == "lib" {
				tree.Entries[i].Hash = plumbing.NewHash(moduleHash)
			}
		}
		encoded := repo.Storer.NewEncodedObject()
		{
			encodeErr := tree.Encode(encoded)
			assert.NoError(t, encodeErr)
		}

		commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
		assert.NoError(t, err)

		commit.ParentHashes, commit.Message = []plumbing.Hash{commit.Hash}, moduleHash
		encoded = repo.Storer.NewEncodedObject()
		{
			encodeErr2 := commit.Encode(encoded)
			assert.NoError(t, encodeErr2)
		}

		commit.Hash, err = repo.Storer.SetEncodedObject(encoded)
		assert.NoError(t, err)

		if pin.IsZero() {
			pin = commit.Hash
		}
	}
	{
		err := repo.Storer.SetReference(plumbing.NewHashReference(head.Name(), commit.Hash))
		assert.NoError(t, err)
	}

	for _, tc := range []struct {
		name    string
		refusal error
		calls   int
	}{
		{"exact", nil, 1},
		{"gitlink", nil, 1},
		{"fallback", errors.New("exact request refused"), 2},
		{"auth", transport.ErrAuthorizationFailed, 1},
		{"auth-required", transport.ErrAuthenticationRequired, 1},
		{"canceled", context.Canceled, 1},
		{"deadline", context.DeadlineExceeded, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testFetcherExactPinBeforeRecursionCase(

				t, &parent, &pin, &tc)
		},
		)
	}
}

func testFetcherSubmoduleSchemesCase(t *testing.T, identity *string,
	file **pinTransport,
	shape *string,
) {
	t.Helper()
	root := t.TempDir()
	leaf := filepath.Join(root, "leaf")
	mustInitRepo(t, leaf)
	hash := plumbing.NewHash(mustHead(t, leaf))
	url := "file://" + leaf
	if (*shape) == "relative" {
		url = "../leaf"
	}
	parents := []string{"parent"}
	if (*shape) == "nested file" {
		parents = []string{"middle", "parent"}
	}
	for _, name := range parents {
		dir := filepath.Join(root, name)
		mustInitRepoWithFiles(t, dir, map[string]string{
			"hello.txt": "parent", ".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = " + url + "\n",
		})
		repo, err := git.PlainOpen(dir)
		assert.NoError(t, err)

		idx, err := repo.Storer.Index()
		assert.NoError(t, err)

		idx.Entries = append(idx.Entries, &index.Entry{Name: "lib", Mode: filemode.Submodule, Hash: hash})
		{
			setIndexErr := repo.Storer.SetIndex(idx)
			assert.NoError(t, setIndexErr)
		}

		wt, err := repo.Worktree()
		assert.NoError(t, err)

		hash, err = wt.Commit(
			"module revision",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		assert.NoError(t, err)

		url = "ssh://git@fixture" + dir
	}
	for _, remote := range []bool{false, true} {
		for _, restrict := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote=%t/restrict=%t", remote, restrict), func(t *testing.T) {
				testFetcherSubmoduleSchemesCaseCase(

					t, testFetcherSubmoduleSchemesCaseCaseInputs{
						identity: &identity,
						file:     &file,
						shape:    &shape,
						hash:     &hash,
						url:      &url,
						remote:   &remote,
						restrict: &restrict,
					})
			},
			)
		}
	}
}

func testFetcherExactPinBeforeRecursionCase(t *testing.T, parent *string,
	pin *plumbing.Hash,
	tc *struct {
		name    string
		refusal error
		calls   int
	},
) {
	t.Helper()
	counter := &pinTransport{Transport: client.Protocols["file"], refusal: tc.refusal}
	client.InstallProtocol("pin", counter)
	t.Cleanup(func() { client.InstallProtocol("pin", nil) })
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	spec := &manifest.GitRepository{
		Name:              "pin",
		URL:               "pin://" + (*parent),
		Reference:         &sourcev1.GitRepositoryRef{Commit: pin.String()},
		RecurseSubmodules: tc.name != "gitlink",
	}
	art, err := fetchtest.Fetch(t.Context(), f.Fetch, spec, (testInputs{}).readSet(spec), &sourcecache.Policy{}, nil)
	if err == nil {
		other := *spec
		other.Name = "other"
		shared := &Fetcher{Cache: f.Cache}
		reused, reuseErr := fetchtest.Fetch(
			t.Context(),
			shared.Fetch,
			&other,
			(testInputs{}).readSet(&other),
			&sourcecache.Policy{},
			nil,
		)
		assert.FatalIf(
			t,
			reuseErr != nil || reused.Key != art.Key,
			"compatible invocation acquisition: %v",
			reuseErr,
		)
	}

	if len(counter.requests) != tc.calls {
		t.Fatalf("parent requests=%d, want %d; %v", len(counter.requests), tc.calls, err)
	}
	first := counter.requests[0]
	if first.Depth != packp.DepthCommits(1) || !slices.Equal(first.Wants, []plumbing.Hash{(*pin)}) {
		t.Fatalf("first request depth=%v wants=%v", first.Depth, first.Wants)
	}
	if tc.refusal != nil && tc.calls == 1 {
		assert.FatalIf(t, !errors.Is(err, tc.refusal), "refusal: %v", err)
		return
	}
	assert.NoError(t, err)

	if tc.name == "gitlink" {
		{
			_, statErr := fs.Stat(art.FS, "lib")
			assert.FatalIf(t, !errors.Is(statErr, fs.ErrNotExist), "gitlink included: %v", statErr)
		}
		{
			data, readFileErr := fs.ReadFile(art.FS, "hello.txt")
			assert.FatalIf(
				t,
				readFileErr != nil || string(data) != "parent",
				"remaining content: %q, %v",
				data,
				readFileErr,
			)
		}
		return
	}
	{
		got, readFileErr2 := fs.ReadFile(art.FS, "lib/hello.txt")
		assert.FatalIf(t, readFileErr2 != nil || string(got) != "hi", "pinned module: %q, %v", got, readFileErr2)
	}
	{
		_, statErr2 := fs.Stat(art.FS, ".git")
		assert.FatalIf(t, !errors.Is(statErr2, fs.ErrNotExist), "worktree metadata: %v", statErr2)
	}
	reader, err := art.FS.Open("lib/hello.txt")
	assert.NoError(t, err)

	defer reader.Close()
	{
		err := f.Cache.Close()
		assert.NoError(t, err)
	}

	data := make([]byte, 2)
	{
		n, err := reader.Read(data)
		assert.FatalIf(t, n != 2 || err != nil || string(data) != "hi", "held reader: %q, %v", data, err)
	}
}

type testFetcherSubmoduleSchemesCaseCaseInputs struct {
	identity **string
	file     ***pinTransport
	shape    **string
	hash     *plumbing.Hash
	url      *string
	remote   *bool
	restrict *bool
}

func testFetcherSubmoduleSchemesCaseCase(t *testing.T, testCaseInput testFetcherSubmoduleSchemesCaseCaseInputs) {
	t.Helper()
	before := len((*(*testCaseInput.file)).requests)
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), RestrictSchemes: (*testCaseInput.restrict)}
	var artifact *store.SourceArtifact
	var err error
	if *testCaseInput.remote {
		artifact, err = f.FetchRemoteBase(
			t.Context(),
			&RemoteBaseRequest{
				URL:        (*testCaseInput.url),
				Ref:        testCaseInput.hash.String(),
				Timeout:    time.Minute,
				Submodules: true,
			},
			&sourcecache.Policy{},
		)
	} else {
		repo := &manifest.GitRepository{
			URL:               (*testCaseInput.url),
			Name:              "parent",
			Namespace:         "apps",
			SecretRef:         &manifest.LocalObjectReference{Name: "auth"},
			Reference:         &sourcev1.GitRepositoryRef{Commit: testCaseInput.hash.String()},
			RecurseSubmodules: true,
		}
		signer, keyErr := ssh.ParsePrivateKey([]byte(*(*testCaseInput.identity)))
		assert.NoError(t, keyErr)
		artifact, err = fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			repo,
			(testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{
					"identity":    *(*testCaseInput.identity),
					"known_hosts": "host " + string(ssh.MarshalAuthorizedKey(signer.PublicKey())),
				}}
			}}).readSet(repo),
			&sourcecache.Policy{},
			nil,
		)
	}
	if (*testCaseInput.restrict) && (*(*testCaseInput.shape)) != "relative" {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) ||
			!strings.Contains(err.Error(), "scheme not allowed") ||
			artifact != nil ||
			len((*(*testCaseInput.file)).
				requests,
			) != before, "restricted submodule: artifact=%v error=%v", artifact, err)
		return
	}
	assert.NoError(t, err)

	path := "lib/hello.txt"
	if (*(*testCaseInput.shape)) == "nested file" {
		path = "lib/lib/hello.txt"
	}
	{
		data, err := fs.ReadFile(artifact.FS, path)
		assert.FatalIf(
			t,
			err != nil || string(data) != "hi",
			"submodule content=%q error=%v",
			data,
			err,
		)
	}
}
