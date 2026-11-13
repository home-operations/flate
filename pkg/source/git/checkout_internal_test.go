package git

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

	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"

	"github.com/home-operations/flate/internal/assert"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	filetransport "github.com/go-git/go-git/v6/plumbing/transport/file"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_SubmoduleSchemes(t *testing.T) {
	t.Parallel()
	identity, _ := testSSHIdentity(t)
	for _, shape := range []string{"direct file", "nested file", "relative"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			testFetcherSubmoduleSchemesCase(t, &identity, &shape)
		},
		)
	}
}

type pinTransport struct {
	requests []*packp.UploadRequest
	refusal  error
}

func (p *pinTransport) connect(ctx context.Context, request TransportRequest) (Connection, error) {
	conn, err := filetransport.NewTransport(filetransport.Options{}).
		Connect(ctx, &transport.Request{URL: request.URL, Command: request.Command, Protocol: protocol.V0})
	if err != nil {
		return nil, err
	}
	refs := &packp.AdvRefs{}
	if err := refs.Decode(conn.Reader()); err != nil {
		_ = conn.Close()
		return nil, err
	}
	refs.Capabilities.Set(capability.AllowReachableSHA1InWant)
	var advertisement bytes.Buffer
	if err := refs.Encode(&advertisement); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &pinConnection{
		Connection: conn,
		reader:     io.MultiReader(&advertisement, conn.Reader()),
		writer:     &pinWriter{WriteCloser: conn.Writer(), owner: p},
	}, nil
}

type pinConnection struct {
	Connection

	reader io.Reader
	writer *pinWriter
}

func (c *pinConnection) Reader() io.Reader      { return c.reader }
func (c *pinConnection) Writer() io.WriteCloser { return c.writer }

type pinWriter struct {
	io.WriteCloser

	owner    *pinTransport
	data     bytes.Buffer
	recorded bool
}

func (w *pinWriter) Write(data []byte) (int, error) {
	if !w.recorded {
		_, _ = w.data.Write(data)
		request := &packp.UploadRequest{}
		if err := request.Decode(bytes.NewReader(w.data.Bytes())); err == nil {
			w.recorded = true
			w.owner.requests = append(w.owner.requests, request)
			if len(w.owner.requests) == 1 && w.owner.refusal != nil {
				return 0, w.owner.refusal
			}
		}
	}
	return w.WriteCloser.Write(data)
}

func TestFetcher_ExactPinBeforeRecursion(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			testFetcherExactPinBeforeRecursionCase(

				t, &parent, &pin, &tc)
		},
		)
	}
}

func testFetcherSubmoduleSchemesCase(t *testing.T, identity, shape *string) {
	t.Helper()
	file := &pinTransport{}
	routes := Transports{
		"ssh":  commandTransport{before: func(context.Context) error { return nil }}.connect,
		"file": file.connect,
	}

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
						routes:   routes,
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
	counter := &pinTransport{refusal: tc.refusal}
	f := &Fetcher{
		Network:       networktest.New(t),
		Cache:         cachetest.New(t, "", sourcecache.New),
		GitTransports: Transports{"pin": counter.connect},
	}
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
		shared := &Fetcher{Network: networktest.New(t), Cache: f.Cache, GitTransports: f.GitTransports}
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
	if first.Depth.Deepen != 1 || !slices.Equal(first.Wants, []plumbing.Hash{(*pin)}) {
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
	routes   Transports
	file     **pinTransport
	shape    **string
	hash     *plumbing.Hash
	url      *string
	remote   *bool
	restrict *bool
}

func testFetcherSubmoduleSchemesCaseCase(t *testing.T, testCaseInput testFetcherSubmoduleSchemesCaseCaseInputs) {
	t.Helper()
	before := len((*testCaseInput.file).requests)
	f := &Fetcher{
		Network:         networktest.New(t),
		Cache:           cachetest.New(t, "", sourcecache.New),
		RestrictSchemes: (*testCaseInput.restrict),
		GitTransports:   testCaseInput.routes,
	}
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
			(testInputs{secrets: func(_, _ string) store.ReferenceFacts {
				secretInput := &manifest.Secret{StringData: map[string]any{
					"identity":    *(*testCaseInput.identity),
					"known_hosts": "host " + string(ssh.MarshalAuthorizedKey(signer.PublicKey())),
				}}

				return store.ReferenceFacts{Data: secretInput, Original: secretInput}
			}}).readSet(repo),
			&sourcecache.Policy{},
			nil,
		)
	}
	if (*testCaseInput.restrict) && (*(*testCaseInput.shape)) != "relative" {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) ||
			!strings.Contains(err.Error(), "scheme not allowed") ||
			artifact != nil ||
			len((*testCaseInput.file).
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
