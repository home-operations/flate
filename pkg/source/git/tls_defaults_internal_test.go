package git

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type testFetcherRecursiveTLSDefaultsCaseInputs struct {
	module     *string
	moduleHash *plumbing.Hash
	child      *string
	exact      *bool
}

func testFetcherRecursiveTLSDefaultsCase(t *testing.T, testCaseInput testFetcherRecursiveTLSDefaultsCaseInputs) {
	t.Helper()

	root := t.TempDir()
	certificate2 := tlsGitCertificate(t)

	parentHandler, moduleHandler := gitHTTPHandler(t, root), gitHTTPHandler(t, (*testCaseInput.module))
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/module/") {
			moduleHandler.ServeHTTP(w, r)
		} else {
			parentHandler.ServeHTTP(w, r)
		}
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{certificate2.certificate}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	url := "file://" + (*testCaseInput.module)
	if (*testCaseInput.child) == "private-ca" {
		url = srv.URL + "/module"
	}
	mustInitRepoWithFiles(
		t,
		root,
		map[string]string{
			"hello.txt":   "parent",
			".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = " + url + "\n",
		},
	)
	r, err := git.PlainOpen(root)
	assert.NoError(t, err)

	head, err := r.Head()
	assert.NoError(t, err)

	commit, err := r.CommitObject(head.Hash())
	assert.NoError(t, err)

	tree, err := commit.Tree()
	assert.NoError(t, err)

	tree.Entries = append(
		tree.Entries,
		object.TreeEntry{Name: "lib", Mode: filemode.Submodule, Hash: (*testCaseInput.moduleHash)},
	)
	encoded := r.Storer.NewEncodedObject()
	{
		encodeErr2 := tree.Encode(encoded)
		assert.NoError(t, encodeErr2)
	}

	commit.TreeHash, err = r.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	commit.ParentHashes = []plumbing.Hash{head.Hash()}
	encoded = r.Storer.NewEncodedObject()
	{
		encodeErr3 := commit.Encode(encoded)
		assert.NoError(t, encodeErr3)
	}

	hash, err := r.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	{
		setReferenceErr := r.Storer.SetReference(
			plumbing.NewHashReference(head.Name(), hash),
		)
		assert.NoError(t, setReferenceErr)
	}

	fInputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts {
		secretInput := &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": certificate2.pem},
		}

		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	}}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	repo := gitRepoWithSecret("recursive", srv.URL, "tls")
	repo.RecurseSubmodules = true
	if *testCaseInput.exact {
		repo.Reference = &sourcev1.GitRepositoryRef{Commit: hash.String()}
	}
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		fInputs.readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	if (*testCaseInput.child) == "private-ca" {
		{
			_, ok := errors.AsType[x509.UnknownAuthorityError](err)
			assert.FatalIf(t, !ok, "child inherited private CA: %v", err)
		}
		return
	}
	assert.NoError(t, err)

	{
		data, err := fs.ReadFile(art.FS, "lib/hello.txt")
		assert.FatalIf(t, err != nil || string(data) != "hi", "child content=%q, error=%v", data, err)
	}
}
