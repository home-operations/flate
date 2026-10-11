package git

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/home-operations/flate/pkg/manifest"
)

type pinTransport struct {
	transport.Transport
	requests []*packp.UploadPackRequest
	refusal  error
}

type pinSession struct {
	transport.UploadPackSession
	owner *pinTransport
}

func (p *pinTransport) NewUploadPackSession(endpoint *transport.Endpoint, auth transport.AuthMethod) (transport.UploadPackSession, error) {
	session, err := p.Transport.NewUploadPackSession(endpoint, auth)
	return &pinSession{session, p}, err
}

func (p *pinSession) UploadPack(ctx context.Context, request *packp.UploadPackRequest) (*packp.UploadPackResponse, error) {
	p.owner.requests = append(p.owner.requests, request)
	if len(p.owner.requests) == 1 && p.owner.refusal != nil {
		return nil, p.owner.refusal
	}
	return p.UploadPackSession.UploadPack(ctx, request)
}

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
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := repo.Storer.Config()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Raw.SetOption("uploadpack", "", "allowReachableSHA1InWant", "true")
	if err := repo.Storer.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}

	head, err := repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	commit, err := repo.CommitObject(head.Hash())
	if err != nil {
		t.Fatal(err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "lib", Mode: filemode.Submodule})
	pin := plumbing.ZeroHash
	for _, moduleHash := range []string{oldModule, newModule} {
		for i := range tree.Entries {
			if tree.Entries[i].Name == "lib" {
				tree.Entries[i].Hash = plumbing.NewHash(moduleHash)
			}
		}
		encoded := repo.Storer.NewEncodedObject()
		if err := tree.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		commit.ParentHashes, commit.Message = []plumbing.Hash{commit.Hash}, moduleHash
		encoded = repo.Storer.NewEncodedObject()
		if err := commit.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		commit.Hash, err = repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if pin.IsZero() {
			pin = commit.Hash
		}
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(head.Name(), commit.Hash)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		refusal error
		calls   int
	}{
		{"exact", nil, 1}, {"gitlink", nil, 1}, {"fallback", errors.New("exact request refused"), 2},
		{"auth", transport.ErrAuthorizationFailed, 1}, {"auth-required", transport.ErrAuthenticationRequired, 1},
		{"canceled", context.Canceled, 1}, {"deadline", context.DeadlineExceeded, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := &pinTransport{Transport: client.Protocols["file"], refusal: tc.refusal}
			client.InstallProtocol("pin", counter)
			t.Cleanup(func() { client.InstallProtocol("pin", nil) })
			f := &Fetcher{Cache: newGitCache(t, "")}
			art, err := f.Fetch(t.Context(), &manifest.GitRepository{
				Name: "pin", URL: "pin://" + parent, Reference: &manifest.GitRepositoryRef{Commit: pin.String()}, RecurseSubmodules: tc.name != "gitlink",
			})
			if len(counter.requests) != tc.calls {
				t.Fatalf("parent requests=%d, want %d; %v", len(counter.requests), tc.calls, err)
			}
			first := counter.requests[0]
			if first.Depth != packp.DepthCommits(1) || !slices.Equal(first.Wants, []plumbing.Hash{pin}) {
				t.Fatalf("first request depth=%v wants=%v", first.Depth, first.Wants)
			}
			if tc.refusal != nil && tc.calls == 1 {
				if !errors.Is(err, tc.refusal) {
					t.Fatalf("refusal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "gitlink" {
				if _, err := fs.Stat(art.FS, "lib"); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("gitlink included: %v", err)
				}
				if data, err := fs.ReadFile(art.FS, "hello.txt"); err != nil || string(data) != "parent" {
					t.Fatalf("remaining content: %q, %v", data, err)
				}
				return
			}
			if got, err := fs.ReadFile(art.FS, "lib/hello.txt"); err != nil || string(got) != "hi" {
				t.Fatalf("pinned module: %q, %v", got, err)
			}
			if _, err := fs.Stat(art.FS, ".git"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("worktree metadata: %v", err)
			}
			reader, err := art.FS.Open("lib/hello.txt")
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if err := f.Cache.Close(); err != nil {
				t.Fatal(err)
			}
			data := make([]byte, 2)
			if n, err := reader.Read(data); n != 2 || err != nil || string(data) != "hi" {
				t.Fatalf("held reader: %q, %v", data, err)
			}
		})
	}
}
