package git

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"io/fs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"testing"
	"testing/synctest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	gitlib "github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ContentIsolation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepoWithFiles(t, root, map[string]string{"hello.txt": "hello", "docs.tmp": "docs"})
	commit := mustSetRefToHEAD(t, root, "refs/heads/other")
	c := cachetest.New(t, "", sourcecache.New)
	f := &Fetcher{Cache: c}
	base := &manifest.GitRepository{URL: "file://" + root, Reference: &sourcev1.GitRepositoryRef{Commit: commit}}
	first, err := f.fetch(t.Context(), gitAcquisition{spec: base, options: gitlib.CloneOptions{URL: base.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Stat(first.FS, "docs.tmp"); err != nil {
		t.Fatalf("plain checkout lost docs.tmp: %v", err)
	}
	for _, tc := range []struct {
		name     string
		change   func(*manifest.GitRepository, *Fetcher)
		wantSame bool
	}{
		{"unchanged", func(*manifest.GitRepository, *Fetcher) {}, true},
		{"name and namespace without auth", func(r *manifest.GitRepository, _ *Fetcher) { r.Name, r.Namespace = "other", "other" }, true},
		{"empty ignore", func(r *manifest.GitRepository, _ *Fetcher) { r.Ignore = new("") }, true},
		{"empty sparse", func(r *manifest.GitRepository, _ *Fetcher) { r.SparseCheckout = []string{} }, true},
		{"ignore", func(r *manifest.GitRepository, _ *Fetcher) { r.Ignore = new("*.tmp\n") }, false},
		{"sparse", func(r *manifest.GitRepository, _ *Fetcher) { r.SparseCheckout = []string{"hello.txt"} }, false},
		{"submodules", func(r *manifest.GitRepository, _ *Fetcher) { r.RecurseSubmodules = true }, false},
		{"ref", func(r *manifest.GitRepository, _ *Fetcher) {
			r.Reference = &sourcev1.GitRepositoryRef{Commit: commit, Branch: "other"}
		}, false},
		{"URL", func(r *manifest.GitRepository, _ *Fetcher) { r.URL += "/." }, false},
		{"depth", func(_ *manifest.GitRepository, f *Fetcher) { f.Depth = 1 }, false},
		{"secret", func(r *manifest.GitRepository, _ *Fetcher) {
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
		}, false},
		{"proxy", func(r *manifest.GitRepository, _ *Fetcher) {
			r.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := *base
			fetcher := &Fetcher{Cache: c}
			tc.change(&r, fetcher)
			art, err := fetcher.fetch(t.Context(), gitAcquisition{spec: &r, options: gitlib.CloneOptions{URL: r.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
			if err != nil {
				t.Fatal(err)
			}
			_, identity, viewErr := source.OperationView(t.Context(), art.FS)
			if viewErr != nil || identity == "" || art.Content != identity {
				t.Fatalf("returned generation = %q, want %q: %v", art.Content, identity, viewErr)
			}
			if tc.name == "unchanged" && art.Content != first.Content {
				t.Fatal("warm hit changed content generation")
			}
			if (art.Key == first.Key) != tc.wantSame {
				t.Fatalf("identity=%q, base=%q, want same=%t", art.Key, first.Key, tc.wantSame)
			}
			if tc.name == "ignore" {
				if _, err := fs.Stat(art.FS, "docs.tmp"); !os.IsNotExist(err) {
					t.Fatalf("ignored checkout retained docs.tmp: %v", err)
				}
				plain, err := f.fetch(t.Context(), gitAcquisition{spec: base, options: gitlib.CloneOptions{URL: base.URL, NoCheckout: true}}, sourcecache.Policy{Retry: sourcecache.RetryConfig{}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fs.Stat(plain.FS, "docs.tmp"); err != nil {
					t.Fatalf("plain checkout lost docs.tmp after ignored acquisition: %v", err)
				}
			}
		})
	}
}

type timeoutTransport struct {
	transport.Transport
	observe func(context.Context)
}

type timeoutSession struct {
	transport.UploadPackSession
	observe func(context.Context)
}

func (tr timeoutTransport) NewUploadPackSession(*transport.Endpoint, transport.AuthMethod) (transport.UploadPackSession, error) {
	return timeoutSession{observe: tr.observe}, nil
}

func (s timeoutSession) AdvertisedReferencesContext(ctx context.Context) (*packp.AdvRefs, error) {
	s.observe(ctx)
	return nil, transport.ErrRepositoryNotFound
}

func (timeoutSession) Close() error { return nil }

func TestFetcher_SourceTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "default"},
		{name: "shorter", timeout: 10 * time.Second},
		{name: "longer", timeout: 180 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.timeout
			if want == 0 {
				want = time.Minute
			}
			calls := 0
			client.InstallProtocol("timeout", timeoutTransport{observe: func(ctx context.Context) {
				calls++
				deadline, ok := ctx.Deadline()
				remaining := time.Until(deadline)
				if !ok || remaining <= want-time.Second || remaining > want {
					t.Errorf("git advertisement deadline: present=%t remaining=%v, want %v", ok, remaining, want)
				}
			}})
			t.Cleanup(func() { client.InstallProtocol("timeout", nil) })
			r := &manifest.GitRepository{URL: "timeout://fixture/repository"}
			if tc.timeout != 0 {
				r.Timeout = &metav1.Duration{Duration: tc.timeout}
			}
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
			_, err := f.Fetch(t.Context(), r, (testInputs{}).readSet(r), sourcecache.Policy{})
			if calls == 0 || !errors.Is(err, transport.ErrRepositoryNotFound) {
				t.Fatalf("controlled advertisement not reached: calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestFetcher_AttemptTimeout(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen=%t", reopen), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				src := t.TempDir()
				mustInitRepo(t, src)
				mustSetRefToHEAD(t, src, "refs/heads/other")
				calls := 0
				installCommandTransport(t, func(ctx context.Context) error {
					calls++
					select {
					case <-time.After(3 * time.Second):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				root := t.TempDir()
				f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
				r := &manifest.GitRepository{URL: "commands://" + src, RecurseSubmodules: true, Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/other"}, Timeout: &metav1.Duration{Duration: 5 * time.Second}}
				start := time.Now()
				_, err := f.Fetch(t.Context(), r, (testInputs{}).readSet(r), sourcecache.Policy{MaxAge: time.Hour})
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 5*time.Second || calls != 2 {
					t.Fatalf("aggregate attempt: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
				}
				if reopen {
					if err := f.Cache.Close(); err != nil {
						t.Fatal(err)
					}
					f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
				}
				longer := *r
				longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
				artifact, err := f.Fetch(t.Context(), &longer, (testInputs{}).readSet(&longer), sourcecache.Policy{MaxAge: time.Hour})
				if err != nil {
					t.Fatalf("longer policy reused failure: %v", err)
				}
				body, err := fs.ReadFile(artifact.FS, "hello.txt")
				if err != nil || string(body) != "hi" {
					t.Fatalf("acquired content=%q error=%v", body, err)
				}
				if err := f.Cache.Close(); err != nil {
					t.Fatal(err)
				}
				f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
				shorter := longer
				shorter.Timeout = &metav1.Duration{Duration: time.Nanosecond}
				before := calls
				warm, err := f.Fetch(t.Context(), &shorter, (testInputs{}).readSet(&shorter), sourcecache.Policy{MaxAge: time.Hour})
				if err != nil || calls != before || warm.Key != artifact.Key {
					t.Fatalf("timeout changed content identity: calls=%d->%d error=%v", before, calls, err)
				}
			})
		})
	}
}
