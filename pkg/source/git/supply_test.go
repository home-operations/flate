package git

import (
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"testing"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_SuppliedHTTPSMaterial(t *testing.T) {
	t.Parallel()
	repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.invalid/source.git", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"username": "filled", "password": "filled"}}}})
	assert.NoError(t, err)
	s := store.New()
	s.SetSupplied(snapshot)
	s.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"username": "repository"}})
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	defer reads.Close()
	prepared, err := (&operation{Fetcher: &Fetcher{}, secrets: source.SecretLookup(&reads)}).resolveTransport(repo, sourcecache.Policy{})
	assert.NoError(t, err)
	auth, ok := prepared.options.Auth.(*githttp.BasicAuth)
	if !ok || auth.Username != "repository" || auth.Password != "filled" {
		t.Fatal("effective HTTPS auth was not fixed during preparation")
	}
}
