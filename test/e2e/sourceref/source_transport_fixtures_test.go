package sourceref_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func setSourceRef(t *testing.T, root, ref string) {
	t.Helper()
	path := filepath.Join(root, "flux/entry.yaml")
	data, err := os.ReadFile(path)
	assert.NoError(t, err)
	line := ""
	if ref != "" {
		line = "  ref: {" + ref + "}\n"
	}
	testutil.WriteFileAt(t, path, strings.Replace(string(data), "  ref: {tag: v1.0.0}\n", line, 1))
}

type sourceRefLoader struct {
	store storer.Storer
	calls atomic.Int64
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (l *sourceRefLoader) Load(endpoint *transport.Endpoint) (storer.Storer, error) {
	if endpoint.Host != "fixture.invalid" || endpoint.Path != "/cluster" {
		return nil, transport.ErrRepositoryNotFound
	}
	l.calls.Add(1)
	return l.store, nil
}

func installSourceRefTransport(t *testing.T, storage storer.Storer) *sourceRefLoader {
	t.Helper()
	loader := &sourceRefLoader{store: storage}
	previous := client.Protocols["git"]
	client.InstallProtocol("git", server.NewClient(loader))
	t.Cleanup(func() { client.InstallProtocol("git", previous) })
	return loader
}
