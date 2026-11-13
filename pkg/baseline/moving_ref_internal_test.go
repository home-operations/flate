package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

func TestAutoResolve_MovingNamedRef(t *testing.T) {
	t.Parallel()
	t.Run("advances", func(t *testing.T) {
		t.Parallel()
		var previous string
		for round := range 2 {
			origin := t.TempDir()
			pin := initRepoWithFile(t, origin, "a.yaml", "base")
			tip := writeAndCommit(t, origin, "a.yaml", "moving-tip")
			st := openHelper(t, origin).Storer
			fixture := &movingRefTransport{pin: pin, tip: tip}
			hashes := make([]plumbing.Hash, 0, 6)
			for i, hash := range []plumbing.Hash{tip, pin} {
				commit, err := object.GetCommit(st, hash)
				assert.NoError(t, err)
				tree, err := commit.Tree()
				assert.NoError(t, err)
				hashes = append(hashes, hash, tree.Hash, tree.Entries[0].Hash)
				var packed bytes.Buffer
				_, err = packfile.NewEncoder(&packed, st, false).Encode(hashes, 0)
				assert.NoError(t, err)
				fixture.packs[i] = packed.Bytes()
			}
			url := "moving://origin/repo"
			dir := historylessCheckout(t, url, "a.yaml", "moving-tip")
			cfg := Config{
				Path: dir, Base: "main", Cache: cachetest.New(t, "", sourcecache.New),
				Network: networktest.New(t), GitTransports: sourcegit.Transports{"moving": fixture.connect},
			}
			res, err := AutoResolve(t.Context(), cfg)
			t.Logf("remote %d: listed A=%s, fetched B=%s, requests=%d", round, pin, tip, len(fixture.requests))
			_, checkoutErr := openHelper(t, dir).CommitObject(pin)
			assert.Equal(t, errors.Is(checkoutErr, plumbing.ErrObjectNotFound), true)
			if err != nil {
				t.Errorf("AutoResolve with moving main must retain listed base %s: %v", pin, err)
				continue
			}
			assert.Equal(t, res.BaseInput.Revision, pin.String())
			body, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(body), "base")
			assert.Diff(t, res.Changes.Paths(), []string{"a.yaml"})
			assert.Equal(t, len(fixture.requests), 2)
			assert.Equal(t, fixture.requests[0].Depth.Deepen, 1)
			assert.Equal(t, fixture.requests[1].Depth.Deepen, 0)
			snapshot := fmt.Sprintf("%s\n%s\n%v", res.BaseInput.Revision, body, res.Changes.Paths())
			if round != 0 {
				assert.Equal(t, snapshot, previous)
			}
			previous = snapshot

			fetches := len(fixture.requests)
			cfg.Base = pin.String()
			reused, err := AutoResolve(t.Context(), cfg)
			assert.NoError(t, err)
			assert.Equal(t, reused.BaseInput.Revision, pin.String())
			reusedBody, err := fs.ReadFile(reused.BaseInput.FS, "a.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(reusedBody), string(body))
			assert.Diff(t, reused.Changes.Paths(), res.Changes.Paths())
			assert.Equal(t, len(fixture.requests), fetches)
			_, checkoutErr = openHelper(t, dir).CommitObject(pin)
			assert.Equal(t, errors.Is(checkoutErr, plumbing.ErrObjectNotFound), true)
		}
	})
	t.Run("rewritten", func(t *testing.T) {
		t.Parallel()
		origin := t.TempDir()
		pin := initRepoWithFile(t, origin, "a.yaml", "base")
		rewritten := t.TempDir()
		tip := initRepoWithFile(t, rewritten, "a.yaml", "rewritten-tip")
		source := openHelper(t, rewritten).Storer
		commit, err := object.GetCommit(source, tip)
		assert.NoError(t, err)
		tree, err := commit.Tree()
		assert.NoError(t, err)
		var packed bytes.Buffer
		_, err = packfile.NewEncoder(&packed, source, false).Encode(
			[]plumbing.Hash{tip, tree.Hash, tree.Entries[0].Hash}, 0,
		)
		assert.NoError(t, err)
		fixture := &movingRefTransport{pin: pin, tip: tip, packs: [2][]byte{packed.Bytes(), packed.Bytes()}}
		url := "moving://origin/rewritten"
		dir := historylessCheckout(t, url, "a.yaml", "rewritten-tip")
		cacheRoot := t.TempDir()
		cfg := Config{
			Path: dir, Base: "main", Cache: cachetest.New(t, cacheRoot, sourcecache.New),
			Network: networktest.New(t), GitTransports: sourcegit.Transports{"moving": fixture.connect},
		}
		_, err = AutoResolve(t.Context(), cfg)
		assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
		assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, strings.Contains(err.Error(), pin.String()), true)
		assert.Equal(t, len(fixture.requests), 2)
		assert.Equal(t, fixture.requests[0].Depth.Deepen, 1)
		assert.Equal(t, fixture.requests[1].Depth.Deepen, 0)
		assert.NoError(t, cfg.Cache.Close())
		cfg.Cache = cachetest.New(t, cacheRoot, sourcecache.New)
		cfg.Base = pin.String()
		_, err = AutoResolve(t.Context(), cfg)
		assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
		assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, strings.Contains(err.Error(), pin.String()), true)
		assert.Equal(t, len(fixture.requests), 3)
		assert.Equal(t, fixture.requests[2].Depth.Deepen, 0)
		_, checkoutErr := openHelper(t, dir).CommitObject(pin)
		assert.Equal(t, errors.Is(checkoutErr, plumbing.ErrObjectNotFound), true)
	})
}

type movingRefTransport struct {
	pin, tip    plumbing.Hash
	packs       [2][]byte
	connections int
	requests    []*packp.UploadRequest
}

func (f *movingRefTransport) connect(
	ctx context.Context,
	req sourcegit.TransportRequest,
) (sourcegit.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Command != transport.UploadPackService {
		return nil, fmt.Errorf("unexpected command %q", req.Command)
	}
	tip := f.tip
	if f.connections == 0 {
		tip = f.pin
	}
	f.connections++
	refs := &packp.AdvRefs{References: []*plumbing.Reference{
		plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), tip),
	}}
	refs.Capabilities.Set(capability.Shallow)
	conn := &movingRefConnection{remote: f}
	if err := refs.Encode(&conn.output); err != nil {
		return nil, err
	}
	return conn, nil
}

type movingRefConnection struct {
	remote        *movingRefTransport
	output, input bytes.Buffer
	request       *packp.UploadRequest
	closed        bool
}

func (c *movingRefConnection) Reader() io.Reader      { return &c.output }
func (c *movingRefConnection) Writer() io.WriteCloser { return c }

func (c *movingRefConnection) Write(data []byte) (int, error) {
	_, _ = c.input.Write(data)
	if c.request != nil {
		return len(data), nil
	}
	request := &packp.UploadRequest{}
	reader := bytes.NewReader(c.input.Bytes())
	if err := request.Decode(reader); err != nil {
		//nolint:nilerr // Upload requests arrive in fragments, so an incomplete frame MUST wait for the next write.
		return len(data), nil
	}
	if !slices.Equal(request.Wants, []plumbing.Hash{c.remote.tip}) {
		return 0, fmt.Errorf("unexpected wants %v, want moved tip %s", request.Wants, c.remote.tip)
	}
	c.input.Next(c.input.Len() - reader.Len())
	c.request = request
	c.remote.requests = append(c.remote.requests, request)
	if request.Depth.Deepen > 0 {
		if err := (&packp.ShallowUpdate{Shallows: []plumbing.Hash{c.remote.tip}}).Encode(&c.output); err != nil {
			return 0, err
		}
	}
	if _, err := pktline.WriteString(&c.output, "NAK\n"); err != nil {
		return 0, err
	}
	if request.Depth.Deepen == 0 {
		// A full pack MUST contain both commits, not concatenated packfiles.
		_, _ = c.output.Write(c.remote.packs[1])
	} else {
		_, _ = c.output.Write(c.remote.packs[0])
	}
	return len(data), nil
}

func (c *movingRefConnection) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.request == nil {
		return nil
	}
	var haves packp.UploadHaves
	if err := haves.Decode(&c.input); err != nil {
		return fmt.Errorf("decode existing commits: %w", err)
	}
	if slices.Contains(haves.Haves, c.remote.tip) {
		return fmt.Errorf("recovery advertised moved tip %s as present", c.remote.tip)
	}
	return nil
}
