package testutil_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestGitSSH_SessionAdmission(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		kind    string
		payload []byte
	}{
		{name: "non-exec request", kind: "shell"},
		{name: "malformed payload", kind: "exec", payload: []byte{0}},
		{
			name:    "wrong service",
			kind:    "exec",
			payload: ssh.Marshal(struct{ Command string }{"git-receive-pack '/unadmitted'"}),
		},
		{
			name:    "unterminated path",
			kind:    "exec",
			payload: ssh.Marshal(struct{ Command string }{"git-upload-pack '/unadmitted"}),
		},
		{
			name:    "unadmitted root",
			kind:    "exec",
			payload: ssh.Marshal(struct{ Command string }{"git-upload-pack '/unadmitted'"}),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "content", "admitted repository\n")
			testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
				Author: &object.Signature{Name: "Test", Email: "test@example.com"},
			})
			var want bytes.Buffer
			assert.NoError(t, transport.AdvertiseRefs(
				t.Context(), repo.Storer, &want, transport.UploadPackService, false, protocol.V0,
			))
			_, key, err := ed25519.GenerateKey(rand.Reader)
			assert.NoError(t, err)
			signer, err := ssh.NewSignerFromKey(key)
			assert.NoError(t, err)
			fixture := &testutil.GitSSH{Host: signer, Allowed: signer.PublicKey(), Roots: []string{root}}
			address := testutil.LocalGitSSH(t, fixture)
			client, err := ssh.Dial("tcp", address, &ssh.ClientConfig{
				User:            "git",
				Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
				HostKeyCallback: ssh.FixedHostKey(signer.PublicKey()),
			})
			assert.NoError(t, err)
			defer client.Close()
			session, err := client.NewSession()
			assert.NoError(t, err)
			defer session.Close()
			accepted, err := session.SendRequest(tt.kind, true, tt.payload)
			assert.NoError(t, err)
			assert.Equal(t, accepted, false)
			var got bytes.Buffer
			session.Stdout = &got
			session.Stdin = bytes.NewReader([]byte("0000"))
			assert.NoError(t, session.Run("git-upload-pack '"+root+"'"))
			assert.Equal(t, got.String(), want.String())
			assert.Equal(t, fixture.Authenticated.Load(), int32(1))
			accepted, err = session.SendRequest("exec", true, nil)
			assert.Equal(t, accepted, false)
			assert.Equal(t, errors.Is(err, io.EOF), true)
		})
	}
}
