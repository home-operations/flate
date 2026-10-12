package deterministic

import (
	"strings"
	"testing"

	bcryptlib "golang.org/x/crypto/bcrypt"

	"github.com/home-operations/flate/internal/assert"
)

func TestCryptoFuncs_ShapeAndInputs(t *testing.T) {
	seed := SeedFor("rel", "ns")
	fm := Funcs(seed)
	hash := fm["bcrypt"].(func(string) string)("password")
	cost, err := bcryptlib.Cost([]byte(hash))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, cost, bcryptlib.DefaultCost)
	assert.Equal(t, len(hash), 60)
	assert.Equal(t, hash, Funcs(seed)["bcrypt"].(func(string) string)("password"))
	if hash == Funcs(seed)["bcrypt"].(func(string) string)("different") {
		t.Fatal("bcrypt placeholder ignores password")
	}
	if hash == Funcs(SeedFor("other", "ns"))["bcrypt"].(func(string) string)("password") {
		t.Fatal("bcrypt placeholder ignores seed")
	}
	htpasswd := fm["htpasswd"].(func(string, string) string)
	assert.Equal(t, htpasswd("bad:user", "password"), "invalid username: bad:user")
	assert.Equal(t, htpasswd("user", strings.Repeat("x", 73)), "user:failed to encrypt string with bcrypt: "+bcryptlib.ErrPasswordTooLong.Error())
	assert.Equal(t, Funcs(seed)["htpasswd"].(func(string, string) string)("user", "password"), "user:"+hash)
	encrypted, err := encryptAES(newStream(seed), "key", "")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, encrypted, "")
}
