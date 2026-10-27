package bucket

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_SuppliedCredentials(t *testing.T) {
	t.Parallel()
	b := &manifest.Bucket{Name: "bucket", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"accesskey": "filled", "secretkey": "filled"}}}})
	assert.NoError(t, err)
	s := store.New()
	s.SetSupplied(snapshot)
	s.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"accesskey": "repository", "secretkey": manifest.ValuePlaceholder("secretkey")}})
	var reads store.ReadSet
	reads.Init(s, b.Named(), nil)
	defer reads.Close()
	credentials, err := (&operation{secrets: source.SecretLookup(&reads)}).resolveCredentials(b)
	assert.NoError(t, err)
	value, err := credentials.GetWithContext(nil)
	assert.NoError(t, err)
	assert.Equal(t, value.AccessKeyID, "repository")
	assert.Equal(t, value.SecretAccessKey, "filled")
}
