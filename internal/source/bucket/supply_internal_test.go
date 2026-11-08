package bucket

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_SuppliedCredentials(t *testing.T) {
	t.Parallel()
	b := &manifest.Bucket{Name: "bucket", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	snapshot := []store.Declaration{
		{
			Doc: testutil.ObjectDocument(
				&manifest.Secret{
					Name:       "auth",
					Namespace:  "apps",
					StringData: map[string]any{"accesskey": "filled", "secretkey": "filled"},
				},
			),
			Obj: &manifest.Secret{
				Name:       "auth",
				Namespace:  "apps",
				StringData: map[string]any{"accesskey": "filled", "secretkey": "filled"},
			},
			Origin: manifest.SuppliedDeclaration,
		},
	}

	s := store.New()
	for _, declaration := range snapshot {
		s.AddDeclaration(declaration)
	}
	s.AddObject(
		&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"accesskey": "repository"}},
	)
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
