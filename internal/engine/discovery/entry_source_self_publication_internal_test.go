package discovery

import (
	"errors"
	"io/fs"
	"maps"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_PublishSelfSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/file", "local material")
	st := store.New()
	result, err := Run(t.Context(), Config{Path: root, Store: st, SelfURLs: []string{"https://fixture.invalid/self"}})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindGitRepository,
		"metadata": map[string]any{"name": "self", "namespace": "apps"},
		"spec":     map[string]any{"url": "ssh://git@fixture.invalid/self.git"},
	}, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	id := parsed.Object.Named()
	st.AddObject(parsed.Object)
	published, err := result.Admission.Publish(
		t.Context(),
		id,
		aliasRequestReads(t, st, id),
		func(child store.Child) (bool, error) {
			assert.Equal(t, child.Obj == parsed.Object, true)
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
			return true, nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, published, true)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
	assert.NoError(t, constructAdmissionTest(t.Context(), result.Admission, id))
	material := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	data, err := fs.ReadFile(material.FS, "apps/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "local material")
	assert.Equal(t, st.GetObject(id) == parsed.Object, true)
	assert.Equal(t, result.Admission.resolvedSources.snapshot()[id].synthetic, false)
}

func TestAdmission_ProjectedBootstrapReusesCheckout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		spec         map[string]any
		sourceIgnore string
		excludesFile bool
		wantProvider store.SourceProvider
	}{
		{name: "default", wantProvider: store.ProviderInput},
		{
			name: "ignore", spec: map[string]any{"ignore": "/other/file"},
			excludesFile: true, wantProvider: store.ProviderAlias,
		},
		{
			name: "include",
			spec: map[string]any{"include": []any{map[string]any{
				"repository": map[string]any{"name": "included"}, "fromPath": "apps", "toPath": "included",
			}}},
			wantProvider: store.ProviderAlias,
		},
		{
			name: "sparse", spec: map[string]any{"sparseCheckout": []any{"apps"}},
			excludesFile: true, wantProvider: store.ProviderAlias,
		},
		{name: "submodules", spec: map[string]any{"recurseSubmodules": true}, wantProvider: store.ProviderFetch},
		{
			name: "admitted policy", sourceIgnore: "/other/file",
			excludesFile: true, wantProvider: store.ProviderAlias,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/file", "local material")
			testutil.WriteFile(t, root, "other/file", "outside apps")
			if tc.sourceIgnore != "" {
				testutil.WriteFile(t, root, ".sourceignore", tc.sourceIgnore+"\n")
			}
			const url = "https://fixture.invalid/self"
			spec := map[string]any{"url": url, "ref": map[string]any{"name": "refs/heads/main"}}
			maps.Copy(spec, tc.spec)
			if spec["recurseSubmodules"] == true {
				repo := testutil.GitInit(t, root)
				head := testutil.GitCommitAll(t, repo, "checkout", &git.CommitOptions{
					Author: &object.Signature{Name: "test", Email: "test@example.com"},
				})
				spec["ref"] = map[string]any{"commit": head.String()}
			}

			result, err := Run(t.Context(), Config{Path: root, Store: store.New(), SelfURLs: []string{url}})
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			admission := result.Admission
			checkout := admission.Input()
			assert.Equal(t, checkout.Provider, store.ProviderInput)
			checkoutPolicy := gittree.PolicyKey(inputprojection.Authored(checkout).FS)
			assert.Equal(t, checkoutPolicy == gittree.DefaultSourcePolicyKey, tc.sourceIgnore == "")

			id := manifest.BootstrapSourceID
			source, err := manifest.ParseDoc(map[string]any{
				"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindGitRepository,
				"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
				"spec":     spec,
			}, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			request := sourceRequest{object: source.Object, projected: true}
			if tc.wantProvider == store.ProviderInput {
				planned, planErr := admission.resolve(t.Context(), id, &request)
				assert.NoError(t, planErr)
				assert.Equal(t, planned.provider, store.ProviderAlias)
			}

			var reads store.ReadSet
			reads.Init(admission.store, id, nil)
			defer reads.Retire()
			request.reads = &reads
			selected, err := admission.resolve(t.Context(), id, &request)
			assert.NoError(t, err)
			assert.Equal(t, selected.object, source.Object)
			assert.Equal(t, selected.provider, tc.wantProvider)
			if tc.wantProvider == store.ProviderInput {
				assert.Equal(t, selected.artifact, checkout)
				return
			}
			if tc.wantProvider == store.ProviderFetch {
				assert.Equal(t, selected.artifact, nil)
				// Recursive acquisition returns before resolve can test checkout reuse.
				assert.Equal(t, selected.usesFullGitCheckout(), false)
				return
			}
			assert.FatalIf(t, !selected.artifact.Available(), "expected an available alias view")
			assert.Equal(t, selected.artifact == checkout, false)
			_, err = fs.Stat(selected.artifact.FS, "apps/file")
			assert.NoError(t, err)
			_, err = fs.Stat(selected.artifact.FS, "other/file")
			if tc.excludesFile {
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
