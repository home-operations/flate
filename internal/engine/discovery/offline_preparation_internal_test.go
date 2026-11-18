package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdvance_UnreadablePreparationDefers(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"unreadable", "readable failure", "malformed readable", "canceled"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML(
				"parent",
				"apps",
				"  path: ./parent\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: Secret, name: settings}]}\n",
			))
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [healthy.yaml, child.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"parent/healthy.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"parent/child.yaml",
				testutil.KustomizationYAML("child", "apps", "  path: ${TOKEN}\n"),
			)
			st := store.New()
			value := "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"
			if state == "readable failure" {
				value = "["
			}
			secret := &manifest.Secret{Name: "settings", Namespace: "apps", StringData: map[string]any{"TOKEN": value}}
			if state == "malformed readable" {
				secret.StringData = nil
				secret.Data = map[string]any{"TOKEN": "!!!"}
			}
			st.AddObject(secret)
			l := loader.New(st)
			configureSelection(t, l, selection.Request{Changed: true})
			session, err := beginTest(t.Context(), Config{
				Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st, Loader: l,
				Trees: testTrees(t), Selection: selection.Request{Changed: true},
			})
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			parent := testutil.KustomizationID("apps", "parent")
			child := testutil.KustomizationID("apps", "child")
			record, _ := l.Index.Lookup(parent)
			declaration := l.IndexedObject(parent, record)
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{})
			st.RecordEmitted(entry.Named(), &store.Emission{Children: []store.Child{declaration}, Complete: true}, nil)
			st.AddObject(declaration.Obj, declaration.Doc)
			ctx := t.Context()
			if state == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err = session.Advance(ctx, &selection.AdmissionResult{Needed: []manifest.NamedResource{child}})
			switch state {
			case "unreadable":
				assert.NoError(t, err)
				assert.Equal(t, st.GetDeclaration(child) == nil, true)
				frame, prepareErr := session.prepareDocuments(
					ctx,
					declaration.Obj.(*manifest.Kustomization),
					session.cfg.Input,
					nil,
				)
				assert.NoError(t, prepareErr)
				assert.Equal(t, frame.complete, false)
				assert.Equal(t, frame.registered, false)
				assert.Equal(t, len(frame.children), 0)
				assert.Equal(t, frame.receipt == nil, true)
				assert.NoError(
					t,
					session.prepareProducer(ctx, declaration.Obj.(*manifest.Kustomization), session.cfg.Input, nil),
				)
				assert.Equal(t, frame.registered, false)
			case "readable failure", "malformed readable":
				if err == nil {
					err = session.prepareProducer(
						ctx,
						declaration.Obj.(*manifest.Kustomization),
						session.cfg.Input,
						nil,
					)
				}
				assert.FatalIf(t, err == nil, "readable render failure disappeared")
			case "canceled":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			}
			assert.Equal(
				t,
				st.GetObject(
					manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "healthy"},
				) == nil,
				true,
			)
		})
	}
}
