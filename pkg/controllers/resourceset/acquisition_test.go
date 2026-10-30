package resourceset

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_ProviderAcquisitionSettlement(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ambient", "rendered Secret", "changed completed provider"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var lists, rejected atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if username, password, ok := r.BasicAuth(); !ok || username != "rendered" || password != "fixture" {
					if ok {
						rejected.Add(1)
					}
					w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/tags/list"):
					lists.Add(1)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"name":"images","tags":["pending"]}`)
				case strings.Contains(r.URL.Path, "/manifests/"):
					w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
					w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("a", 64))
					w.Header().Set("Content-Length", "2")
					fmt.Fprint(w, "{}")
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			host := strings.TrimPrefix(server.URL, "https://")
			credentials := fmt.Sprintf(`{"auths":{%q:{"username":"rendered","password":"fixture"}}}`, host)
			registryConfig := filepath.Join(t.TempDir(), "config.json")
			machine := credentials
			if mode != "ambient" {
				machine = `{"auths":`
			}
			testutil.WriteFileAt(t, registryConfig, machine)
			network := new(source.Network)
			t.Cleanup(network.Close)
			st := store.New()
			tasks := task.NewBounded(2)
			t.Cleanup(tasks.BlockTillDone)
			c := New(st, tasks, false)
			c.Inputs = &oci.Fetcher{Network: network, Cache: cachetest.New(t, t.TempDir(), cache.New), RegistryConfig: registryConfig}
			c.Configure(base.Options{})
			c.StartLifecycle()
			ready := staticRSIP("a-ready", "effective", nil, map[string]string{"tag": "ready"})
			pending := &manifest.ResourceSetInputProvider{Name: "b-pending", Namespace: "effective",
				Type: fluxopv1.InputProviderOCIArtifactTag, URL: "oci://" + host + "/images", Insecure: true, SecretRef: &manifest.LocalObjectReference{Name: "registry-auth"}}
			rs := &manifest.ResourceSet{Name: "consumer", Namespace: "effective",
				InputsFrom:        []fluxopv1.InputProviderReference{{Name: pending.Name}, {Name: ready.Name}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>'}\ndata: {tag: '<< inputs.tag >>'}\n"}
			st.AddObjects([]manifest.BaseManifest{ready, pending, rs})
			before, err := json.Marshal(ready)
			assert.NoError(t, err)
			var publications atomic.Int32
			defer st.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, payload any) {
				if emission, ok := payload.(*store.Emission); id == rs.Named() && ok && emission.Complete {
					publications.Add(1)
				}
			}, false)()
			var run *base.Execution
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) { run = c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone) })
			tasks.BlockTillDone()
			defer run.Close()
			secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: pending.Namespace, Name: pending.SecretRef.Name}
			if run.Outcome() != schedule.Blocked || !slices.Equal(run.Blocked, []manifest.NamedResource{secretID}) || lists.Load() != 0 || publications.Load() != 0 || st.GetArtifact(rs.Named(), store.ArtifactContent) != nil {
				t.Fatalf("partial collection outcome=%v blocked=%v listings=%d publications=%d", run.Outcome(), run.Blocked, lists.Load(), publications.Load())
			}
			wantTag := "ready"
			if mode != "ambient" {
				st.AddObject(&manifest.Secret{Name: secretID.Name, Namespace: secretID.Namespace, StringData: map[string]any{".dockerconfigjson": credentials}})
				if mode == "changed completed provider" {
					st.AddObject(staticRSIP(ready.Name, ready.Namespace, nil, map[string]string{"tag": "updated"}))
					wantTag = "updated"
				}
				if run.Reads.Valid() {
					t.Fatal("rendered input did not invalidate the parked collection")
				}
			}
			tasks.Go(t.Context(), "settle", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
			tasks.BlockTillDone()
			if mode != "ambient" {
				if publications.Load() != 0 || lists.Load() != 0 || st.GetArtifact(rs.Named(), store.ArtifactContent) != nil {
					t.Fatal("stale partial collection acquired or published inputs")
				}
				info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: st, ID: rs.Named(), ReconcileNode: c.ReconcileNode})
				if info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
					t.Fatalf("rendered Secret did not take precedence over broken machine credentials: %+v", info)
				}
			} else {
				if run.Outcome() != schedule.Terminal {
					t.Fatalf("settled collection outcome=%v", run.Outcome())
				}
				tasks.Go(t.Context(), "settle again", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
				tasks.BlockTillDone()
			}
			emission, ok := st.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
			if !ok {
				t.Fatal("completed collection did not publish its output")
			}
			if publications.Load() != 1 || lists.Load() != 1 || rejected.Load() != 0 || len(emission.Manifests) != 2 || !emission.Complete || len(st.Warnings()) != 0 {
				t.Fatalf("completed collection publications=%d listings=%d rejected=%d output=%+v warnings=%+v", publications.Load(), lists.Load(), rejected.Load(), emission, st.Warnings())
			}
			data := map[string]any{}
			for _, doc := range emission.Manifests {
				name, _ := manifest.DocMetadata(doc.Document)
				data[name] = doc.Document["data"].(map[string]any)["tag"]
			}
			assert.Diff(t, data, map[string]any{ready.Name: wantTag, pending.Name: "pending"})
			after, err := json.Marshal(ready)
			assert.NoError(t, err)
			assert.Equal(t, string(after), string(before))
		})
	}
}

func TestReconcile_ProviderAcquisitionRefresh(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"digest", "defaults", "credentials"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			firstDigest := "sha256:" + strings.Repeat("a", 64)
			secondDigest := "sha256:" + strings.Repeat("b", 64)
			var changed atomic.Bool
			var lists atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				password, digest := "first", firstDigest
				if changed.Load() {
					if change == "credentials" {
						password = "second"
					}
					if change != "defaults" {
						digest = secondDigest
					}
				}
				if user, pass, ok := r.BasicAuth(); !ok || user != "fixture" || pass != password {
					w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/tags/list"):
					lists.Add(1)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"name":"images","tags":["current"]}`)
				case strings.Contains(r.URL.Path, "/manifests/"):
					w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
					w.Header().Set("Docker-Content-Digest", digest)
					w.Header().Set("Content-Length", "2")
					fmt.Fprint(w, "{}")
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			registryConfig := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(t, registryConfig, `{}`)
			network := new(source.Network)
			t.Cleanup(network.Close)
			st := store.New()
			tasks := task.NewBounded(2)
			t.Cleanup(tasks.BlockTillDone)
			c := New(st, tasks, false)
			c.Inputs = &oci.Fetcher{Network: network, Cache: cachetest.New(t, t.TempDir(), cache.New), RegistryConfig: registryConfig}
			c.Configure(base.Options{})
			c.StartLifecycle()
			provider := &manifest.ResourceSetInputProvider{Name: "images", Namespace: "apps",
				Type: fluxopv1.InputProviderOCIArtifactTag, URL: "oci://" + strings.TrimPrefix(server.URL, "https://") + "/images", Insecure: true,
				SecretRef:     &manifest.LocalObjectReference{Name: "registry-auth"},
				DefaultValues: fluxopv1.ResourceSetInput{"color": {Raw: []byte(`"blue"`)}}}
			credentialData := func(password string) map[string]any {
				return map[string]any{".dockerconfigjson": fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":%q}}}`, strings.TrimPrefix(server.URL, "https://"), password)}
			}
			secret := &manifest.Secret{Name: "registry-auth", Namespace: "apps", StringData: credentialData("first")}
			rs := &manifest.ResourceSet{Name: "consumer", Namespace: "apps",
				InputsFrom:        []fluxopv1.InputProviderReference{{Name: provider.Name}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: current}\ndata: {tag: '<< inputs.tag >>', digest: '<< inputs.digest >>', color: '<< inputs.color >>'}\n"}
			st.AddObjects([]manifest.BaseManifest{provider, secret, rs})
			render := func(digest, color string) *store.Emission {
				t.Helper()
				var run *base.Execution
				tasks.Go(t.Context(), "refresh", func(ctx context.Context) { run = c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone) })
				tasks.BlockTillDone()
				defer run.Close()
				if run.Outcome() != schedule.Terminal || run.Reused || !run.Reads.Valid() {
					t.Fatalf("fresh acquisition outcome=%v reused=%t valid=%t", run.Outcome(), run.Reused, run.Reads.Valid())
				}
				info, _ := st.GetStatus(rs.Named())
				assert.Equal(t, info.Status, store.StatusReady)
				emission, ok := st.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
				if !ok || !emission.Complete || emission.Fingerprint == "" || len(emission.Manifests) != 1 {
					t.Fatalf("missing complete emission: %+v", emission)
				}
				assert.Diff(t, emission.Manifests[0].Document["data"], map[string]any{"tag": "current", "digest": digest, "color": color})
				return emission
			}
			first := render(firstDigest, "blue")
			assert.Equal(t, lists.Load(), int32(1))
			changed.Store(true)
			wantDigest, wantColor := secondDigest, "blue"
			switch change {
			case "defaults":
				updated := *provider
				updated.DefaultValues = fluxopv1.ResourceSetInput{"color": {Raw: []byte(`"green"`)}}
				st.AddObject(&updated)
				wantDigest, wantColor = firstDigest, "green"
			case "credentials":
				updated := *secret
				updated.StringData = credentialData("second")
				st.AddObject(&updated)
			}
			current := render(wantDigest, wantColor)
			assert.Equal(t, lists.Load(), int32(2))
			if current.Fingerprint == first.Fingerprint {
				t.Fatal("changed live inputs retained the prior emission fingerprint")
			}
			assert.Diff(t, first.Manifests[0].Document["data"], map[string]any{"tag": "current", "digest": firstDigest, "color": "blue"})
			assert.Equal(t, string(provider.DefaultValues["color"].Raw), `"blue"`)
			assert.Diff(t, secret.StringData, credentialData("first"))
		})
	}
}
