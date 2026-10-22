package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func generatorForTest(t *testing.T, artifacts []any) (*manifest.RawObject, map[string]any) {
	t.Helper()
	doc := map[string]any{"apiVersion": "source.extensions.fluxcd.io/v1beta1", "kind": "ArtifactGenerator", "metadata": map[string]any{"name": "generator", "namespace": "apps", "uid": "authored"}, "spec": map[string]any{"sources": []any{map[string]any{"alias": "repo", "kind": "GitRepository", "name": "repo"}}, "artifacts": artifacts}}
	obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*manifest.RawObject), authoritative
}
func runForTest(t *testing.T, c *Controller, id manifest.NamedResource) {
	t.Helper()
	c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) { t.Cleanup(c.ReconcileNode(ctx, id, manifest.DrainNone).Close) })
	c.Tasks.BlockTillDone()
	if c.Tasks.Failures() != 0 {
		t.Fatal("reconcile task panicked")
	}
}

func TestArtifactGenerator_AllStrategies(t *testing.T) {
	t.Parallel()
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	archive := tar.NewWriter(gz)
	if err := archive.WriteHeader(&tar.Header{Name: "inside.txt", Mode: 0o755, Size: 6}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write([]byte("inside")); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		operations []any
		path, want string
	}{
		{"directory", []any{map[string]any{"from": "@repo/app/", "to": "@artifact/app/"}}, "app/app/file.txt", "file"},
		{"merge", []any{map[string]any{"from": "@repo/base.yaml", "to": "@artifact/values.yaml"}, map[string]any{"from": "@repo/overlay.yaml", "to": "@artifact/values.yaml", "strategy": "Merge"}}, "values.yaml", "a: 1\nb: 3\n"},
		{"extract", []any{map[string]any{"from": "@repo/archive.TGZ", "to": "@artifact/out/", "strategy": "Extract"}}, "out/inside.txt", "inside"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			owner, err := cache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			c := New(st, tasks, owner)

			obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "copy": tc.operations}})
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			st.SetArtifact(repo, &store.SourceArtifact{Kind: repo.Kind, Key: "input", Digest: "sha256:input", FS: fstest.MapFS{
				"app/file.txt": {Data: []byte("file"), Mode: 0o644}, "base.yaml": {Data: []byte("a: 1\nb: 2\n")}, "overlay.yaml": {Data: []byte("b: 3\n")}, "archive.TGZ": {Data: compressed.Bytes()},
			}})
			runForTest(t, c, obj.Named())
			status, _ := st.GetStatus(obj.Named())
			if status.Status != store.StatusReady {
				t.Fatalf("generator status = %+v", status)
			}
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
			artifact, ok := st.GetArtifact(target).(*store.SourceArtifact)
			if !ok || artifact.FS == nil || artifact.Key == "" || !strings.HasPrefix(artifact.Digest, "sha256:") || artifact.Kind != manifest.KindExternalArtifact || artifact.Revision != "latest@"+artifact.Digest {
				t.Fatalf("generated artifact = %+v", artifact)
			}
			data, err := fs.ReadFile(artifact.FS, tc.path)
			if err != nil || string(data) != tc.want {
				t.Fatalf("output %s = %q, %v", tc.path, data, err)
			}
			accepted, ok := st.GetArtifact(obj.Named()).(*store.Emission)
			if !ok || !accepted.Complete || accepted.Fingerprint == "" || len(accepted.Children) != 1 {
				t.Fatalf("accepted inventory = %+v", accepted)
			}
			runForTest(t, c, obj.Named())
			if st.GetArtifact(target) != artifact {
				t.Fatal("complete replay changed accepted bytes")
			}
		})
	}
}

func TestArtifactGenerator_PartialFailure(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	owner, err := cache.New(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	c := New(st, tasks, owner)

	var outputs []any
	for _, name := range []string{"a", "b", "c"} {
		outputs = append(outputs, map[string]any{"name": name, "copy": []any{map[string]any{"from": "@repo/" + name, "to": "@artifact/file"}}})
	}
	obj, doc := generatorForTest(t, outputs)
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
	input := fstest.MapFS{"a": {Data: []byte("a")}, "b": {Data: []byte("b")}, "c": {Data: []byte("c")}}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "first", Digest: "first", FS: &observedFS{FS: input, failPath: "c"}})
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())
	if status.Status != store.StatusFailed {
		t.Fatalf("partial status = %+v", status)
	}
	for _, name := range []string{"a", "b"} {
		artifact, ok := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: name}).(*store.SourceArtifact)
		if !ok || artifact.FS == nil {
			t.Fatalf("accepted %s unavailable", name)
		}
		if data, err := fs.ReadFile(artifact.FS, "file"); err != nil || string(data) != name {
			t.Fatalf("accepted %s = %q, %v", name, data, err)
		}
	}
	accepted := st.GetArtifact(obj.Named()).(*store.Emission)
	if accepted.Complete || accepted.Fingerprint != "" {
		t.Fatal("partial attempt became replayable")
	}
	runForTest(t, c, obj.Named())
	status, _ = st.GetStatus(obj.Named())
	if status.Status != store.StatusReady {
		t.Fatalf("retry status = %+v", status)
	}
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "c"}
	artifact, ok := st.GetArtifact(target).(*store.SourceArtifact)
	if !ok {
		t.Fatal("same-input retry did not execute the failed output")
	}
	if data, err := fs.ReadFile(artifact.FS, "file"); err != nil || string(data) != "c" {
		t.Fatalf("retried c = %q, %v", data, err)
	}
	accepted = st.GetArtifact(obj.Named()).(*store.Emission)
	if !accepted.Complete || len(accepted.Children) != 3 || accepted.Fingerprint == "" {
		t.Fatalf("retried inventory = %+v", accepted)
	}
	nextInput := maps.Clone(input)
	delete(nextInput, "b")
	st.SetArtifact(repo, &store.SourceArtifact{Key: "second", Digest: "second", FS: nextInput})
	runForTest(t, c, obj.Named())
	status, _ = st.GetStatus(obj.Named())
	if status.Status != store.StatusFailed {
		t.Fatalf("successor failure status = %+v", status)
	}
	if current := st.GetArtifact(target); current != artifact {
		t.Fatal("failure withdrew an unvisited accepted output")
	}
	if data, err := fs.ReadFile(artifact.FS, "file"); err != nil || string(data) != "c" {
		t.Fatalf("retained c = %q, %v", data, err)
	}
}

func TestArtifactGenerator_Disabled(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)

	obj, doc := generatorForTest(t, nil)
	doc["metadata"].(map[string]any)["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": "DiSaBlEd"}
	st.AddObject(obj, doc)
	var emissions atomic.Int64
	unsubscribe := st.AddListener(store.EventArtifactUpdated, func(manifest.NamedResource, any) { emissions.Add(1) }, false)
	t.Cleanup(unsubscribe)
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())
	if status.Status != store.StatusReady || st.GetArtifact(obj.Named()) != nil {
		t.Fatalf("disabled result = %+v", status)
	}
	if emissions.Load() != 0 {
		t.Fatal("disabled generator published an artifact")
	}
	if _, err := decodeSpec(obj.Spec); err == nil {
		t.Fatal("disabled fixture should be invalid when enabled")
	}
}

type observedFS struct {
	fs.FS
	opens    atomic.Int64
	failPath string
	failed   atomic.Bool
	onOpen   func(string)
}

func (f *observedFS) Open(name string) (fs.File, error) {
	if f.onOpen != nil {
		f.onOpen(name)
	}
	if name == f.failPath && f.failed.CompareAndSwap(false, true) {
		return nil, fs.ErrPermission
	}
	if name == "file" || strings.HasSuffix(name, "/file") {
		f.opens.Add(1)
	}
	return f.FS.Open(name)
}

func TestArtifactGenerator_Metadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, revision, origin string
		sourceMetadata         map[string]string
		wantOrigin             string
		uid                    bool
	}{
		{"selected_origin", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": "origin"}, "origin", true},
		{"origin_revision_fallback", "@repo", "@repo", nil, "revision", true},
		{"empty_origin_fallback", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": ""}, "revision", true},
		{"absent_origin_alias", "", "@absent", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			c := New(st, task.NewBounded(2), owner)

			_, doc := generatorForTest(t, []any{map[string]any{"name": "{App}", "revision": tc.revision, "originRevision": tc.origin, "copy": []any{map[string]any{"from": "@repo/apps/{App}/file", "to": "@artifact/file"}}}})
			spec := doc["spec"].(map[string]any)
			spec["pathPattern"] = "@repo/apps/{App}"
			spec["commonMetadata"] = map[string]any{"labels": map[string]any{"App": "common", "app.kubernetes.io/managed-by": "common", "source.extensions.fluxcd.io/generator": "common", "kept": "common"}, "annotations": map[string]any{"note": "kept"}}
			if !tc.uid {
				delete(doc["metadata"].(map[string]any), "uid")
			}
			parsed, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			obj := parsed.(*manifest.RawObject)
			st.AddObject(obj, doc)
			input := &observedFS{FS: fstest.MapFS{"apps/Prod/file": {Data: []byte("bytes")}}}
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			selected := &store.SourceArtifact{Key: "input", Digest: "digest", Revision: "revision", FS: input, Metadata: tc.sourceMetadata}
			st.SetArtifact(repo, selected)
			runForTest(t, c, obj.Named())
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "prod"}
			artifact, ok := st.GetArtifact(target).(*store.SourceArtifact)
			if !ok {
				status, _ := st.GetStatus(obj.Named())
				t.Fatalf("missing generated source: %+v", status)
			}
			wantRevision := "latest@" + artifact.Digest
			if tc.revision != "" {
				wantRevision = "revision"
			}
			if artifact.Revision != wantRevision || artifact.Metadata["org.opencontainers.image.revision"] != tc.wantOrigin {
				t.Fatalf("metadata = %+v; revision=%q origin=%q", artifact, wantRevision, tc.wantOrigin)
			}
			if tc.origin == "@absent" && len(artifact.Metadata) != 0 {
				t.Fatalf("absent origin alias added metadata: %+v", artifact.Metadata)
			}
			emission, ok := st.GetArtifact(obj.Named()).(*store.Emission)
			if !ok {
				t.Fatal("missing complete generator inventory")
			}
			metadata := emission.Children[0].Doc["metadata"].(map[string]any)
			uid := ""
			if tc.uid {
				uid = "authored"
			}
			want := map[string]any{"App": "prod", "app.kubernetes.io/managed-by": "source-watcher", "source.extensions.fluxcd.io/generator": uid, "kept": "common"}
			if diff := cmp.Diff(want, metadata["labels"]); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(map[string]any{"note": "kept"}, metadata["annotations"]); diff != "" {
				t.Fatal(diff)
			}
			for _, key := range []string{"uid", "resourceVersion", "creationTimestamp"} {
				if _, present := metadata[key]; present {
					t.Fatalf("invented %s", key)
				}
			}
			sourceRef := emission.Children[0].Doc["spec"].(map[string]any)["sourceRef"]
			if diff := cmp.Diff(map[string]any{"apiVersion": obj.APIVersion, "kind": obj.Kind, "name": obj.Name, "namespace": obj.Namespace}, sourceRef); diff != "" {
				t.Fatal(diff)
			}
			opens := input.opens.Load()
			updated := *selected
			updated.Metadata = maps.Clone(selected.Metadata)
			if updated.Metadata == nil {
				updated.Metadata = map[string]string{}
			}
			updated.Metadata["unrelated"] = "changed"
			st.SetArtifact(repo, &updated)
			runForTest(t, c, obj.Named())
			if st.GetArtifact(target) != artifact || input.opens.Load() != opens {
				t.Fatal("unrelated metadata prevented replay")
			}
			updatedDoc := maps.Clone(doc)
			updatedMetadata := maps.Clone(doc["metadata"].(map[string]any))
			updatedMetadata["uid"] = "next"
			updatedDoc["metadata"] = updatedMetadata
			st.AddObject(obj, updatedDoc)
			runForTest(t, c, obj.Named())
			emission, ok = st.GetArtifact(obj.Named()).(*store.Emission)
			if !ok {
				t.Fatal("UID refresh left no authoritative generator inventory")
			}
			labels := emission.Children[0].Doc["metadata"].(map[string]any)["labels"].(map[string]any)
			if labels["source.extensions.fluxcd.io/generator"] != "next" || input.opens.Load() != opens {
				t.Fatal("cache-hit metadata did not refresh without input work")
			}
			if tc.origin == "@repo" {
				next := updated
				next.Metadata = maps.Clone(updated.Metadata)
				next.Metadata["org.opencontainers.image.revision"] = "next-origin"
				st.SetArtifact(repo, &next)
				runForTest(t, c, obj.Named())
				fresh := st.GetArtifact(target).(*store.SourceArtifact)
				if fresh.Revision != "revision" || fresh.Metadata["org.opencontainers.image.revision"] != "next-origin" {
					t.Fatalf("origin-only metadata stayed stale: %+v", fresh)
				}
				last := next
				last.Revision = "next-revision"
				st.SetArtifact(repo, &last)
				runForTest(t, c, obj.Named())
				fresh = st.GetArtifact(target).(*store.SourceArtifact)
				if fresh.Revision != "next-revision" || fresh.Metadata["org.opencontainers.image.revision"] != "next-origin" {
					t.Fatalf("selected metadata stayed stale: %+v", fresh)
				}
			}
			disabledDoc := maps.Clone(updatedDoc)
			disabledMetadata := maps.Clone(updatedMetadata)
			disabledMetadata["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": "disabled"}
			disabledDoc["metadata"] = disabledMetadata
			st.AddObject(obj, disabledDoc)
			beforeDisabled := st.GetArtifact(target)
			var emissions atomic.Int64
			unsubscribe := st.AddListener(store.EventArtifactUpdated, func(manifest.NamedResource, any) { emissions.Add(1) }, false)
			runForTest(t, c, obj.Named())
			unsubscribe()
			if emissions.Load() != 0 || st.GetArtifact(target) != beforeDisabled {
				t.Fatal("disable annotation arrival changed accepted outputs")
			}
		})
	}
}

func TestArtifactGenerator_RequiredUnusedAlias(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), nil)

	_, doc := generatorForTest(t, []any{map[string]any{"name": "output", "copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}}})
	spec := doc["spec"].(map[string]any)
	spec["sources"] = append(spec["sources"].([]any), map[string]any{"alias": "unused", "name": "unused", "kind": "GitRepository"})
	obj, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "input", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}})
	repo.Name = "unused"
	st.UpdateStatus(repo, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "unavailable unused alias"})
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())
	if status.Status != store.StatusFailed || st.GetArtifact(obj.Named()) != nil {
		t.Fatalf("unused alias result = %+v", status)
	}
}

func TestArtifactGenerator_InputAvailability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		artifact     *store.SourceArtifact
		sourceStatus store.Status
		sourceReason store.StatusReason
		want         store.Status
		wantReason   store.StatusReason
	}{
		{"missing", nil, "", "", store.StatusPending, ""},
		{"key_without_fs", &store.SourceArtifact{Key: "key", LocalPath: "unused"}, store.StatusReady, "", store.StatusFailed, ""},
		{"contentless", &store.SourceArtifact{}, store.StatusReady, "", store.StatusFailed, ""},
		{"skipped", nil, store.StatusReady, store.ReasonSkipped, store.StatusReady, store.ReasonSkipped},
		{"readable_before_ready", &store.SourceArtifact{Key: "key", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}}, store.StatusFailed, "", store.StatusReady, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			c := New(st, task.NewBounded(2), owner)

			obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}}})
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			if tc.artifact != nil {
				st.SetArtifact(repo, tc.artifact)
			}
			if tc.sourceStatus != "" {
				st.UpdateStatus(repo, store.StatusInfo{Status: tc.sourceStatus, Reason: tc.sourceReason, Message: "source condition"})
			}
			runForTest(t, c, obj.Named())
			status, _ := st.GetStatus(obj.Named())
			if status.Status != tc.want || tc.wantReason != "" && status.Reason != tc.wantReason {
				t.Fatalf("input availability = %+v; want %s/%s", status, tc.want, tc.wantReason)
			}
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
			if (st.GetArtifact(target) != nil) != (tc.name == "readable_before_ready") {
				t.Fatalf("input %s output = %+v", tc.name, st.GetArtifact(target))
			}
			if tc.name == "missing" {
				c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
					c.ReconcileNode(ctx, obj.Named(), manifest.DrainCascade)
				})
				c.Tasks.BlockTillDone()
				status, _ = st.GetStatus(obj.Named())
				if status.Status != store.StatusFailed {
					t.Fatalf("missing input after drain = %+v", status)
				}
			}
		})
	}
}

func TestArtifactGenerator_AuthoritativeDocument(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(map[string]any) map[string]any
	}{
		{"missing", func(map[string]any) map[string]any { return nil }},
		{"different_group", func(doc map[string]any) map[string]any { doc["apiVersion"] = "other.example/v1"; return doc }},
		{"different_kind", func(doc map[string]any) map[string]any { doc["kind"] = "Other"; return doc }},
		{"different_name", func(doc map[string]any) map[string]any {
			doc["metadata"].(map[string]any)["name"] = "other"
			return doc
		}},
		{"different_spec", func(doc map[string]any) map[string]any { doc["spec"] = map[string]any{}; return doc }},
		{"spec_marshal", func(doc map[string]any) map[string]any { return doc }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			c := New(st, task.NewBounded(2), nil)

			obj, doc := generatorForTest(t, nil)
			if tc.name == "spec_marshal" {
				obj.Spec["invalid"] = make(chan int)
			}
			doc["metadata"].(map[string]any)["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": "disabled"}
			doc = tc.edit(doc)
			st.AddObject(obj, doc)
			var got error
			c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
				run := &execution{Controller: c}
				run.run.Init(c.Controller, obj.Named())
				got = run.reconcile(ctx, any(obj).(*manifest.RawObject), manifest.DrainNone)
			})
			c.Tasks.BlockTillDone()
			if !errors.Is(got, manifest.ErrInput) || !errors.Is(got, manifest.ErrFlux) || st.GetArtifact(obj.Named()) != nil {
				t.Fatalf("document rejection = %v", got)
			}
			if tc.name == "missing" && !strings.Contains(got.Error(), "missing authoritative document") {
				t.Fatalf("missing document diagnostic = %v", got)
			}
		})
	}
}

func TestArtifactGenerator_InputIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*store.SourceArtifact)
	}{
		{"key", func(input *store.SourceArtifact) { input.Key = "next-key" }},
		{"digest", func(input *store.SourceArtifact) { input.Digest = "next-digest" }},
		{"revision", func(input *store.SourceArtifact) { input.Revision = "next-revision" }},
		{"root", func(input *store.SourceArtifact) { input.LocalRoot = "next-root" }},
		{"path", func(input *store.SourceArtifact) { input.LocalPath = "next-path" }},
		{"origin_presence", func(input *store.SourceArtifact) {
			input.Metadata = map[string]string{"org.opencontainers.image.revision": ""}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			c := New(st, task.NewBounded(2), owner)

			obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}}})
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			input := store.SourceArtifact{Key: "key", Digest: "digest", Revision: "revision", LocalRoot: "root", LocalPath: "path", FS: fstest.MapFS{"file": {Data: []byte("same bytes")}}}
			st.SetArtifact(repo, &input)
			runForTest(t, c, obj.Named())
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
			before := st.GetArtifact(target).(*store.SourceArtifact)
			next := input
			tc.edit(&next)
			st.SetArtifact(repo, &next)
			runForTest(t, c, obj.Named())
			after := st.GetArtifact(target).(*store.SourceArtifact)
			if before.Key == after.Key || before.Digest != after.Digest {
				t.Fatalf("input %s identity before=%+v after=%+v", tc.name, before, after)
			}
		})
	}
}

func TestArtifactGenerator_EmptyAndCanceledInventory(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner, err := cache.New(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	_, doc := generatorForTest(t, []any{map[string]any{"name": "{App}", "copy": []any{map[string]any{"from": "@repo/apps/{App}/file", "to": "@artifact/file"}}}})
	doc["spec"].(map[string]any)["pathPattern"] = "@repo/apps/{App}"
	obj, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "first", Digest: "first", FS: fstest.MapFS{"apps/prod/file": {Data: []byte("bytes")}}})
	runForTest(t, c, obj.Named())
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "prod"}
	accepted := st.GetArtifact(target)
	if accepted == nil {
		t.Fatal("initial output missing")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st.SetArtifact(repo, &store.SourceArtifact{Key: "empty", Digest: "empty", FS: cancelOnReadDirFS{FS: fstest.MapFS{}, cancel: cancel}})
	var got error
	c.Tasks.Go(t.Context(), "generator", func(context.Context) {
		run := &execution{Controller: c}
		run.run.Init(c.Controller, obj.Named())
		got = run.reconcile(ctx, any(obj).(*manifest.RawObject), manifest.DrainNone)
	})
	c.Tasks.BlockTillDone()
	if !errors.Is(got, context.Canceled) || st.GetArtifact(target) != accepted {
		t.Fatalf("canceled evaluation = %v; output=%v", got, st.GetArtifact(target))
	}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "empty", Digest: "empty", Metadata: map[string]string{"fresh": "readable"}, FS: fstest.MapFS{}})
	runForTest(t, c, obj.Named())
	emission := st.GetArtifact(obj.Named()).(*store.Emission)
	if !emission.Complete || len(emission.Children) != 0 || emission.Fingerprint == "" || st.GetArtifact(target) != nil {
		t.Fatalf("zero expansion inventory = %+v", emission)
	}
}

type cancelOnReadDirFS struct {
	fs.FS
	cancel context.CancelFunc
}

func (f cancelOnReadDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(f.FS, name)
	f.cancel()
	return entries, err
}

func TestArtifactGenerator_MetadataCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner, err := cache.New(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "revision": "@repo", "originRevision": "@repo", "copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}}})
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
	check := func(want string) {
		t.Helper()
		artifact, ok := st.GetArtifact(target).(*store.SourceArtifact)
		if !ok {
			t.Fatalf("missing %s output", want)
		}
		data, err := fs.ReadFile(artifact.FS, "file")
		if err != nil || string(data) != want || artifact.Revision != want+"-revision" || artifact.Metadata["org.opencontainers.image.revision"] != want+"-origin" {
			t.Fatalf("captured %s = %q, %+v, %v", want, data, artifact, err)
		}
	}
	stable := &store.SourceArtifact{Key: "stable", Digest: "stable", Revision: "stable-revision", Metadata: map[string]string{"org.opencontainers.image.revision": "stable-origin"}, FS: fstest.MapFS{"file": {Data: []byte("stable")}}}
	st.SetArtifact(repo, stable)
	runForTest(t, c, obj.Named())
	check("stable")
	accepted := st.GetArtifact(target)
	var replaced atomic.Bool
	first := &observedFS{FS: fstest.MapFS{"file": {Data: []byte("first")}}}
	first.onOpen = func(name string) {
		if name == "file" && replaced.CompareAndSwap(false, true) {
			st.SetArtifact(repo, &store.SourceArtifact{Key: "next", Digest: "next", Revision: "next-revision", Metadata: map[string]string{"org.opencontainers.image.revision": "next-origin"}, FS: fstest.MapFS{"file": {Data: []byte("next")}}})
		}
	}
	st.SetArtifact(repo, &store.SourceArtifact{Key: "first", Digest: "first", Revision: "first-revision", Metadata: map[string]string{"org.opencontainers.image.revision": "first-origin"}, FS: first})
	runForTest(t, c, obj.Named())
	if !replaced.Load() || st.GetArtifact(target) != accepted {
		t.Fatal("stale evaluation replaced the accepted bundle")
	}
	runForTest(t, c, obj.Named())
	check("next")
}

func TestArtifactGenerator_MutableInput(t *testing.T) {
	t.Parallel()
	for _, identity := range []struct{ name, key, digest string }{{"missing_key", "", "digest"}, {"missing_generation", "family", ""}} {
		t.Run(identity.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			owner, err := cache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			c := New(st, tasks, owner)

			obj, doc := generatorForTest(t, []any{map[string]any{"name": "output", "copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}}}})
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}
			for _, value := range []string{"first", "second"} {
				st.SetArtifact(repo, &store.SourceArtifact{Key: identity.key, Digest: identity.digest, Metadata: map[string]string{"snapshot": value}, FS: fstest.MapFS{"file": {Data: []byte(value)}}})
				runForTest(t, c, obj.Named())
				artifact, ok := st.GetArtifact(target).(*store.SourceArtifact)
				if !ok {
					t.Fatalf("missing %s output", value)
				}
				data, err := fs.ReadFile(artifact.FS, "file")
				if err != nil || string(data) != value {
					t.Fatalf("mutable source = %q, %v; want %s", data, err, value)
				}
			}
		})
	}
}
