package orchestrator

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	gogit "github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	resourcesetctrl "github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

const issueResourceSet = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: flux-operator
  namespace: flux-system
spec:
  serviceAccountName: flux-operator
  dependsOn:
    - apiVersion: apiextensions.k8s.io/v1
      kind: CustomResourceDefinition
      name: helmreleases.helm.toolkit.fluxcd.io
  resources:
    - apiVersion: source.toolkit.fluxcd.io/v1
      kind: OCIRepository
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        url: oci://ghcr.io/controlplaneio-fluxcd/charts/flux-operator
        ref:
          semver: '*'
        verify:
          provider: cosign
          matchOIDCIdentity:
          - issuer: ^https://token\.actions\.githubusercontent\.com$
            subject: ^https://github\.com/controlplaneio-fluxcd/charts/\.github/workflows/release\.yml@refs/tags/v\d+\.\d+\.\d+$
    - apiVersion: helm.toolkit.fluxcd.io/v2
      kind: HelmRelease
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        releaseName: flux-operator
        serviceAccountName: flux-operator
        chartRef:
          kind: OCIRepository
          name: flux-operator
        values:
          multitenancy:
            enabled: true
            defaultServiceAccount: default
          reporting:
            interval: 5m
`
const helmReleaseCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: helmreleases.helm.toolkit.fluxcd.io}
spec:
 group: helm.toolkit.fluxcd.io
 names: {kind: HelmRelease, plural: helmreleases, singular: helmrelease}
 scope: Namespaced
 versions:
 - name: v2
   served: true
   storage: true
   schema:
    openAPIV3Schema:
     type: object
     x-kubernetes-preserve-unknown-fields: true
`

func crdFixture(t *testing.T, mode string) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := gogit.PlainInit(dir, false); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n")
	rs := strings.ReplaceAll(issueResourceSet, "        interval: 1h\n", "        suspend: true\n        interval: 1h\n")
	testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
	switch mode {
	case "supplied":
		testutil.WriteFile(t, dir, "apps/crd.yaml", helmReleaseCRD)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- crd.yaml\n")
	case "indexed", "unreferenced":
		testutil.WriteFile(t, dir, "indexed/crd.yaml", helmReleaseCRD)
		if mode == "indexed" {
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- ../indexed/crd.yaml\n")
		}
	case "produced", "unordered", "sibling ks":
		dependency := ""
		if mode == "produced" {
			dependency = "crds"
		}
		testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", dependency)+"---\n"+ksYAML("crds", "crds", ""))
		testutil.WriteFile(t, dir, "crds/kustomization.yaml", "resources:\n- crd.yaml\n")
		testutil.WriteFile(t, dir, "crds/crd.yaml", helmReleaseCRD)
	case "helm produced", "sibling hr":
		testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
		if mode == "helm produced" {
			rs = strings.Replace(rs, "  dependsOn:\n", "  dependsOn:\n    - apiVersion: helm.toolkit.fluxcd.io/v2\n      kind: HelmRelease\n      name: crd-producer\n", 1)
		}
		testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
		testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: crd-producer, namespace: flux-system}
spec:
 interval: 1h
 chart:
  spec:
   chart: charts/crds
   sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- producer.yaml\n")
		testutil.WriteFile(t, dir, "charts/crds/Chart.yaml", "apiVersion: v2\nname: crds\nversion: 0.1.0\n")
		testutil.WriteFile(t, dir, "charts/crds/templates/crd.yaml", helmReleaseCRD)
	}
	return dir
}

func assertIssueChildren(t *testing.T, o *Orchestrator) {
	t.Helper()
	id := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "flux-operator"}
	info, ok := o.store.GetStatus(id)
	if !ok || info.Status != store.StatusReady {
		t.Fatalf("ResourceSet status=%+v", info)
	}
	art, ok := o.store.GetArtifact(id).(*store.ResourceSetArtifact)
	if !ok || len(art.Manifests) != 2 {
		t.Fatalf("ResourceSet artifact=%+v", art)
	}
	docs, err := manifest.DecodeDocs(strings.NewReader(issueResourceSet))
	if err != nil {
		t.Fatal(err)
	}
	spec := docs[0]["spec"].(map[string]any)
	children := spec["resources"].([]any)
	want := make([]map[string]any, 0, len(children))
	for _, child := range children {
		doc := child.(map[string]any)
		doc["spec"].(map[string]any)["suspend"] = true
		doc["metadata"].(map[string]any)["labels"] = map[string]any{"resourceset.fluxcd.controlplane.io/name": "flux-operator", "resourceset.fluxcd.controlplane.io/namespace": "flux-system"}
		want = append(want, doc)
		obj, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		assert.Diff(t, o.store.GetObject(obj.Named()), obj)
	}
	slices.SortFunc(want, func(a, b map[string]any) int { return strings.Compare(manifest.DocKind(a), manifest.DocKind(b)) })
	got := slices.Clone(art.Manifests)
	slices.SortFunc(got, func(a, b map[string]any) int { return strings.Compare(manifest.DocKind(a), manifest.DocKind(b)) })
	assert.Diff(t, got, want)
}

func TestMissingCRDs_ChildrenAndProducers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, enabled := range []bool{false, true} {
			for _, mode := range []string{"absent", "supplied", "indexed", "produced", "helm produced", "unreferenced", "unordered", "sibling ks", "sibling hr"} {
				t.Run(fmt.Sprintf("workers_%d/enabled_%t/%s", workers, enabled, mode), func(t *testing.T) {
					dir := crdFixture(t, mode)
					o, err := New(Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), Concurrency: workers, AllowMissingCRDs: enabled})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(o.Stop)
					res, err := o.Render(t.Context())
					if (mode == "absent" || mode == "unreferenced") && !enabled {
						if err == nil || !strings.Contains(err.Error(), "dependency not found") {
							t.Fatalf("strict absent error=%v", err)
						}
						assert.Equal(t, o.store.GetArtifact(manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "flux-operator"}) == nil, true)
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					assertIssueChildren(t, o)
					crd := manifest.NamedResource{Kind: manifest.KindCustomResourceDefinition, Name: "helmreleases.helm.toolkit.fluxcd.io"}
					var warnings []manifest.Warning
					for _, w := range res.Warnings {
						if w.Category == manifest.WarnMissingCRD {
							warnings = append(warnings, w)
						}
					}
					if mode == "absent" || mode == "unreferenced" {
						assert.Diff(t, warnings, []manifest.Warning{{Resource: crd, Category: manifest.WarnMissingCRD, Count: 1, Message: "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet/flux-system/flux-operator"}})
						assert.Equal(t, o.store.GetObject(crd) == nil, true)
					} else {
						assert.Equal(t, len(warnings), 0)
						assert.Equal(t, o.store.GetObject(crd) != nil, true)
					}
				})
			}
		}
	}
}

func TestWarnMissingCRDs_FinalEligibilityAndOrdering(t *testing.T) {
	s := store.New()
	o := &Orchestrator{store: s, cfg: Config{AllowMissingCRDs: true}}
	crdA := manifest.NamedResource{Kind: manifest.KindCustomResourceDefinition, Name: "a.example.com"}
	crdB := manifest.NamedResource{Kind: manifest.KindCustomResourceDefinition, Name: "b.example.com"}
	for _, tc := range []struct {
		name                string
		status              store.Status
		hasStatus, artifact bool
	}{
		{"z", store.StatusReady, true, true}, {"a", store.StatusReady, true, true}, {"failed", store.StatusFailed, true, true},
		{"pending", store.StatusPending, true, true}, {"no status", store.StatusReady, false, true}, {"no artifact", store.StatusReady, true, false},
	} {
		rs := &manifest.ResourceSet{Name: tc.name, Namespace: "ns", DependsOn: []fluxopv1.Dependency{{Kind: crdB.Kind, Name: crdB.Name, Namespace: "explicit"}, {Kind: crdA.Kind, Name: crdA.Name}, {Kind: crdA.Kind, Name: crdA.Name, Namespace: "explicit"}, {Kind: manifest.KindConfigMap, Name: "missing"}}}
		s.AddObject(rs)
		if tc.hasStatus {
			s.UpdateStatus(rs.Named(), tc.status, "")
		}
		if tc.artifact {
			s.SetArtifact(rs.Named(), &store.ResourceSetArtifact{})
		}
	}
	o.warnMissingCRDs()
	want := []manifest.Warning{}
	for _, crd := range []manifest.NamedResource{crdA, crdB} {
		want = append(want, manifest.Warning{Resource: crd, Category: manifest.WarnMissingCRD, Count: 1, Message: "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet/ns/a, ResourceSet/ns/z"})
	}
	assert.Diff(t, s.Warnings(), want)
	for _, state := range []string{"object", "ready", "pending", "failed", "disabled"} {
		t.Run(state, func(t *testing.T) {
			st := store.New()
			rs := &manifest.ResourceSet{Name: "app", Namespace: "ns", DependsOn: []fluxopv1.Dependency{{Kind: crdA.Kind, Name: crdA.Name, Namespace: "explicit"}}}
			st.AddObject(rs)
			st.UpdateStatus(rs.Named(), store.StatusReady, "")
			st.SetArtifact(rs.Named(), &store.ResourceSetArtifact{})
			cfg := Config{AllowMissingCRDs: true}
			switch state {
			case "object":
				st.AddObject(&manifest.RawObject{Kind: crdA.Kind, Name: crdA.Name})
			case "disabled":
				cfg.AllowMissingCRDs = false
			default:
				status := store.StatusReady
				if state == "pending" {
					status = store.StatusPending
				}
				if state == "failed" {
					status = store.StatusFailed
				}
				st.UpdateStatus(crdA, status, "")
			}
			(&Orchestrator{store: st, cfg: cfg}).warnMissingCRDs()
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}

func TestWarnMissingCRDs_ReplacementAndRetainedOutput(t *testing.T) {
	for _, state := range []string{"unchanged", "remove", "add", "failed gate", "filter remove", "filter add", "dedup"} {
		t.Run(state, func(t *testing.T) {
			s := store.New()
			tasks := task.NewBounded(2)
			o := &Orchestrator{store: s, cfg: Config{AllowMissingCRDs: true}}
			parent := ksID("parent")
			s.UpdateStatus(parent, store.StatusReady, "")
			opts := resourcesetctrl.Options{AllowMissingCRDs: true, ParentOf: func(manifest.NamedResource) (manifest.NamedResource, bool) { return parent, true }, RawSink: func(owner, parent manifest.NamedResource, doc map[string]any) {
				o.rsRawSink.Record(owner, parent, resourceset.DedupKey(doc), doc)
			}}
			c := resourcesetctrl.New(s, tasks, true)
			c.Configure(opts)
			c.Start(t.Context())
			t.Cleanup(c.Close)
			t.Cleanup(tasks.BlockTillDone)
			crd := fluxopv1.Dependency{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}
			rs := &manifest.ResourceSet{Name: "app", Namespace: "ns", ResourcesTemplate: "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: old}\nspec: {value: old}\n"}
			if state != "add" && state != "filter add" {
				rs.DependsOn = []fluxopv1.Dependency{crd}
			}
			s.AddObject(rs)
			if blocked := c.ReconcileNode(t.Context(), rs.Named(), 1); len(blocked) != 0 {
				t.Fatalf("initial render blocked: %v", blocked)
			}
			before := s.GetArtifact(rs.Named())
			fresh := *rs
			fresh.ResourceSetSpec = *rs.DeepCopy()
			switch state {
			case "remove", "filter remove":
				fresh.DependsOn = nil
			case "add", "filter add":
				fresh.DependsOn = []fluxopv1.Dependency{crd}
			case "failed gate":
				fresh.DependsOn = append(fresh.DependsOn, fluxopv1.Dependency{Kind: manifest.KindConfigMap, Name: "missing"})
			}
			if state == "remove" || state == "add" {
				fresh.ResourcesTemplate = strings.ReplaceAll(rs.ResourcesTemplate, "old", "new")
			}
			s.AddObject(&fresh)
			if strings.HasPrefix(state, "filter") {
				c.Close()
				opts.Filter = change.NewFilter(change.NewSet(nil), nil, t.TempDir(), s)
				c = resourcesetctrl.New(s, tasks, true)
				c.Configure(opts)
				c.Start(t.Context())
				t.Cleanup(c.Close)
			}
			if blocked := c.ReconcileNode(t.Context(), rs.Named(), 1); len(blocked) != 0 {
				t.Fatalf("final render blocked: %v", blocked)
			}
			info, _ := s.GetStatus(rs.Named())
			wantStatus := store.StatusReady
			if state == "failed gate" {
				wantStatus = store.StatusFailed
				if !strings.Contains(info.Message, "dependency not found") {
					t.Fatalf("primary error missing: %+v", info)
				}
			}
			assert.Equal(t, info.Status, wantStatus)
			if state == "dedup" || strings.HasPrefix(state, "filter") {
				if s.GetArtifact(rs.Named()) != before {
					t.Fatal("retained artifact changed")
				}
			}
			o.warnMissingCRDs()
			wantWarnings := 1
			if state == "remove" || state == "filter remove" || state == "failed gate" {
				wantWarnings = 0
			}
			assert.Equal(t, len(s.Warnings()), wantWarnings)
			if wantWarnings == 1 {
				assert.Equal(t, s.Warnings()[0].Count, 1)
			}
			raw := o.rsRawSink.commit()[parent]
			wantNames := []string{"old"}
			if state == "remove" || state == "add" {
				wantNames = append(wantNames, "new")
			}
			names := []string{}
			for _, doc := range raw {
				name, ns := manifest.DocMetadata(doc)
				names = append(names, name)
				assert.Equal(t, ns, "ns")
				assert.Equal(t, doc["spec"].(map[string]any)["value"].(string), name)
			}
			slices.Sort(names)
			slices.Sort(wantNames)
			assert.Diff(t, names, wantNames)
		})
	}
}

func TestMissingCRDs_SharedWarningsAndOutput(t *testing.T) {
	var first *Result
	for _, workers := range []int{2, 4} {
		cache := t.TempDir()
		dir := t.TempDir()
		if _, err := gogit.PlainInit(dir, false); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- sets.yaml\n")
			var sets strings.Builder
			for _, name := range []string{"z", "a"} {
				fmt.Fprintf(&sets, `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: flux-system}
spec:
 dependsOn:
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: z.example.com}
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: a.example.com}
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: a.example.com}
 resources:
 - apiVersion: example.com/v1
   kind: Widget
   metadata: {name: %s, namespace: flux-system}
   spec: {value: %s}
`, name, name, name)
			}
			testutil.WriteFile(t, dir, "apps/sets.yaml", sets.String())
			o, err := New(Config{Path: dir, RepoRoot: dir, CacheDir: cache, Concurrency: workers, AllowMissingCRDs: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(o.Stop)
			res, err := o.Render(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			want := []manifest.Warning{}
			for _, name := range []string{"a.example.com", "z.example.com"} {
				crd := manifest.NamedResource{Kind: manifest.KindCustomResourceDefinition, Name: name}
				want = append(want, manifest.Warning{Resource: crd, Category: manifest.WarnMissingCRD, Count: 1, Message: "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet/flux-system/a, ResourceSet/flux-system/z"})
			}
			assert.Diff(t, res.Warnings, want)
			for _, name := range []string{"a", "z"} {
				id := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: name}
				art, ok := o.store.GetArtifact(id).(*store.ResourceSetArtifact)
				if !ok || len(art.Manifests) != 1 {
					t.Fatalf("%s artifact=%+v", id, art)
				}
				raw, ok := o.store.GetObject(manifest.NamedResource{Kind: "Widget", Namespace: "flux-system", Name: name}).(*manifest.RawObject)
				if !ok {
					t.Fatalf("missing emitted Widget/%s", name)
				}
				assert.Diff(t, raw.Spec, map[string]any{"value": name})
			}
			if first == nil {
				first = res
			} else {
				assert.Diff(t, res.Manifests, first.Manifests)
				assert.Diff(t, res.Warnings, first.Warnings)
			}
		}
	}
}
