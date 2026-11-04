package loader

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_EmissionParentByFile(t *testing.T) {
	t.Parallel()
	child := testutil.KustomizationID("flux-system", "child")
	parent := testutil.KustomizationID("flux-system", "parent")
	other := testutil.KustomizationID("flux-system", "other")
	for _, tc := range []emissionParentCase{
		{name: "unique owner", emitters: []manifest.NamedResource{parent}, wantParent: parent},
		{name: "bootstrap origin gains owner", emitters: []manifest.NamedResource{parent}, wantParent: parent, seeded: true},
		{name: "non structural record", emitters: []manifest.NamedResource{parent}, nonStructural: true},
		{name: "missing owner"},
		{name: "ambiguous owners", emitters: []manifest.NamedResource{other, parent}},
		{name: "self owner", emitters: []manifest.NamedResource{child}},
		{name: "self and another owner are ambiguous", emitters: []manifest.NamedResource{child, parent}},
		{name: "strict parent wins", emitters: []manifest.NamedResource{parent}, wantParent: other, strict: true},
		{name: "repeated owner", emitters: []manifest.NamedResource{parent}, wantParent: parent, repeated: true},
		{name: "graph added record", emitters: []manifest.NamedResource{parent}, wantParent: parent, graphAdded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runEmissionParentCase(t, new(tc), &emissionParentIDs{child: child, parent: parent, other: other})
		})
	}
}

func secretID(ns, name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindSecret, Namespace: ns, Name: name}
}

// The discovery-time producer scan rides the same self-produce walk: an in-repo
// ExternalSecret / SealedSecret under a KS spec.path is recorded as
// target-Secret → producer, with the SAME effective-namespace resolution the
// ConfigMap path uses — an enclosing `namespace:` wins over the file's own.
func TestFinalizeIndex_RecordsProducers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n  - ./sealed.yaml\n  - ./explicit.yaml\n  - ./obc.yaml\n",
	)
	// ExternalSecret, no namespace in file → inherits `secure`; explicit target.
	testutil.WriteFile(
		t,
		dir,
		"apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n",
	)
	// SealedSecret with spec.template.metadata.name.
	testutil.WriteFile(
		t,
		dir,
		"apps/sealed.yaml",
		"apiVersion: bitnami.com/v1alpha1\nkind: SealedSecret\nmetadata:\n  name: db\nspec:\n  template:\n    metadata:\n      name: db-secret\n",
	)
	// ExternalSecret carrying its own namespace — the transformer (secure)
	// overrides it, exactly as kustomize does.
	testutil.WriteFile(
		t,
		dir,
		"apps/explicit.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: own\n  namespace: ignored\nspec:\n  target:\n    name: own-values\n",
	)
	// ObjectBucketClaim: its provisioner materializes a Secret AND a ConfigMap
	// both named after the claim, in the (transformer-resolved) namespace.
	testutil.WriteFile(
		t,
		dir,
		"apps/obc.yaml",
		"apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata:\n  name: media-bucket\nspec:\n  bucketName: media\n  storageClassName: ceph-bucket\n",
	)

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	want := map[manifest.NamedResource]string{ // target → producer name
		secretID("secure", "app-values"): "app-creds",
		secretID("secure", "db-secret"):  "db",
		secretID("secure", "own-values"): "own",
		// The OBC produces BOTH a Secret and a ConfigMap named after the claim.
		secretID("secure", "media-bucket"):                                        "media-bucket",
		{Kind: manifest.KindConfigMap, Namespace: "secure", Name: "media-bucket"}: "media-bucket",
	}
	for target, prodName := range want {
		declarations := testutil.Declarations(s.Productions(target, idx))
		if len(declarations) != 1 {
			t.Errorf("Producer(%v) missing; want %q", target, prodName)
			continue
		}
		got := declarations[0]

		assert.ErrorIf(
			t,
			got.Name != prodName || got.Namespace != target.Namespace,
			"Producer(%v).Name = %q, want %q",
			target,
			got.Name,
			prodName,
		)
	}
}

func TestFinalizeIndex_ExplicitProducerTargetName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nnamePrefix: prod-\nresources:\n  - ./es.yaml\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n",
	)

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	idx := rendererFixture(t, loaderFixture(s, nil), dir)

	{
		declarations := testutil.Declarations(
			s.Productions(secretID("secure", "app-values"), idx),
		)
		assert.ErrorIf(t, len(
			declarations,
		) == 0, "%v", "producer not recorded under its raw target name")
	}

	{
		declarations := testutil.Declarations(
			s.Productions(secretID("secure", "prod-app-values"), idx),
		)
		assert.ErrorIf(t, len(
			declarations,
		) > 0, "%v", "explicit producer target was renamed without a nameReference")
	}
}

func TestFinalizeIndex_LookupReferenceData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./secret.sops.yaml\n  - ./plain.yaml\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/secret.sops.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: cluster-secrets\nstringData:\n  SECRET_DOMAIN: ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]\nsops:\n  mac: ENC[AES256_GCM,data:a,type:str]\n  version: 3.9.0\n",
	)
	testutil.WriteFile(t, dir, "apps/plain.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: plain-secret\nstringData:\n  TOKEN: hunter2\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	testutil.WriteFile(
		t,
		dir,
		"generated/secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: deferred-secret}\nstringData: {TOKEN: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
	)
	s.AddObject(
		&manifest.Kustomization{
			Name:            "generated",
			Namespace:       "flux-system",
			Path:            "./generated",
			TargetNamespace: "flux-system",
		},
	)
	testutil.WriteFile(
		t,
		dir,
		"generated/nested/kustomization.yaml",
		"namespace: nested\nresources: [secret.yaml, producer.yaml]\nconfigMapGenerator: [{name: generated-data, files: [payload=../../inputs/payload.yaml]}]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"generated/nested/secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: nested-secret}\nstringData: {TOKEN: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"generated/nested/producer.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: nested-producer}\nspec: {target: {name: nested-produced}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"inputs/payload.yaml",
		testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n"),
	)
	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	for _, file := range []string{"generated/nested/secret.yaml", "generated/nested/producer.yaml", "inputs/payload.yaml"} {
		assert.Diff(
			t,
			idx.Readers[file],
			[]manifest.FileReader{
				{Reader: testutil.KustomizationID("flux-system", "generated"), Source: manifest.BootstrapSourceID},
			},
		)
	}
	generated, _ := lookupDataForTest[*manifest.ConfigMap](
		t,
		s,
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "generated-data"},
	)
	assert.FatalIf(t, generated == nil ||
		generated.Data["payload"] != testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n") ||
		s.GetObject(testutil.KustomizationID("authored", "terminal")) != nil,

		"implicit generator data was lost or admitted as a resource: generated=%+v terminal=%+v",
		generated,
		s.GetObject(testutil.KustomizationID("authored", "terminal")))

	if obj := s.GetObject(secretID("flux-system", "deferred-secret")); obj != nil {
		t.Errorf("generated Secret materialized before rendering: %#v", obj)
	}
	for _, name := range []string{"nested-secret", "nested-produced"} {
		for _, namespace := range []string{"nested", "flux-system"} {
			if obj := s.GetObject(secretID(namespace, name)); obj != nil {
				t.Fatalf("implicit-boundary input materialized early: %v", obj)
			}
		}
	}

	got, _ := lookupDataForTest[*manifest.Secret](t, s, secretID("flux-system", "cluster-secrets"))
	assert.FatalIf(
		t,
		s.GetObject(secretID("flux-system", "cluster-secrets")) != nil,
		"%s",
		"SOPS lookup entered canonical inventory",
	)
	assert.FatalIf(t, got != nil, "%s", "unadmitted data supplied a second reference path")

	if obj := s.GetObject(secretID("flux-system", "plain-secret")); obj != nil {
		t.Errorf("a cleartext Secret must not be materialized; got %#v", obj)
	}
}

func TestFinalizeIndex_ProducerOwners(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"", "authored"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s := store.New()
			want := make([]manifest.NamedResource, 0, 2)
			for _, name := range []string{"b", "a"} {
				ks := &manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./" + name}
				s.AddObject(ks)
				want = append(want, ks.Named())
				testutil.WriteFile(
					t,
					root,
					name+"/kustomization.yaml",
					"namespace: effective\nresources: [../shared, missing.yaml]\n",
				)
			}
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [producer.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"shared/producer.yaml",
				"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: '"+namespace+"'}\nspec:\n  target: {name: values}\n  dataFrom: [{extract: {key: values}}]\n",
			)
			idx := rendererFixture(t, loaderFixture(s, nil), root)
			slices.SortFunc(want, manifest.NamedResource.Compare)
			for _, ns := range []string{namespace, "effective"} {
				id := manifest.NamedResource{
					Group:     "external-secrets.io",
					Kind:      "ExternalSecret",
					Namespace: ns,
					Name:      "producer",
				}
				got := declarationOwnersForTest(idx, id)
				slices.SortFunc(got, manifest.NamedResource.Compare)

				assert.FatalIf(t, !slices.Equal(got, want), "declaration %s owners = %v, want %v", id, got, want)

				got[0] = manifest.NamedResource{}
				fresh := declarationOwnersForTest(idx, id)
				slices.SortFunc(fresh, manifest.NamedResource.Compare)

				assert.FatalIf(t, !slices.Equal(fresh, want), "%v", "caller mutated index")
			}
			effective := manifest.NamedResource{
				Group:     "external-secrets.io",
				Kind:      "ExternalSecret",
				Namespace: "effective",
				Name:      "producer",
			}
			{
				r := idx.Records[effective]
				assert.FatalIf(t, r.Authored.Namespace != namespace || r.Authored.Name != "producer" ||
					r.File != "shared/producer.yaml", "producer lost authored file proof: %+v", r)
			}

			assert.FatalIf(t, len(
				declarationOwnersForTest(
					idx,
					manifest.NamedResource{
						Group:     "external-secrets.io",
						Kind:      "ExternalSecret",
						Namespace: "foreign",
						Name:      "producer",
					},
				),
			) != 0, "%v", "foreign namespace inherited declaration owners")

			{
				got := renderers(
					idx,
					manifest.NamedResource{Kind: "ConfigMap", Namespace: "effective", Name: "values"},
				)
				assert.FatalIf(t, len(
					got,
				) != 0, "producer ownership changed self-substitution admission: %v", got)
			}
		})
	}
}

func TestFinalizeIndex_ProducerOwnersMultipleTargets(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"", "apps"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s := store.New()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./values"}
			s.AddObject(ks)
			testutil.WriteFile(t, root, "values/kustomization.yaml", "namespace: apps\nresources: [bucket.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"values/bucket.yaml",
				"apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata: {name: bucket, namespace: '"+namespace+"'}\nspec: {generateBucketName: bucket, storageClassName: bucket}\n",
			)
			idx := rendererFixture(t, loaderFixture(s, nil), root)
			for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
				target := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "bucket"}
				{
					declarations := testutil.Declarations(s.Productions(target, idx))
					assert.FatalIf(t, len(declarations) == 0, "missing %s target", kind)
				}
			}
			for _, ns := range []string{namespace, "apps"} {
				producer := manifest.NamedResource{
					Group:     "objectbucket.io",
					Kind:      "ObjectBucketClaim",
					Namespace: ns,
					Name:      "bucket",
				}
				{
					got := declarationOwnersForTest(
						idx,
						producer,
					)
					assert.FatalIf(t, !slices.Equal(
						got,
						[]manifest.NamedResource{ks.Named()},
					), "repeated targets duplicate declaration owner: %v", got)
				}
			}
		})
	}
}

type emissionParentCase struct {
	name                         string
	emitters                     []manifest.NamedResource
	wantParent                   manifest.NamedResource
	strict, repeated, graphAdded bool
	nonStructural, seeded        bool
}

type emissionParentIDs struct {
	child, parent, other manifest.NamedResource
}

func runEmissionParentCase(t *testing.T, scenario *emissionParentCase, ids *emissionParentIDs) {
	t.Helper()
	tc := *scenario
	child, parent, other := ids.child, ids.parent, ids.other
	root := t.TempDir()
	file := "objects/ks.yaml"
	path := filepath.Join(root, file)
	body := testutil.KustomizationYAML("child", "flux-system", "  path: ./apps/${ENV}/child\n")
	id := child
	if tc.nonStructural {
		id = cmID("flux-system")
		body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings, namespace: flux-system}\n"
	}
	if tc.repeated {
		body += "---\n" + testutil.KustomizationYAML("peer", "flux-system", "  path: ./apps/${ENV}/peer\n")
	}
	testutil.WriteFile(t, root, file, body)
	testutil.WriteFile(t, root, "render/kustomization.yaml", "resources: [../objects]\n")
	testutil.WriteFile(t, root, "objects/kustomization.yaml", "resources: [ks.yaml]\n")
	if tc.strict {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	}
	newLoader := func() *Loader {
		t.Helper()
		return buildEmissionParentLoader(
			t,
			new(tc),
			&emissionParentInput{other: other, root: root, path: path, file: file, id: id},
		)
	}
	l := newLoader()
	_, exists := l.Index.Lookup(id)
	assert.Equal(t, exists, !tc.graphAdded)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assertEmissionParentRecord(
		t,
		new(tc),
		l,
		&emissionParentInput{other: other, root: root, path: path, file: file, id: id},
	)
	if tc.repeated {
		peer := testutil.KustomizationID("flux-system", "peer")
		assert.Equal(t, l.Index.Records[peer].Parent, parent)
	}
	wantOwners := tc.emitters
	if tc.strict {
		wantOwners = []manifest.NamedResource{other, parent}
	}
	result1 := l.Index.OwnersOfFile(file)
	owners, ancestors := result1.Owners, result1.Ancestors
	assert.Diff(t, owners, wantOwners)
	assert.Equal(t, len(ancestors), 0)
	snapshot := snapshotIndexForTest(l.Index)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		diff := cmp.Diff(snapshot.Records, l.Index.Records, cmp.Comparer(manifest.Equal))
		assert.ErrorIf(t, diff != "", "discovery records differ (-want +got):\n%s", diff)
	}
	assert.Diff(t, l.Index.Readers, snapshot.Readers)
	result2 := l.Index.OwnersOfFile(file)
	owners, ancestors = result2.Owners, result2.Ancestors
	assert.Diff(t, owners, wantOwners)
	assert.Equal(t, len(ancestors), 0)
	fresh := newLoader()
	{
		err := fresh.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assertIndexForTest(t, *fresh.Index, snapshot)
}

type emissionParentInput struct {
	other            manifest.NamedResource
	root, path, file string
	id               manifest.NamedResource
}

func buildEmissionParentLoader(t *testing.T, scenario *emissionParentCase, fixture *emissionParentInput) *Loader {
	t.Helper()
	tc := *scenario
	st := store.New()
	for _, owner := range tc.emitters {
		st.AddObject(
			&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./render"},
		)
	}
	if tc.strict {
		st.AddObject(&manifest.Kustomization{Name: fixture.other.Name, Namespace: fixture.other.Namespace, Path: "."})
	}
	l := New(st)
	admitForTest(st, fixture.root)
	l.mountSources()
	objects, err := l.documents.parseFile(t.Context(), fixture.path, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	for _, child := range objects {
		obj := child.Obj
		if _, ok := obj.(*manifest.Kustomization); ok && st.GetObject(obj.Named()) == nil {
			st.AddObject(obj, child.Doc)
		}
	}
	if !tc.graphAdded {
		l.Index.Records[fixture.id] = manifest.DiscoveryRecord{
			Authored: fixture.id,
			Path:     fixture.path,
			File:     fixture.file,
		}
		if tc.seeded {
			l.Index.Records[fixture.id] = manifest.DiscoveryRecord{
				Authored: fixture.id,
				Path:     fixture.path,
				File:     fixture.file,
				Productions: []manifest.Production{
					{Authored: fixture.id, Path: fixture.path, File: fixture.file, Source: manifest.BootstrapSourceID},
				},
			}
		}
	}
	return l
}

func assertEmissionParentRecord(t *testing.T, scenario *emissionParentCase, l *Loader, fixture *emissionParentInput) {
	t.Helper()
	tc := *scenario
	declaration := manifest.BaseManifest(
		&manifest.Kustomization{Name: "child", Namespace: "flux-system", Path: "./apps/${ENV}/child"},
	)
	if tc.nonStructural {
		declaration = &manifest.ConfigMap{Name: "cluster-settings", Namespace: "flux-system"}
	}
	want := manifest.DiscoveryRecord{
		Authored:     fixture.id,
		Path:         fixture.path,
		File:         fixture.file,
		Parent:       tc.wantParent,
		Declaration:  declaration,
		StaticOutput: !tc.nonStructural,
	}
	for _, owner := range tc.emitters {
		production := manifest.Production{
			Owner:    owner,
			Authored: fixture.id,
			File:     fixture.file,
			Source:   manifest.BootstrapSourceID,
		}
		if !fixture.id.KindInfo().Runnable || l.Store.GetObject(fixture.id) != nil {
			production.Path = fixture.path
		}
		want.Productions = append(want.Productions, production)
	}
	if len(want.Productions) == 0 {
		want.Productions = []manifest.Production{
			{Authored: fixture.id, Path: fixture.path, File: fixture.file, Source: manifest.BootstrapSourceID},
		}
	}
	{
		diff := cmp.Diff(want, l.Index.Records[fixture.id], cmp.Comparer(manifest.Equal))
		assert.ErrorIf(t, diff != "", "discovery record differs (-want +got):\n%s", diff)
	}
}
