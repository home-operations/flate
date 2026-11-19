package helmchart

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetchDependency_IndexedAdmission(t *testing.T) {
	t.Parallel()
	repository := &manifest.HelmRepository{
		Name: "declared", Namespace: "charts",
		URL: "oci://registry.example/charts/", SecretRef: &manifest.LocalObjectReference{Name: "credentials"},
	}
	other := *repository
	other.Namespace = "elsewhere"
	other.Name = "unrelated"
	st := store.New()
	st.AddObject(&other)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{repository.Named(): {Declaration: repository}},
	}
	stub := new(stubOCI)
	fetcher := &Fetcher{oci: stub}
	dep := &chart.Dependency{Name: "child", Version: "1.0.0", Repository: "oci://registry.example/charts"}
	var first store.ReadSet
	first.Init(st, manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "release"}, nil)
	defer first.Close()
	prepared, err := fetcher.FetchDependency(t.Context(), "charts", dep, index, &first, &cache.Policy{}, false)
	assert.NoError(t, err)
	assert.Diff(
		t,
		prepared.Inputs,
		[]manifest.DependencyRef{{NamedResource: repository.Named(), RequiresDeclaration: true}},
	)
	assert.Equal(t, stub.got == nil, true)
	st.AddObject(repository)
	assert.Equal(t, first.Valid(), false)
	var admitted store.ReadSet
	admitted.Init(st, first.ID(), nil)
	defer admitted.Close()
	prepared, err = fetcher.FetchDependency(t.Context(), "charts", dep, index, &admitted, &cache.Policy{}, false)
	assert.NoError(t, err)
	assert.Equal(t, len(prepared.Inputs), 0)
	assert.Equal(t, stub.got.Namespace, "charts")
	assert.Equal(t, stub.got.SecretRef.Name, "credentials")
	assert.Equal(t, stub.got.URL, "oci://registry.example/charts/child")
	assert.Equal(t, stub.got.Reference.Tag, "1.0.0")
	assert.Equal(t, stub.reads == &admitted, true)
	stub.got = nil
	probe, err := fetcher.FetchDependency(t.Context(), "charts", dep, index, &admitted, &cache.Policy{}, true)
	assert.NoError(t, err)
	assert.Equal(t, probe.Fetch == nil && probe.Close == nil, true)
	assert.Equal(t, stub.got == nil, true)
}

func TestFetchDependency_AdmissionCancellation(t *testing.T) {
	t.Parallel()
	archive := buildChartTarGz(t, "child", "1.0.0")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/index.yaml" {
			_, _ = io.WriteString(
				w,
				"apiVersion: v1\nentries:\n  child:\n  - name: child\n    version: 1.0.0\n    urls: [child.tgz]\n",
			)
			return
		}
		_, _ = w.Write(archive)
	}))
	t.Cleanup(server.Close)
	repository := httpRepo(server.URL)
	synctest.Test(t, func(t *testing.T) {
		fetcher, _ := newHTTPFetcher(t, repository)
		st := store.New()
		st.AddObject(repository)
		var reads store.ReadSet
		reads.Init(st, manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "release"}, nil)
		defer reads.Close()
		prepared, err := fetcher.FetchDependency(
			t.Context(),
			repository.Namespace,
			&chart.Dependency{Name: "child", Version: "1.0.0", Repository: server.URL},
			nil,
			&reads,
			&cache.Policy{},
			false,
		)
		assert.NoError(t, err)
		if prepared.Close != nil {
			defer prepared.Close()
		}
		fetcher.indexDecodes <- struct{}{}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, fetchErr := prepared.Fetch(ctx, nil)
			done <- fetchErr
		}()
		synctest.Wait()
		cancel()
		err = <-done
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, requests.Load(), int32(0))
		assert.Equal(t, len(fetcher.indexDecodes), 1)
		<-fetcher.indexDecodes
		artifact, err := prepared.Fetch(t.Context(), nil)
		assert.NoError(t, err)
		assert.Equal(t, artifact != nil, true)
		assert.Equal(t, requests.Load(), int32(2))
	})
}

func TestFetchDependency_HTTPAdmissionAndSnapshotRotation(t *testing.T) {
	t.Parallel()
	archive := buildChartTarGz(t, "child", "1.0.0")
	var requests atomic.Int32
	var password atomic.Value
	password.Store("first")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		username, secret, ok := r.BasicAuth()
		if !ok || username != "user" || secret != password.Load().(string) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/index.yaml" {
			_, _ = io.WriteString(
				w,
				"apiVersion: v1\nentries:\n  child:\n  - name: child\n    version: 1.0.0\n    urls: [child.tgz]\n",
			)
			return
		}
		_, _ = w.Write(archive)
	}))
	t.Cleanup(server.Close)
	repository := httpRepo(server.URL)
	repository.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	fetcher, _ := newHTTPFetcher(t, repository)
	st := store.New()
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{repository.Named(): {Declaration: repository}},
	}
	dependency := &chart.Dependency{Name: "child", Version: "1.0.0", Repository: server.URL}
	owner := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Namespace: "apps",
		Name:      "release",
	}
	var pending store.ReadSet
	pending.Init(st, owner, nil)
	defer pending.Close()
	prepared, err := fetcher.FetchDependency(
		t.Context(),
		repository.Namespace,
		dependency,
		index,
		&pending,
		&cache.Policy{},
		false,
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		prepared.Inputs,
		[]manifest.DependencyRef{{NamedResource: repository.Named(), RequiresDeclaration: true}},
	)
	assert.Equal(t, prepared.Fetch == nil, true)
	assert.Equal(t, requests.Load(), int32(0))
	st.AddObject(repository)
	assert.Equal(t, pending.Valid(), false)
	secret := &manifest.Secret{
		Name:       "auth",
		Namespace:  repository.Namespace,
		StringData: map[string]any{"username": "user", "password": "first"},
	}
	st.AddObject(secret)
	var previous string
	for _, wave := range []string{"first", "second"} {
		var reads store.ReadSet
		reads.Init(st, owner, nil)
		t.Cleanup(reads.Close)
		password.Store(wave)
		prepared, prepareErr := fetcher.FetchDependency(
			t.Context(),
			repository.Namespace,
			dependency,
			index,
			&reads,
			&cache.Policy{},
			false,
		)
		assert.NoError(t, prepareErr)
		if wave == "first" {
			rotated := *secret
			rotated.StringData = map[string]any{"username": "user", "password": "second"}
			st.AddObject(&rotated)
			assert.Equal(t, reads.Valid(), false)
		}
		artifact, fetchErr := prepared.Fetch(t.Context(), nil)
		assert.NoError(t, fetchErr)
		if prepared.Close != nil {
			prepared.Close()
		}
		assert.FatalIf(
			t,
			artifact == nil || artifact.Key == previous,
			"dependency reused a prior credential snapshot: %+v",
			artifact,
		)
		previous = artifact.Key
		assert.Equal(t, artifact.Kind, manifest.KindHelmChart)
	}
	assert.Equal(t, requests.Load(), int32(4))
	stWithoutSecret := store.New()
	stWithoutSecret.AddObject(repository)
	var missing store.ReadSet
	missing.Init(stWithoutSecret, owner, nil)
	defer missing.Close()
	_, err = fetcher.FetchDependency(
		t.Context(),
		repository.Namespace,
		dependency,
		index,
		&missing,
		&cache.Policy{},
		false,
	)
	_, absent := errors.AsType[*source.MissingSecretError](err)
	assert.Equal(t, absent, true)
	assert.Equal(t, requests.Load(), int32(4))
}

func TestFetchDependency_StableWinner(t *testing.T) {
	t.Parallel()
	st := store.New()
	for _, name := range []string{"z", "a"} {
		st.AddObject(&manifest.HelmRepository{
			Name: name, Namespace: "apps",
			URL: "oci://registry.example/charts", SecretRef: &manifest.LocalObjectReference{Name: name},
		})
	}
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "release"}, nil)
	defer reads.Close()
	stub := new(stubOCI)
	f := &Fetcher{oci: stub}
	_, err := f.FetchDependency(
		t.Context(),
		"apps",
		&chart.Dependency{Name: "child", Repository: "oci://registry.example/charts/"},
		nil,
		&reads,
		&cache.Policy{},
		false,
	)
	assert.NoError(t, err)
	assert.Equal(t, stub.got.SecretRef.Name, "a")
}
