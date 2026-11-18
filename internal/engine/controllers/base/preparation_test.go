package base_test

import (
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_RendererPreparationTransfer(t *testing.T) {
	t.Parallel()
	t.Run("helm", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{Kind: manifest.KindGitRepository, FS: fstest.MapFS{
			"chart/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: chart\nversion: 0.1.0\n")},
			"chart/templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")},
		}})
		client := helm.NewClient(cachetest.New(t, "", cache.New), nil)
		owner := &manifest.HelmRelease{Name: "owner", Namespace: "apps", Chart: manifest.HelmChart{
			Name: "chart", Source: source.Named(),
		}}
		input := helm.RenderOptions{Sources: helm.NewSources(st, nil)}
		testRendererPreparationTransfer(
			t,
			owner,
			func(t *testing.T, p *helm.Preparation, mismatch bool) []manifest.DocOccurrence {
				t.Helper()
				request := input
				request.Preparation, request.SkipTests = p, mismatch
				docs, _, err := client.Template(t.Context(), owner, nil, request)
				assert.NoError(t, err)
				return docs
			},
			func(p *helm.Preparation) bool { return p.Matches(client, owner, &input) },
		)
	})
	t.Run("kustomize", func(t *testing.T) {
		t.Parallel()
		trees := kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, nil)
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
		input := kustomize.RenderRequest{Spec: owner, SubPath: ".", Source: &store.SourceArtifact{FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
			"cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")},
		}}}
		testRendererPreparationTransfer(
			t,
			owner,
			func(t *testing.T, p *kustomize.Preparation, mismatch bool) []manifest.DocOccurrence {
				t.Helper()
				request := input
				request.Preparation, request.StrictSubstitutions = p, mismatch
				docs, _, err := kustomize.Render(t.Context(), trees, request, cache.RetryConfig{})
				assert.NoError(t, err)
				return docs
			},
			func(p *kustomize.Preparation) bool { return p.Matches(trees, &input) },
		)
	})
}

func testRendererPreparationTransfer[T any](
	t *testing.T,
	owner manifest.BaseManifest,
	render func(*testing.T, *T, bool) []manifest.DocOccurrence,
	matches func(*T) bool,
) {
	t.Helper()
	for _, tc := range []struct {
		name, destination, previous, discovery string
		want, consumed                         bool
	}{
		{name: "destination", destination: "match", previous: "match", discovery: "match", want: true},
		{name: "predecessor", destination: "mismatch", previous: "match", discovery: "match", want: true},
		{name: "discovery", destination: "mismatch", previous: "mismatch", discovery: "match", want: true, consumed: true},
		{name: "mismatches", destination: "mismatch", previous: "mismatch", discovery: "mismatch"},
		{name: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			controller := base.New(store.New(), nil)
			var previous, next base.Execution
			previous.InitProbe(controller, owner)
			t.Cleanup(previous.Close)
			next.InitProbe(controller, owner)
			t.Cleanup(next.Close)
			var predecessor, destination base.Preparation[T]
			prior := previous.Prepare(&predecessor, nil, matches)
			current := next.Prepare(&destination, nil, matches)
			var discovery T
			var want []manifest.DocOccurrence
			for _, candidate := range []struct {
				value *T
				state string
			}{{prior, tc.previous}, {current, tc.destination}, {&discovery, tc.discovery}} {
				if candidate.state != "" {
					want = render(t, candidate.value, candidate.state == "mismatch")
				}
			}
			next.Take(schedule.Result{Assessment: &previous})
			got := next.Prepare(&destination, &discovery, matches)
			assert.Equal(t, got, current)
			assert.Equal(t, matches(got), tc.want)
			assert.Equal(t, matches(prior), false)
			assert.Equal(t, matches(&discovery), tc.discovery == "match" && !tc.consumed)
			previous.Retire()
			assert.Equal(t, matches(got), tc.want)
			if tc.want {
				assert.Equal(t, manifest.ValueEqual(render(t, got, false), want), true)
			}
			next.Retire()
			assert.Equal(t, matches(got), false)
		})
	}
	t.Run("aliases", func(t *testing.T) {
		t.Parallel()
		for _, mismatch := range []bool{false, true} {
			t.Run(map[bool]string{false: "match", true: "mismatch"}[mismatch], func(t *testing.T) {
				t.Parallel()
				var run base.Execution
				run.InitProbe(base.New(store.New(), nil), owner)
				defer run.Close()
				var holder base.Preparation[T]
				value := run.Prepare(&holder, nil, matches)
				render(t, value, mismatch)
				assert.Equal(t, run.Prepare(&holder, value, matches), value)
				assert.Equal(t, matches(value), !mismatch)
				run.Retire()
				assert.Equal(t, matches(value), false)
			})
		}
	})
	t.Run("wrong holder type", func(t *testing.T) {
		t.Parallel()
		var run base.Execution
		run.InitProbe(base.New(store.New(), nil), owner)
		defer run.Close()
		var wrong base.Preparation[int]
		prior := run.Prepare(&wrong, nil, func(*int) bool { return false })
		*prior = 1
		var discovery T
		render(t, &discovery, false)
		var destination base.Preparation[T]
		got := run.Prepare(&destination, &discovery, matches)
		assert.Equal(t, *prior, 0)
		assert.Equal(t, matches(&discovery), false)
		assert.Equal(t, matches(got), true)
		run.Retire()
		assert.Equal(t, matches(got), false)
	})
	t.Run("distinct owners", func(t *testing.T) {
		t.Parallel()
		var ready, finished sync.WaitGroup
		start := make(chan struct{})
		for _, namespace := range []string{"first", "second"} {
			var previous, next base.Execution
			controller := base.New(store.New(), nil)
			object := owner.WithNamespace(namespace)
			previous.InitProbe(controller, object)
			t.Cleanup(previous.Close)
			next.InitProbe(controller, object)
			t.Cleanup(next.Close)
			var predecessor, destination base.Preparation[T]
			prior := previous.Prepare(&predecessor, nil, matches)
			render(t, prior, false)
			ready.Add(1)
			finished.Go(func() {
				ready.Done()
				<-start
				next.Take(schedule.Result{Assessment: &previous})
				got := next.Prepare(&destination, nil, matches)
				assert.Equal(t, matches(prior), false)
				assert.Equal(t, matches(got), true)
				previous.Retire()
				assert.Equal(t, matches(got), true)
				next.Retire()
				assert.Equal(t, matches(got), false)
			})
		}
		ready.Wait()
		close(start)
		finished.Wait()
	})
}
