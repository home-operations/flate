package loader

import (
	"context"
	"errors"
	"iter"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestReduceStatic_PublishesBorrowedConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := New(store.New())
	l.SourceRoot = root
	l.ensureScopes()
	owner := testutil.KustomizationID("apps", "owner")
	file := filepath.Join(root, "owner.yaml")
	child := filepath.Join(root, "child", "object.yaml")
	terminal := filepath.Join(root, "patch.yaml")
	l.Index.Records[owner] = manifest.DiscoveryRecord{Path: file}
	l.documents.cache[file] = document{objects: []parsedObject{{}}, rootKnown: true, rootAdmitted: true, terminal: true}
	l.documents.cache[child] = document{objects: []parsedObject{{}}}
	l.documents.cache[terminal] = document{objects: []parsedObject{{}}}
	graph := graphContext{root: root, dir: "child"}
	visits := []graphVisit{
		{role: kustomizeinput.Resource, file: child},
		{role: kustomizeinput.File, file: terminal},
	}
	l.scopes.walks[graph] = scopeTraversal{visits: visits}
	l.scopes.contributions = []scopeContribution{{owner: owner, context: graph, visits: visits, input: 1}}
	l.scopes.inputs = []scopeInput{{}, {}}
	var reads []string
	for dir, err := range selection.ReduceStatic(t.Context(), staticScope{loader: l}) {
		assert.NoError(t, err)
		reads = append(reads, dir)
		l.documents.cache[dir] = document{searched: true}
	}
	assert.Equal(t, 1, len(reads))
	assert.Equal(t, root, reads[0])
	assert.Equal(t, selection.RootEligible, l.documents.cache[file].eligible)
	assert.Equal(t, selection.RootEligible, l.documents.cache[child].eligible)
	assert.Equal(t, selection.RootInactive, l.documents.cache[terminal].eligible)
	assert.Equal(t, false, l.documents.cache[file].rootKnown)
	assert.Equal(t, false, l.documents.cache[file].rootAdmitted)
	assert.Equal(t, false, l.documents.cache[file].terminal)
	assert.Equal(t, true, l.scopes.walks[graph].controlled)
	assert.Equal(t, selection.RootEligible, l.scopes.walks[graph].reached)
	assert.Equal(t, true, l.scopes.contributions[0].active)
	assert.Equal(t, true, l.scopes.inputs[0].active)
	assert.Equal(t, true, l.scopes.inactive)
	assert.Equal(t, true, &l.scopes.walks[graph].visits[0] == &visits[0])
}

type cancellationStaticScope struct {
	staticScope

	cancel                    context.CancelFunc
	stage                     string
	resets, rounds, finalized int
}

func (s *cancellationStaticScope) ResetStatic() {
	s.resets++
	s.staticScope.ResetStatic()
}

func (s *cancellationStaticScope) Files() iter.Seq2[string, selection.StaticFile] {
	return func(yield func(string, selection.StaticFile) bool) {
		for file, doc := range s.staticScope.Files() {
			if !yield(file, doc) {
				return
			}
		}
		if s.stage == "before propagation" && s.cancel != nil {
			s.cancel()
			s.cancel = nil
		}
	}
}

func (s *cancellationStaticScope) SetActive(i int, active bool) {
	s.rounds++
	s.staticScope.SetActive(i, active)
	if s.stage == "propagation" && s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

func (s *cancellationStaticScope) SetInactive(inactive bool) {
	s.finalized++
	s.staticScope.SetInactive(inactive)
}

func TestReduceStatic_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		stage                           string
		failures, reads, resets, rounds int
		err                             error
		eligibility                     selection.RootEligibility
	}{
		{stage: "before reset", failures: 1, err: context.Canceled},
		{stage: "read", failures: 1, reads: 1, resets: 1, err: context.Canceled},
		{stage: "before propagation", failures: 1, reads: 1, resets: 1, err: context.Canceled, eligibility: selection.RootEligible},
		{stage: "propagation", failures: 1, reads: 1, resets: 1, rounds: 1, err: context.Canceled, eligibility: selection.RootIndependent},
		{stage: "early stop", reads: 1, resets: 1},
	} {
		t.Run(tt.stage, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			l := New(store.New())
			defer l.Close()
			l.SourceRoot = root
			l.ensureScopes()
			file := filepath.Join(root, "child.yaml")
			l.documents.cache[file] = document{objects: []parsedObject{{}}}
			l.scopes.contributions = []scopeContribution{
				{visits: []graphVisit{{role: kustomizeinput.Resource, file: file}}},
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			scope := &cancellationStaticScope{loader: l, cancel: cancel, stage: tt.stage}
			if tt.stage == "before reset" {
				cancel()
			}
			reads, failures := 0, 0
			for dir, err := range selection.ReduceStatic(ctx, scope) {
				if err != nil {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
					assert.Equal(t, dir, "")
					failures++
					continue
				}
				reads++
				assert.Equal(t, dir, root)
				if tt.stage == "early stop" {
					break
				}
				l.documents.cache[dir] = document{searched: true}
				if tt.stage == "read" {
					cancel()
				}
			}
			assert.Equal(t, failures, tt.failures)
			assert.Equal(t, reads, tt.reads)
			assert.Equal(t, ctx.Err(), tt.err)
			assert.Equal(t, scope.resets, tt.resets)
			assert.Equal(t, scope.rounds, tt.rounds)
			assert.Equal(t, l.documents.cache[file].eligible, tt.eligibility)
			assert.Equal(t, scope.finalized, 0)
		})
	}
}
