package store

import (
	"github.com/home-operations/flate/pkg/manifest"
)

// SetWork borrows invocation-owned recording storage for a temporary proof.
func (r *ReadSet) SetWork(work *manifest.WorkRecorder) { r.work = work }

func (s *Store) admissionWorkSize() int {
	if s.workScope == (manifest.WorkScope{}) {
		return 0
	}
	count := 0
	for _, sh := range s.shards {
		for _, cell := range sh.cells {
			if cell.generation != 0 {
				count++
			}
		}
	}
	return count
}

func (s *Store) appendAdmissionWork(summary manifest.WorkSummary) manifest.WorkSummary {
	if s.workScope == (manifest.WorkScope{}) {
		return summary
	}
	for _, sh := range s.shards {
		for id, cell := range sh.cells {
			if cell.generation == 0 {
				continue
			}
			source := manifest.BootstrapSourceID
			if cell.binding != nil {
				source = cell.binding.Ref.NamedResource
			}
			summary = append(summary, manifest.WorkRecord{
				Key:    manifest.WorkKey{Tree: s.workTree, Owner: id, Source: source, Phase: manifest.WorkAdmission},
				Counts: manifest.WorkCounts{Attempts: 1, Completed: 1},
			})
		}
	}
	return summary
}

// WorkScope returns the invocation's tree preparation attribution.
func (s *Store) WorkScope() manifest.WorkScope { return s.workScope }

// Work returns this operation's accounting, including a borrowed proof's parent.
func (r *ReadSet) Work() *manifest.WorkRecorder {
	if r == nil {
		return nil
	}
	return r.work
}

// WorkScope returns the operation's attribution or its store's discovery scope.
func (r *ReadSet) WorkScope() manifest.WorkScope {
	if work := r.Work(); work != nil {
		return work.Scope()
	}
	return r.store.WorkScope()
}

// SetWorkTree qualifies subsequent execution accounting before admission begins.
func (s *Store) SetWorkTree(tree manifest.TreeID, scope manifest.WorkScope) {
	s.workTree, s.workScope = tree, scope
}

// WorkSnapshot detaches this run's sorted admission and operation counters.
// All writers MUST join before the snapshot is constructed.
func (s *Store) WorkSnapshot() manifest.WorkSummary {
	return s.appendAdmissionWork(s.workScope.Snapshot(s.admissionWorkSize())).Normalize()
}

const sourceReferenceUse = "source"

// BeginExecution borrows optional run-owned counters for an execution.
func (r *ReadSet) BeginExecution(obj manifest.BaseManifest) {
	if r.store.workScope == (manifest.WorkScope{}) {
		return
	}
	if obj == nil {
		obj = r.GetObject(r.id)
	}
	if obj == nil {
		return
	}
	key := manifest.WorkKey{Tree: r.store.workTree, Owner: r.id, Source: manifest.BootstrapSourceID}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == sourceReferenceUse || ref.Use == "chart" {
			key.Source = ref.NamedResource
			break
		}
	}
	if manifest.KindOf(obj).Group == manifest.SourceDomain {
		key.Source = r.id
	}
	r.work = r.store.workScope.Recorder(&key)
}
