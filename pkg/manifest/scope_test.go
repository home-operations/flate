package manifest_test

import (
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	kustomize "github.com/home-operations/flate/internal/kustomizestate"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestKindInfo_NonScopeFactsDoNotResolveScope(t *testing.T) {
	t.Parallel()

	kustomize.BuildMutex.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		facts := manifest.KindOf(&manifest.RawObject{APIVersion: "non-scope.example/v1", Kind: "NeverResolved"})
		assert.ErrorIf(
			t,
			facts.Tracked || facts.Runnable || facts.Status || facts.Render || facts.Discovery,
			"%s",
			"raw object gained registry facts",
		)
	}()
	select {
	case <-done:
		kustomize.BuildMutex.Unlock()
	case <-time.After(5 * time.Second):
		kustomize.BuildMutex.Unlock()
		<-done
		t.Fatal("non-scope facts tried to resolve scope")
	}
}
