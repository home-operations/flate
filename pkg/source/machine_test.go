package source

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestNetwork_RegistryStoresOpenOnce(t *testing.T) {
	docker, helm := t.TempDir(), filepath.Join(t.TempDir(), "registry.json")
	t.Setenv("PATH", t.TempDir())
	t.Setenv("DOCKER_CONFIG", docker)
	t.Setenv("HELM_REGISTRY_CONFIG", helm)
	testutil.WriteFile(t, docker, "config.json", `{"auths":{"docker.invalid":{"username":"docker","password":"secret"}}}`)
	testutil.WriteFileAt(t, helm, `{"auths":{"helm.invalid":{"username":"helm","password":"secret"}}}`)
	n := new(Network)
	first, err := n.RegistryCredentials(t.Context(), "", "docker.invalid")
	if err != nil || first.Username != "docker" {
		t.Fatalf("Docker selection: %v", err)
	}
	testutil.WriteFile(t, docker, "config.json", "{")
	testutil.WriteFileAt(t, helm, "{")
	var workers sync.WaitGroup
	for _, host := range []string{"docker", "helm"} {
		workers.Go(func() {
			selected, err := n.RegistryCredentials(t.Context(), "", host+".invalid")
			if err != nil || selected.Username != host {
				t.Errorf("shared %s store: %v", host, err)
			}
		})
	}
	workers.Wait()
}
