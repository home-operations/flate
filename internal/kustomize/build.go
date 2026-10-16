// Package kustomize coordinates access to krusty's process-global schema and plugins.
package kustomize

import (
	"fmt"
	"sync"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/resmap"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
)

// BuildMutex MUST guard every krusty build and uncached scope lookup because
// krusty mutates process-global OpenAPI and plugin state. The registered scope
// owner resolves cold keys here before publication; reference walks only read its memo.
var BuildMutex sync.Mutex

// Build renders with Flux's options while retaining builtin schema definitions.
// Callers MUST hold BuildMutex for the complete call.
func Build(fs filesys.FileSystem, dir string) (res resmap.ResMap, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from kustomize build panic: %v", r)
			openapi.ResetOpenAPI()
		}
		if openapi.GetSchemaVersion() != openapi.DefaultOpenAPI {
			openapi.ResetOpenAPI()
		}
	}()

	openapi.Schema()
	return krusty.MakeKustomizer(&krusty.Options{
		LoadRestrictions: kustypes.LoadRestrictionsNone,
		PluginConfig:     kustypes.DisabledPluginConfig(),
	}).Run(fs, dir)
}
