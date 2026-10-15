// Package kustomize coordinates access to krusty's process-global schema and plugins.
package kustomize

import "sync"

// BuildMutex MUST guard every krusty build and uncached scope lookup because
// krusty mutates process-global OpenAPI and plugin state.
var BuildMutex sync.Mutex
