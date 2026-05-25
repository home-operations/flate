// Package image discovers container images inside rendered Kubernetes
// manifests by parsing string values, not by hard-coding a table of
// kinds and field names.
//
// Every string value reachable in a manifest tree is fed through
// distribution/reference — the canonical OCI image reference parser
// used by Docker, containerd, and Kubernetes. A string is recognised
// as an image when it parses as a named reference AND carries either
// a tag or a digest, which excludes plain hostnames, paths, and
// version-only strings like "v1.2.3".
//
// Because detection is purely value-based, new CRDs that embed image
// references are picked up automatically — no per-kind override list
// to maintain.
package image

import (
	"maps"
	"slices"
	"strings"

	"github.com/distribution/reference"
)

// Extract returns the unique sorted image references found anywhere
// inside doc. Non-string values are ignored. Returns nil when no
// images are found.
func Extract(doc map[string]any) []string {
	set := map[string]struct{}{}
	walk(doc, set)
	if len(set) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(set))
}

// walk recursively descends into v collecting strings that look like
// OCI image references.
func walk(v any, set map[string]struct{}) {
	switch tv := v.(type) {
	case map[string]any:
		for _, child := range tv {
			walk(child, set)
		}
	case []any:
		for _, item := range tv {
			walk(item, set)
		}
	case string:
		if IsImageRef(tv) {
			set[tv] = struct{}{}
		}
	}
}

// IsImageRef reports whether s looks like a tagged or digested OCI
// image reference. distribution/reference is permissive — many
// non-image strings ("count:up0", "system:auth-delegator",
// "localhost:11220") parse cleanly as `name:tag` — so we apply
// cheap heuristics first to weed out common false positives.
func IsImageRef(s string) bool {
	if len(s) < 5 {
		return false
	}
	if strings.Contains(s, "://") || strings.HasPrefix(s, "/") {
		return false // URLs / filesystem paths
	}
	if !strings.ContainsAny(s, ":@") {
		return false // images always carry a tag or digest
	}
	if strings.Contains(s, "${") {
		return false // unsubstituted postBuild placeholder
	}
	// Every real image reference contains a `/` somewhere — either
	// separating the registry from the repository (`ghcr.io/foo:v1`)
	// or separating the registry:port from the path
	// (`localhost:5000/foo:v1`). Bare-name references like
	// `nginx:latest` aren't seen in production GitOps manifests, and
	// requiring `/` rules out PromQL recording rules
	// ("apiserver_request:burnrate1d"), RBAC role bindings
	// ("system:auth-delegator"), and hostname-port pairs
	// ("localhost:11220") in one shot.
	if !strings.Contains(s, "/") {
		return false
	}
	ref, err := reference.ParseNormalizedNamed(s)
	if err != nil {
		return false
	}
	_, tagged := ref.(reference.Tagged)
	_, digested := ref.(reference.Digested)
	return tagged || digested
}
