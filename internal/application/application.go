// Package application screens content for possible entry applications.
package application

import (
	"bytes"
	"regexp"
)

// The screen MUST remain immutable for concurrent producer reads.
var producerKind = regexp.MustCompile(
	`(?:kind\s*:|["']kind["']\s*:)\s*["']?(?:(?:Kustomization|FluxInstance|HelmRelease|ResourceSet)(?:[^A-Za-z0-9_]|$)|\{\{|<<)`,
)

// Contains MUST retain producer kinds when templates construct their API groups.
func Contains(content []byte) bool {
	return bytes.Contains(content, []byte("kustomize.toolkit.fluxcd.io")) ||
		bytes.Contains(content, []byte("fluxcd.controlplane.io")) ||
		bytes.Contains(content, []byte("helm.toolkit.fluxcd.io")) || producerKind.Match(content)
}
