package helm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// locateOCIChart resolves a chart whose source is an OCIRepository.
//
// Preferred path: the source.oci.Fetcher has already pulled the
// artifact (applying spec.verify cosign verification, spec.layerSelector,
// spec.certSecretRef, spec.proxySecretRef, spec.insecure, spec.ignore,
// semver tag resolution) into a slot under the shared source cache —
// the HR depwait blocks render until that source is Ready, so by the
// time this runs the artifact is on disk. Reading it from the store
// keeps every Flux OCIRepository feature working uniformly for both
// Kustomization and HelmRelease consumers.
//
// Fallback path: when no SourceArtifact is present (typically
// --enable-oci=false, which wires source.ExistenceFetcher for
// OCIRepository so HR depwait still unblocks but no artifact is
// written), pull via helm's registry client. This preserves the
// pre-unification behavior for embedders that don't wire the OCI
// fetcher — but in that mode none of the OCIRepository spec.*
// features apply, exactly as before.
func (c *Client) locateOCIChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	r := c.resolveOCIRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: OCIRepository %s not registered", manifest.ErrObjectNotFound, hr.Chart.RepoFullName())
	}
	if art := c.resolveLocalSource(hr); art != nil && art.LocalPath != "" {
		path, err := ociChartPathFromArtifact(art.LocalPath)
		if err != nil {
			return "", fmt.Errorf("OCIRepository %s/%s: %w", r.Namespace, r.Name, err)
		}
		return path, nil
	}
	// Fallback path. The source.oci.Fetcher didn't produce an
	// artifact for this OCIRepository — typically because the
	// orchestrator is configured with EnableOCI=false (which wires
	// source.ExistenceFetcher) or because the HR's source controller
	// was never wired in this embedding. NONE of the OCIRepository
	// spec.* features (verify / layerSelector / certSecretRef /
	// proxySecretRef / insecure / ignore) apply on this path.
	if r.Reference != nil && r.Reference.SemVer != "" {
		// Semver resolution requires listing tags from the registry —
		// that's part of source.oci.Fetcher, not helm's registry
		// client. The helm-side fall-back would just pass the semver
		// constraint to Pull and get a cryptic "invalid tag" error.
		return "", fmt.Errorf(
			"OCIRepository %s/%s uses spec.ref.semver but the source.oci.Fetcher is not active "+
				"(likely --enable-oci=false); semver resolution requires the OCI fetcher",
			r.Namespace, r.Name)
	}
	ver := r.Version()
	// Operator-visible: the fallback path silently drops every
	// security-relevant spec.* field (verify, layerSelector,
	// certSecretRef, proxySecretRef, insecure, ignore). Bootstrap-
	// time warnOnDisabledOCIFeatures already calls this out per
	// CR; the per-lookup log surfaces the actual moment the
	// fallback runs — useful both for diagnosing missing-feature
	// surprises and for noticing when the fallback is hit
	// unexpectedly. Upgraded from Debug to Warn so operators on
	// default log level (info) see it.
	slog.Warn("helm: OCIRepository SourceArtifact missing; falling back to helm registry client — spec.verify/layerSelector/etc. NOT applied on this path",
		"ociRepository", r.Namespace+"/"+r.Name, "url", r.URL, "version", ver)
	return c.fetchOCIChart(ctx, r.URL, ver)
}

// ociChartPathFromArtifact picks the right chart path under an
// OCIRepository SourceArtifact's slot. The source/oci fetcher's
// applyLayerSelector produces one of three observable layouts:
//
//  1. Chart.yaml at slot root — the rare shape where a chart-as-OCI
//     artifact is published WITHOUT helm's standard `<chartname>/`
//     wrapper directory. Slot itself is the chart root.
//  2. layer.tar.gz at slot root — operation=copy on the OCIRepository's
//     layerSelector. Slot contains the packaged chart tgz; helm's
//     loader.Load handles it via FileLoader.
//  3. <slot>/<chartname>/Chart.yaml — the common shape: `helm package`
//     emits tarballs with a single top-level directory named after
//     the chart, and operation=extract (Flux's default) preserves
//     that layout when unpacking. The chart name in the dir comes
//     from the artifact, NOT hr.Chart.Name (those can differ), so we
//     scan for the single subdir that contains a Chart.yaml.
//
// Probing the filesystem keeps this hr.Chart.Name-independent and
// works uniformly across vendor packaging styles.
func ociChartPathFromArtifact(slot string) (string, error) {
	if _, err := os.Stat(filepath.Join(slot, chartYamlFilename)); err == nil {
		return slot, nil
	}
	tgz := filepath.Join(slot, copiedOCILayerFilename)
	if _, err := os.Stat(tgz); err == nil {
		return tgz, nil
	}
	switch sub, status := findChartSubdir(slot); status {
	case chartSubdirFound:
		return sub, nil
	case chartSubdirAmbiguous:
		// More than one Chart.yaml-bearing subdir — distinct failure
		// from "no chart found", and the right hint is "this is a
		// bundle-of-charts artifact, not a single chart".
		return "", fmt.Errorf("OCIRepository artifact at %s contains multiple Chart.yaml-bearing subdirs; "+
			"flate cannot disambiguate a bundle-of-charts artifact", slot)
	}
	return "", fmt.Errorf("OCIRepository artifact at %s has neither %s, %s, nor a <name>/Chart.yaml subdir — "+
		"chart layer missing or layerSelector misconfigured",
		slot, chartYamlFilename, copiedOCILayerFilename)
}

// chartSubdirStatus is the typed result of findChartSubdir. The
// caller branches between "not found" and "ambiguous" to surface
// distinct error messages — the operator hint is different.
type chartSubdirStatus int

const (
	chartSubdirNotFound chartSubdirStatus = iota
	chartSubdirFound
	chartSubdirAmbiguous
)

// findChartSubdir scans the immediate children of slot for one that
// contains a Chart.yaml — the shape produced by `helm package` when
// extracted via operation=extract. Hidden entries (anything starting
// with `.`) are skipped: this safely covers the .flate-* sentinels and
// any incidental dotfiles. Valid charts never use a dot-prefixed
// top-level directory.
//
// Returns ("", chartSubdirNotFound) when no subdir matches and
// ("", chartSubdirAmbiguous) when multiple match, so the caller can
// emit a specific error for each.
func findChartSubdir(slot string) (string, chartSubdirStatus) {
	entries, err := os.ReadDir(slot)
	if err != nil {
		return "", chartSubdirNotFound
	}
	var match string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if _, err := os.Stat(filepath.Join(slot, e.Name(), chartYamlFilename)); err != nil {
			continue
		}
		if match != "" {
			return "", chartSubdirAmbiguous
		}
		match = filepath.Join(slot, e.Name())
	}
	if match == "" {
		return "", chartSubdirNotFound
	}
	return match, chartSubdirFound
}

// chartYamlFilename / copiedOCILayerFilename mirror, by string value,
// the on-disk names produced by source/oci.applyLayerSelector. Kept
// as constants here (and not imported across packages) to avoid a
// pkg/helm → pkg/source/oci dependency for two static strings.
const (
	chartYamlFilename      = "Chart.yaml"
	copiedOCILayerFilename = "layer.tar.gz"
)

// ociPullRef joins an OCI repo URL and an optional ref into the form
// the helm registry client expects. A digest ref (`sha256:<hex>` and
// friends) joins with `@`; a tag joins with `:`. Per OCI tag spec a
// tag can never contain `:`, so its presence in `version` is an
// unambiguous digest signal — without this branch, the helm client
// rejects `repo:sha256:<hex>` as an invalid tag.
func ociPullRef(ref, version string) string {
	if version == "" {
		return ref
	}
	sep := ":"
	if strings.Contains(version, ":") {
		sep = "@"
	}
	return ref + sep + version
}

// fetchOCIChart pulls an OCI chart via the helm registry client.
// Used only as the EnableOCI=false fallback path of locateOCIChart;
// when EnableOCI=true the source.oci.Fetcher's slot is consumed
// directly via ociChartPathFromArtifact.
func (c *Client) fetchOCIChart(ctx context.Context, ref, version string) (string, error) {
	if c.registry == nil {
		return "", errors.New("helm registry client not initialized")
	}
	target := filepath.Join(c.cacheDir, safeName(filepath.Base(ref))+"-"+version+".tgz")

	release, err := chartCacheLocks.Acquire(ctx, target)
	if err != nil {
		return "", err
	}
	defer release()

	if _, err := os.Stat(target); err == nil {
		return target, nil
	}

	pullRef := ociPullRef(ref, version)
	_ = ctx // reserved for future per-pull cancellation when helm supports it
	result, err := c.registry.Pull(pullRef)
	if err != nil {
		return "", fmt.Errorf("oci pull %s: %w", pullRef, err)
	}
	if result == nil || result.Chart == nil {
		return "", fmt.Errorf("oci pull %s: empty result", pullRef)
	}
	if err := writeAtomic(target, result.Chart.Data); err != nil {
		return "", err
	}
	return target, nil
}

// safeName sanitizes an OCI ref's base name into a filesystem-safe
// token for the on-disk cache target.
func safeName(s string) string {
	out := strings.Builder{}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-' || r == '_' || r == '.':
			out.WriteRune(r)
		default:
			out.WriteRune('-')
		}
	}
	return out.String()
}
