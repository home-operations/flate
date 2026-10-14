package helm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// chartFingerprint returns the hex-encoded sha256 of every input
// loader.Load consumes for ch (Metadata + Templates + Files + Schema
// + chart defaults + every subchart's same). Stable across Go runs
// because encoding/json sorts map[string]V keys for us. Empty chart
// pointer returns a stable sentinel digest distinct from any
// well-formed chart.
//
// Memoized by Client.LoadChart at chart-cache fill time so repeat
// Template calls against the same path never recompute it; the
// fingerprint then participates in the template-output key built
// by computeTemplateKey.
func chartFingerprint(ch *chart.Chart) string {
	h := sha256.New()
	if !writeChartFingerprint(h, ch) {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

// computeTemplateKey returns a hex-encoded sha256 of every input
// that affects Template's output for a given (chart, values, opts,
// hr) tuple. A change in any input yields a distinct key, so a
// stale entry never serves a different render. Unmarshalable values return
// an empty key; callers MUST skip cache reads and writes for that render.
//
// loaded.Fingerprint is computed by LoadChart; when empty, the chart
// content is fingerprinted here. Cache owns persistent build identity isolation.
//
// Inputs folded into the key:
//   - chart fingerprint (Metadata + Templates + Values + Schema + Files —
//     loader.Load reads exactly these from disk; their content captures
//     the chart's identity even when a mutable OCI tag is re-pushed
//     under the same name+version)
//   - chart Dependencies' fingerprints (subcharts contribute Templates
//     and Values to the render)
//   - finalValues, marshaled stably via encoding/json (which already
//     sorts map[string]V keys alphabetically — no extra normalization
//     needed)
//   - render-affecting Options fields (KubeVersion, APIVersions,
//     SkipCRDs, etc.) — fields like ShowOnly that filter the post-
//     render text without changing the upstream render are
//     intentionally folded in too: the cached value IS the post-
//     filter text, so a different filter means a different output
//   - HR fields that affect the helm action (ReleaseName,
//     ReleaseNamespace, CRDsPolicy, DisableSchemaValidation,
//     DisableOpenAPIValidation, Install.DisableHooks,
//     Install.Replace, Upgrade.DisableHooks, Test.Enable,
//     PostRenderers) — Template reads these straight off hr without
//     funneling them through opts, so they have to land in the key
//     independently. PostRenderers in particular DOES change the
//     rendered bytes (kustomize patches/images run inside Template),
//     so it stays.
//
// Deliberately EXCLUDED — the cache boundary:
//   - CommonMetadata. The cached value is the pre-stamp render
//     (releaseManifest); spec.commonMetadata is applied strictly
//     downstream of TemplateDocs by the controller
//     (helm.ApplyHRCommonMetadata at controllers/helmrelease/
//     controller.go), on both the fresh-render and the
//     fingerprint-dedup replay paths. It never reaches Template's
//     output (neither newInstallAction nor newPostRenderer reads it),
//     so folding it in would only force spurious misses for HRs that
//     set it. Two renders that differ only in CommonMetadata share one
//     cache entry whose bytes are correct for both; each caller stamps
//     its own metadata afterward.
//
// Cost of computing the key: dominated by JSON-marshaling
// finalValues when loaded.Fingerprint is precomputed. A 50 KB values map
// marshals in ~50 µs — cheap vs. the 5-50 ms render the cache hit
// avoids.
func computeTemplateKey(
	loaded ChartLoadResult,
	finalValues map[string]any,
	opts Options,
	hr *manifest.HelmRelease,
) string {
	h := sha256.New()

	// Chart fingerprint. Precomputed at LoadChart time when the
	// chart is loaded; fall back to a fresh walk when
	// callers pass an empty fingerprint (tests, direct compute calls).
	if loaded.Fingerprint == "" {
		if !writeChartFingerprint(h, loaded.Chart) {
			return ""
		}
	} else {
		_, _ = h.Write([]byte("chart-fp:"))
		_, _ = h.Write([]byte(loaded.Fingerprint))
		_, _ = h.Write([]byte{0})
	}

	// Options that affect rendering. Marshal a tagged anonymous
	// struct so each field is delimited (no risk of "kube=1.30" +
	// "api=v1" colliding with "kube=1.30api=v1"). An anonymous
	// literal sidesteps staticcheck's S1016 hint about converting
	// from Options (which has no JSON tags) — and the struct field
	// order is the source order so writes are deterministic without
	// an explicit sort. Indirected through writeOptionsBlob so the
	// allocation lives in a single helper rather than ballooning the
	// keyer.
	writeOptionsBlob(h, opts)

	// HR-level fields that drive action.Install but bypass opts. A
	// dedicated tagged blob — same rationale as writeOptionsBlob above.
	type keyHR struct {
		ReleaseName              string                `json:"release_name"`
		ReleaseNamespace         string                `json:"release_namespace"`
		CRDsPolicy               string                `json:"crds_policy,omitempty"`
		DisableSchemaValidation  bool                  `json:"disable_schema_validation,omitempty"`
		DisableOpenAPIValidation bool                  `json:"disable_openapi_validation,omitempty"`
		InstallDisableHooks      bool                  `json:"install_disable_hooks,omitempty"`
		InstallReplace           bool                  `json:"install_replace,omitempty"`
		UpgradeDisableHooks      bool                  `json:"upgrade_disable_hooks,omitempty"`
		TestEnable               bool                  `json:"test_enable,omitempty"`
		ChartVersion             string                `json:"chart_version,omitempty"`
		ChartValuesFiles         []string              `json:"chart_values_files,omitempty"`
		IgnoreMissingValuesFiles bool                  `json:"ignore_missing_values_files,omitempty"`
		PostRenderers            []helmv2.PostRenderer `json:"post_renderers,omitempty"`
	}
	khr := keyHR{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		CRDsPolicy:               hr.CRDsPolicy,
		DisableSchemaValidation:  schemaValidationSkipped(opts, hr, nil),
		DisableOpenAPIValidation: hr.DisableOpenAPIValidation,
		ChartVersion:             hr.Chart.Version,
		ChartValuesFiles:         hr.ChartValuesFiles,
		IgnoreMissingValuesFiles: hr.IgnoreMissingValuesFiles,
		PostRenderers:            hr.PostRenderers,
	}
	if hr.Install != nil {
		khr.InstallDisableHooks = hr.Install.DisableHooks
		khr.InstallReplace = hr.Install.Replace
	}
	if hr.Upgrade != nil {
		khr.UpgradeDisableHooks = hr.Upgrade.DisableHooks
	}
	if hr.Test != nil {
		khr.TestEnable = hr.Test.Enable
	}
	hrBlob, _ := json.Marshal(khr)
	_, _ = h.Write([]byte("hr:"))
	_, _ = h.Write(hrBlob)
	_, _ = h.Write([]byte{0})

	// Resolved values: encoding/json already sorts map[string]V keys
	// alphabetically so this is stable across Go runs without an
	// explicit pre-sort pass.
	valuesBlob, err := json.Marshal(finalValues)
	if err != nil {
		return ""
	}
	_, _ = h.Write([]byte("values:"))
	_, _ = h.Write(valuesBlob)

	return hex.EncodeToString(h.Sum(nil))
}

// writeChartFingerprint mixes every loader.Load-derived field of ch
// into h. We deliberately read Metadata + Templates + Files + Values
// + Schema + Lock rather than serializing the whole *chart.Chart so
// the unexported parent/dependencies fields (cyclic references) don't
// trip encoding/json. Subcharts contribute via a recursive call.
//
// A nil chart yields a stable sentinel write so the key remains
// distinct from an empty chart.
func writeChartFingerprint(h hashWriter, ch *chart.Chart) bool {
	if ch == nil {
		_, _ = h.Write([]byte("chart:nil\n"))
		return true
	}
	_, _ = h.Write([]byte("chart:meta:"))
	if ch.Metadata != nil {
		metaBlob, _ := json.Marshal(ch.Metadata)
		_, _ = h.Write(metaBlob)
	}
	_, _ = h.Write([]byte{0})

	// Templates: loader.Load reads each file's Name+Data verbatim;
	// hash the same content in the same order helm walks (the slice
	// order is the on-disk file order, deterministic per layout).
	writeFileSlice(h, "tmpl", ch.Templates)
	// Files: README/LICENSE/values-*.yaml entries — chart valuesFiles
	// stack reads these, and `_` helpers can `tpl` them too. Their
	// content matters.
	writeFileSlice(h, "files", ch.Files)
	// Schema bytes (raw values.schema.json content) — when present,
	// helm uses it to validate values during render, and a schema
	// edit can flip an otherwise-identical (chart, values) pair from
	// success to failure.
	_, _ = h.Write([]byte("schema:"))
	_, _ = h.Write(ch.Schema)
	_, _ = h.Write([]byte{0})

	// Values: chart-default values map. helm coalesces these under
	// user values, so a default change can flip the render even when
	// the user values are byte-identical.
	valuesBlob, err := json.Marshal(ch.Values)
	if err != nil {
		return false
	}
	_, _ = h.Write([]byte("defaults:"))
	_, _ = h.Write(valuesBlob)
	_, _ = h.Write([]byte{0})

	// Subcharts: recurse. Helm's render walks dependencies and
	// includes their templates / values, so a subchart content
	// change must invalidate the parent's cache entry.
	for _, sub := range ch.Dependencies() {
		if !writeChartFingerprint(h, sub) {
			return false
		}
	}
	return true
}

// writeFileSlice hashes a tag + every (name, len, data) tuple in fs.
// Length-prefixing makes "ab"+"cd" distinct from "abc"+"d" so we
// don't lose collisions through naive concatenation.
func writeFileSlice(h hashWriter, tag string, fs []*common.File) {
	_, _ = h.Write([]byte(tag))
	_, _ = h.Write([]byte{':'})
	for _, f := range fs {
		if f == nil {
			_, _ = h.Write([]byte{0})
			continue
		}
		_, _ = h.Write([]byte(f.Name))
		_, _ = h.Write([]byte{0})
		// Cheap length delimiter — avoids ambiguity between
		// (name="a", data="b") and (name="ab", data="").
		_, _ = h.Write(strconv.AppendInt(nil, int64(len(f.Data)), 10))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write(f.Data)
		_, _ = h.Write([]byte{0})
	}
}

// hashWriter is the subset of hash.Hash and *bytes.Buffer we use
// for the chart-fingerprint walk. Carved out so writeChartFingerprint
// and writeFileSlice don't take an unbounded io.Writer dependency.
type hashWriter interface {
	Write(p []byte) (int, error)
}

// writeOptionsBlob marshals every render-affecting Options field
// into h via a tagged anonymous struct. Pulled into its own helper
// (rather than inlined into computeTemplateKey) so the json.Marshal
// stack frame doesn't dominate the keyer's escape analysis.
func writeOptionsBlob(h hashWriter, opts Options) {
	blob, _ := json.Marshal(struct {
		SkipCRDs    bool     `json:"skip_crds"`
		SkipTests   bool     `json:"skip_tests"`
		KubeVersion string   `json:"kube_version,omitempty"`
		APIVersions string   `json:"api_versions,omitempty"`
		IsUpgrade   bool     `json:"is_upgrade"`
		NoHooks     bool     `json:"no_hooks"`
		ShowOnly    []string `json:"show_only,omitempty"`
		EnableDNS   bool     `json:"enable_dns"`
	}{
		SkipCRDs:    opts.SkipCRDs,
		SkipTests:   opts.SkipTests,
		KubeVersion: opts.KubeVersion,
		APIVersions: opts.APIVersions,
		IsUpgrade:   opts.IsUpgrade,
		NoHooks:     opts.NoHooks,
		ShowOnly:    opts.ShowOnly,
		EnableDNS:   opts.EnableDNS,
	})
	_, _ = h.Write([]byte("opts:"))
	_, _ = h.Write(blob)
	_, _ = h.Write([]byte{0})
}
