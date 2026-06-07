package helm

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// templateCache is a size-bounded LRU of rendered helm template
// output. Keyed by computeTemplateKey (chart digest + resolved values
// + render options + post-renderers), it lets repeat HRs with the
// same effective inputs skip action.Install.RunWithContext — the
// single largest CPU + allocation consumer in the codebase
// (template.go's cited ~300 MB on a 200-HR run).
//
// The cache is in-memory only; persistence across `flate` invocations
// is handled by the disk-backed layer.
//
// Eviction is strict LRU bounded by total `limit` bytes (the sum of
// every entry's value size). On insert we evict from the back of the
// list until the running total is within the limit; on Get we promote
// to the front. A nil receiver is a tombstone for "caching disabled"
// — both Get and Put return cleanly on nil so call sites only need
// `c.templateCache != nil` guards at the constructor.
type templateCache struct {
	mu    sync.Mutex
	limit int64 // bytes; 0 = disabled
	size  int64 // current total bytes
	list  *list.List
	index map[string]*list.Element

	// disk is the persistent cross-process layer. nil
	// when disk caching is disabled (RenderCacheBytes <= 0 or empty
	// RenderCacheRoot). Get falls through to disk on memory miss and
	// promotes hits to the in-process LRU; Put writes through to disk
	// after the in-process insert. The disk layer is content-
	// addressed so two processes pointing at the same root share
	// entries safely.
	disk *diskRenderCache
}

// templateEntry is the value type stored in templateCache.list. cost
// is the byte count we charge against the limit — value's length at
// insert time, captured so we don't re-len() during eviction (which
// holds the mutex).
type templateEntry struct {
	key   string
	value string
	cost  int64
}

// newTemplateCache constructs a templateCache with the given byte
// limit and (optional) disk-backed layer. A limit of 0 (or negative)
// AND a nil disk cache returns nil — callers wire that through as
// "caching disabled" and never reach Get/Put.
//
// Passing disk != nil with limitBytes <= 0 still returns a valid
// cache: the in-memory layer becomes a write-through to disk on every
// Put (size accounting stays at zero, so eviction is a no-op), and
// Get falls straight through to disk. That shape is useful for
// embedders that want cross-process reuse without the memory cost of
// a same-process LRU.
func newTemplateCache(limitBytes int64, disk *diskRenderCache) *templateCache {
	if limitBytes <= 0 && disk == nil {
		return nil
	}
	return &templateCache{
		limit: limitBytes,
		list:  list.New(),
		index: map[string]*list.Element{},
		disk:  disk,
	}
}

// Get returns the cached value for key, promoting the entry to the
// front of the LRU list. Returns (zero, false) on miss or when c is
// nil (the "caching disabled" sentinel).
//
// On an in-memory miss with a wired disk layer, Get reads through to
// disk and promotes the hit back into the LRU so subsequent same-
// process Gets stay fast. Cross-process disk hits incur a gunzip read
// (cheap vs. the helm render they avoid).
func (c *templateCache) Get(key string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	if el, ok := c.index[key]; ok {
		c.list.MoveToFront(el)
		v := el.Value.(*templateEntry).value
		c.mu.Unlock()
		return v, true
	}
	c.mu.Unlock()

	// In-memory miss. Try the disk layer. Reads run unsynchronized —
	// atomic-rename writes mean we either see the previous complete
	// payload or the new one, never a torn read.
	if c.disk == nil {
		return "", false
	}
	raw, ok := c.disk.Get(key)
	if !ok {
		return "", false
	}
	v := string(raw)
	// Promote to the in-memory LRU so the second same-process Get
	// skips the disk read entirely. Reuses putLocked's eviction
	// accounting — including the "single entry exceeds limit" guard.
	c.mu.Lock()
	c.putLocked(key, v)
	c.mu.Unlock()
	return v, true
}

// Put inserts (or replaces) the entry for key. Evicts least-recently-
// used entries from the back until the running total is within limit.
// An entry whose own cost exceeds the limit is rejected from the in-
// memory layer (silently — the alternative is "store but immediately
// evict on the next Put", which churns the list for no benefit). The
// disk layer still receives the write — it has its own (much larger)
// size cap and the oversized-skip heuristic doesn't apply there.
//
// nil-receiver no-ops so callers can unconditionally Put after a
// render without re-checking the constructor wiring.
func (c *templateCache) Put(key, value string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.putLocked(key, value)
	c.mu.Unlock()
	// Write through to the disk layer outside the in-memory lock so
	// the gzip + atomic-write doesn't hold off concurrent Gets. Put
	// no-ops on a nil disk receiver.
	if c.disk != nil {
		c.disk.Put(key, []byte(value))
	}
}

// putLocked inserts (or replaces) the in-memory entry for key. Must
// be called with c.mu held. Pulled out of Put so Get's disk-promote
// path can reuse the same eviction accounting without duplicating the
// "oversized rejected", "replacement drops old", and "evict from
// back" branches.
func (c *templateCache) putLocked(key, value string) {
	cost := int64(len(value))
	// Replacement: drop the old entry first so the size accounting
	// stays correct (the replacement's cost may differ from the
	// previous entry's).
	if el, ok := c.index[key]; ok {
		c.removeElement(el)
	}
	if cost > c.limit {
		// Single oversized entry — don't try to cache it; evicting
		// every other entry to make room would thrash the cache for
		// no payoff (the entry would still be the next eviction
		// target on the next Put). Skip silently.
		return
	}
	for c.size+cost > c.limit && c.list.Len() > 0 {
		c.removeElement(c.list.Back())
	}
	entry := &templateEntry{key: key, value: value, cost: cost}
	c.index[key] = c.list.PushFront(entry)
	c.size += cost
}

// removeElement drops el from both the list and the index, and
// decrements the running size. Must be called with c.mu held.
func (c *templateCache) removeElement(el *list.Element) {
	entry := el.Value.(*templateEntry)
	c.list.Remove(el)
	delete(c.index, entry.key)
	c.size -= entry.cost
}

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
	writeChartFingerprint(h, ch)
	return hex.EncodeToString(h.Sum(nil))
}

// computeTemplateKey returns a hex-encoded sha256 of every input
// that affects Template's output for a given (chart, values, opts,
// hr) tuple. A change in any input yields a distinct key, so a
// stale entry never serves a different render.
//
// chartFP is the precomputed chart fingerprint (see chartFingerprint
// + Client.LoadChart's caching layer); pass the empty string when no
// memoized digest is available and the function will recompute it
// on the fly.
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
// finalValues when chartFP is precomputed. A 50 KB values map
// marshals in ~50 µs — cheap vs. the 5-50 ms render the cache hit
// avoids.
func computeTemplateKey(chartFP string, ch *chart.Chart, finalValues map[string]any, opts Options, hr *manifest.HelmRelease) string {
	h := sha256.New()

	// Chart fingerprint. Precomputed at LoadChart time when the
	// template cache is enabled — fall back to a fresh walk when
	// callers pass an empty fingerprint (tests, direct compute calls).
	if chartFP == "" {
		writeChartFingerprint(h, ch)
	} else {
		_, _ = h.Write([]byte("chart-fp:"))
		_, _ = h.Write([]byte(chartFP))
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
		DisableSchemaValidation:  hr.DisableSchemaValidation,
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
	// explicit pre-sort pass. nil and empty map collapse to the same
	// `null` token, matching action.Install's treatment of both.
	valuesBlob, _ := json.Marshal(finalValues)
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
func writeChartFingerprint(h hashWriter, ch *chart.Chart) {
	if ch == nil {
		_, _ = h.Write([]byte("chart:nil\n"))
		return
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
	valuesBlob, _ := json.Marshal(ch.Values)
	_, _ = h.Write([]byte("defaults:"))
	_, _ = h.Write(valuesBlob)
	_, _ = h.Write([]byte{0})

	// Subcharts: recurse. Helm's render walks dependencies and
	// includes their templates / values, so a subchart content
	// change must invalidate the parent's cache entry.
	for _, sub := range ch.Dependencies() {
		writeChartFingerprint(h, sub)
	}
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
		SkipCRDs             bool     `json:"skip_crds"`
		SkipTests            bool     `json:"skip_tests"`
		SkipSecrets          bool     `json:"skip_secrets"`
		SkipKinds            []string `json:"skip_kinds,omitempty"`
		KubeVersion          string   `json:"kube_version,omitempty"`
		APIVersions          string   `json:"api_versions,omitempty"`
		IsUpgrade            bool     `json:"is_upgrade"`
		NoHooks              bool     `json:"no_hooks"`
		ShowOnly             []string `json:"show_only,omitempty"`
		EnableDNS            bool     `json:"enable_dns"`
		SkipSchemaValidation bool     `json:"skip_schema_validation"`
	}{
		SkipCRDs:             opts.SkipCRDs,
		SkipTests:            opts.SkipTests,
		SkipSecrets:          opts.SkipSecrets,
		SkipKinds:            opts.SkipKinds,
		KubeVersion:          opts.KubeVersion,
		APIVersions:          opts.APIVersions,
		IsUpgrade:            opts.IsUpgrade,
		NoHooks:              opts.NoHooks,
		ShowOnly:             opts.ShowOnly,
		EnableDNS:            opts.EnableDNS,
		SkipSchemaValidation: opts.SkipSchemaValidation,
	})
	_, _ = h.Write([]byte("opts:"))
	_, _ = h.Write(blob)
	_, _ = h.Write([]byte{0})
}
