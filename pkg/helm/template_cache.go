package helm

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/values"
)

type chartFileContent struct {
	Name string
	Data []byte
}

type chartAnalysis struct {
	Version        string
	Templates      string
	Allowed        map[string]struct{}
	RequiresLoader bool
}

type renderRecord struct {
	key      string
	identity string
	Output   []byte
	Analysis *chartAnalysis
	frame    renderFrame
	receipt  *manifest.RenderReceipt
	binding  templateBinding
}

type ociChartContent struct {
	Digest          string
	Revision        string
	DisableTracking bool
}

// CommonMetadata is stamped after the cached pre-stamp render. Unsupported
// content returns an empty key, which MUST bypass lookup and publication.
func computeTemplateKey(loaded chartDescription, releaseValues map[string]any, opts Options, hr *manifest.HelmRelease) string {
	if loaded.fingerprint == "" {
		return ""
	}
	type keyHR struct {
		ReleaseName              string
		ReleaseNamespace         string
		CRDsPolicy               helmv2.CRDsPolicy
		DisableSchemaValidation  bool
		DisableOpenAPIValidation bool
		InstallDisableHooks      bool
		InstallReplace           bool
		UpgradeDisableHooks      bool
		TestEnable               bool
		ChartVersion             string
		ChartValuesFiles         []string
		IgnoreMissingValuesFiles bool
		PostRenderers            []helmv2.PostRenderer
	}
	crds, disableOpenAPI := selectedActionPolicy(hr, opts.IsUpgrade)
	khr := keyHR{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		CRDsPolicy:               crds,
		DisableSchemaValidation:  schemaValidationSkipped(opts, hr, nil),
		DisableOpenAPIValidation: disableOpenAPI,
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

	inputs := loaded.values.fingerprint
	type keyOptions struct {
		SkipCRDs, SkipTests      bool
		KubeVersion, APIVersions string
		IsUpgrade, NoHooks       bool
		ShowOnly                 []string
		EnableDNS                bool
	}
	return manifest.Fingerprint(struct {
		Chart   string
		OCI     *ociChartContent
		Inputs  *string
		Values  map[string]any
		Options keyOptions
		Release keyHR
	}{
		Chart: loaded.fingerprint, OCI: loaded.oci, Inputs: inputs, Values: releaseValues,
		Options: keyOptions{opts.SkipCRDs, opts.SkipTests, opts.KubeVersion, opts.APIVersions, opts.IsUpgrade, opts.NoHooks, opts.ShowOnly, opts.EnableDNS},
		Release: khr,
	})
}

const frameDomain = "flatehr2"

const acceptedRenderAnnotation = "flate.accepted-render"

type renderHeader struct {
	Key, Binding, Output string
	Size                 int
	Analysis             *chartAnalysis
}

const frameHeaderLimit = 16 << 20

type templateBinding struct {
	Policy                                       manifest.HelmReleaseHistoryPolicy
	Source                                       manifest.NamedResource
	Kind, URL, Path, Root, Key, Revision, Digest string
	Alias                                        bool
	Options                                      Options
	Wipe                                         bool
	Dependencies                                 values.DependencySubstitutions
}

func (a templateBinding) same(b templateBinding) bool {
	if !a.Policy.Equal(b.Policy) || a.Source != b.Source || a.Kind != b.Kind || a.URL != b.URL || a.Path != b.Path || a.Root != b.Root || a.Key != b.Key || a.Revision != b.Revision || a.Digest != b.Digest || a.Alias != b.Alias || a.Wipe != b.Wipe || !a.Dependencies.Equal(b.Dependencies) {
		return false
	}
	x, y := a.Options, b.Options
	return x.DisableChartDigestTracking == y.DisableChartDigestTracking && x.SkipCRDs == y.SkipCRDs && x.SkipTests == y.SkipTests && x.KubeVersion == y.KubeVersion && x.APIVersions == y.APIVersions && x.IsUpgrade == y.IsUpgrade && x.NoHooks == y.NoHooks && x.EnableDNS == y.EnableDNS && slices.Equal(x.SkipKinds, y.SkipKinds) && slices.Equal(x.ShowOnly, y.ShowOnly)
}

type renderFrame struct {
	header                renderHeader
	binding               templateBinding
	chart                 *chart.Chart
	lookup, raw           string
	output, encoded       []byte
	accepted              *acceptedRender
	acceptedKey, identity string
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.header.Key != right.header.Key || !bytes.Equal(left.output, right.output) {
		return false
	}
	return left.binding.same(right.binding)
}

type acceptedRoot struct {
	Source manifest.NamedResource
	Paths  []string
}

type acceptedRecord struct {
	ID     manifest.NamedResource
	Record manifest.DiscoveryRecord
	Roots  []acceptedRoot
}

type acceptedRender struct {
	Version  int
	Complete bool
	Owner    manifest.NamedResource
	Raw      string
	Records  []acceptedRecord
}

func (a *acceptedRender) equal(b *acceptedRender) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.Complete != b.Complete || a.Owner != b.Owner || a.Raw != b.Raw || (a.Records == nil) != (b.Records == nil) || len(a.Records) != len(b.Records) {
		return false
	}
	for i, x := range a.Records {
		y := b.Records[i]
		p, q := x.Record, y.Record
		if x.ID != y.ID || p.Authored != q.Authored || p.Path != q.Path || p.File != q.File || p.Parent != q.Parent || p.Builder != q.Builder || (p.Inputs == nil) != (q.Inputs == nil) || !slices.Equal(p.Inputs, q.Inputs) || p.Pinned != q.Pinned || (p.Refs == nil) != (q.Refs == nil) || !slices.Equal(p.Refs, q.Refs) || (p.Productions == nil) != (q.Productions == nil) || !slices.Equal(p.Productions, q.Productions) || (x.Roots == nil) != (y.Roots == nil) || len(x.Roots) != len(y.Roots) {
			return false
		}
		for j, root := range x.Roots {
			other := y.Roots[j]
			if root.Source != other.Source || (root.Paths == nil) != (other.Paths == nil) || !slices.Equal(root.Paths, other.Paths) {
				return false
			}
		}
	}
	return true
}

func encodeFrame(header renderHeader, output []byte) ([]byte, error) {
	if output == nil {
		output = []byte{}
	}
	header.Size, header.Output = len(output), manifest.Fingerprint(output)
	data, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	if len(data) > frameHeaderLimit {
		return nil, errors.New("render header exceeds frame limit")
	}
	frame := make([]byte, 12+len(data)+len(output))
	copy(frame, frameDomain)
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(data))) //nolint:gosec // The header is bounded by frameHeaderLimit.
	copy(frame[12:], data)
	copy(frame[12+len(data):], output)
	return frame, nil
}

func decodeFrame(data []byte) (renderFrame, error) {
	if len(data) < 12 || string(data[:8]) != frameDomain {
		return renderFrame{}, errors.New("invalid render frame domain or version")
	}
	size := int(binary.BigEndian.Uint32(data[8:12]))
	if size > frameHeaderLimit || size > len(data)-12 {
		return renderFrame{}, errors.New("invalid render header length")
	}
	var header renderHeader
	if err := json.Unmarshal(data[12:12+size], &header); err != nil {
		return renderFrame{}, err
	}
	begin := 12 + size
	if header.Size < 0 || header.Size > len(data)-begin {
		return renderFrame{}, errors.New("invalid render payload length")
	}
	end := begin + header.Size
	output := data[begin:end]
	if header.Output == "" || manifest.Fingerprint(output) != header.Output {
		return renderFrame{}, errors.New("invalid render payload identity")
	}
	frame := renderFrame{header: header, output: bytes.Clone(output), raw: manifest.Fingerprint(data[12:begin])}
	tail := data[end:]
	if len(tail) == 0 || len(tail) > frameHeaderLimit {
		return frame, nil
	}
	var accepted acceptedRender
	if err := json.Unmarshal(tail, &accepted); err != nil || accepted.Version != 1 || !accepted.Complete || accepted.Raw != frame.raw {
		return frame, nil
	}
	for i, item := range accepted.Records {
		if i > 0 && accepted.Records[i-1].ID.Compare(item.ID) >= 0 {
			return frame, nil
		}
		for j, root := range item.Roots {
			if j > 0 && item.Roots[j-1].Source.Compare(root.Source) >= 0 {
				return frame, nil
			}
		}
	}
	frame.accepted, frame.acceptedKey = &accepted, manifest.Fingerprint(accepted)
	return frame, nil
}

func prepareAccepted(owner manifest.NamedResource, frame renderFrame, records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]) (acceptedRender, error) {
	accepted := acceptedRender{Version: 1, Complete: true, Owner: owner, Raw: frame.raw}
	for id, record := range records {
		item := acceptedRecord{ID: id, Record: record}
		item.Record.ContentRoots = nil
		item.Record.Inputs = slices.Clone(record.Inputs)
		item.Record.Refs = slices.Clone(record.Refs)
		slices.SortFunc(item.Record.Refs, manifest.NamedResource.Compare)
		item.Record.Refs = slices.Compact(item.Record.Refs)
		item.Record.Productions = slices.Clone(record.Productions)
		slices.SortFunc(item.Record.Productions, func(a, b manifest.Production) int {
			return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration), a.Authored.Compare(b.Authored), a.Source.Compare(b.Source), cmp.Compare(a.Path, b.Path), cmp.Compare(a.File, b.File))
		})
		item.Record.Productions = slices.Compact(item.Record.Productions)
		for source, paths := range record.ContentRoots {
			paths = slices.Clone(paths)
			slices.Sort(paths)
			item.Roots = append(item.Roots, acceptedRoot{Source: source, Paths: slices.Compact(paths)})
		}
		slices.SortFunc(item.Roots, func(a, b acceptedRoot) int { return a.Source.Compare(b.Source) })
		accepted.Records = append(accepted.Records, item)
	}
	slices.SortFunc(accepted.Records, func(a, b acceptedRecord) int { return a.ID.Compare(b.ID) })
	for i := 1; i < len(accepted.Records); i++ {
		if accepted.Records[i-1].ID == accepted.Records[i].ID {
			return acceptedRender{}, errors.New("duplicate accepted render identity")
		}
	}
	return accepted, nil
}

func (f renderFrame) history(owner manifest.NamedResource) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.accepted == nil || f.accepted.Owner != owner {
		return nil, false
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		for _, item := range f.accepted.Records {
			record := item.Record
			record.Inputs = slices.Clone(record.Inputs)
			record.Refs = slices.Clone(record.Refs)
			record.Productions = slices.Clone(record.Productions)
			if len(item.Roots) > 0 {
				record.ContentRoots = make(map[manifest.NamedResource][]string, len(item.Roots))
			}
			for _, root := range item.Roots {
				record.ContentRoots[root.Source] = slices.Clone(root.Paths)
			}
			if !yield(item.ID, record) {
				return
			}
		}
	}, true
}

// RecordRender persists a receipt only with a complete accepted inventory.
func (c *Client) RecordRender(ctx context.Context, owner manifest.NamedResource, receipt *manifest.RenderReceipt, records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]) error {
	if receipt == nil || records == nil {
		return nil
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" || frame.header.Analysis == nil && frame.chart == nil {
		return fmt.Errorf("%w: invalid helm receipt", manifest.ErrFlux)
	}
	descriptor, identity, enabled, err := c.cache.RenderMetadata(ctx, frame.header.Key, frame.lookup)
	if err != nil && !errors.Is(err, errdef.ErrNotFound) {
		return fmt.Errorf("lookup helm render cache: %w", err)
	}
	if !enabled {
		return nil
	}
	if frame.header.Binding == "" {
		frame.header.Binding = manifest.Fingerprint(frame.binding)
	}
	if frame.header.Analysis == nil {
		frame.header.Analysis = (chartDescription{chart: frame.chart}).analyze()
	}
	if frame.encoded == nil && frame.raw == "" {
		frame.encoded, err = encodeFrame(frame.header, frame.output)
		if err != nil {
			return fmt.Errorf("%w: encode render frame: %w", manifest.ErrFlux, err)
		}
	}
	if frame.raw == "" {
		frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	}
	accepted, err := prepareAccepted(owner, frame, records)
	if err != nil {
		return fmt.Errorf("%w: encode accepted helm render: %w", manifest.ErrFlux, err)
	}
	if frame.accepted != nil && frame.identity != "" && identity == frame.identity && frame.acceptedKey != "" && descriptor.Annotations[acceptedRenderAnnotation] == frame.acceptedKey && frame.accepted.equal(&accepted) {
		return nil
	}
	content := manifest.Fingerprint(accepted)
	if descriptor.Annotations[acceptedRenderAnnotation] == content {
		return nil
	}
	data, err := json.Marshal(accepted)
	if err != nil {
		return fmt.Errorf("%w: encode accepted helm render: %w", manifest.ErrFlux, err)
	}
	if len(data) > frameHeaderLimit {
		return fmt.Errorf("%w: accepted helm render exceeds frame limit", manifest.ErrFlux)
	}
	if frame.encoded == nil {
		frame.encoded, err = encodeFrame(frame.header, frame.output)
		if err != nil {
			return fmt.Errorf("%w: encode render frame: %w", manifest.ErrFlux, err)
		}
	}
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	raw := frame.encoded
	result := cache.ProducerResult{Annotations: map[string]string{acceptedRenderAnnotation: content}}
	_, _, _, err = c.cache.Blob(ctx, key, cache.Policy{MaxAge: 0, Retry: cache.RetryConfig{Attempts: 1}}, func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
		_, err := io.Copy(w, io.MultiReader(bytes.NewReader(raw), bytes.NewReader(data)))
		return result, err
	})
	if err != nil {
		return fmt.Errorf("publish helm render cache: %w", err)
	}
	return nil
}

func (c *Client) lookupRender(ctx context.Context, loaded *chartDescription, key string) (*renderRecord, error) {
	record := loaded.record
	if record != nil && record.key == key {
		_, identity, enabled, err := c.cache.RenderMetadata(ctx, key, record.frame.lookup)
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lookup helm render cache: %w", err)
		}
		if !enabled || identity == "" || identity != record.identity {
			record = nil
		}
	} else {
		record = nil
	}
	if record == nil {
		reader, _, lookup, err := c.cache.Blob(ctx, cache.BlobKey{Content: key, Render: cache.RenderAccepted}, cache.Policy{MaxAge: cache.Forever}, nil)
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lookup helm render cache: %w", err)
		}
		identity := cache.AcquisitionIdentity(reader)
		data, err := fs.ReadFile(reader, "content")
		if err != nil {
			return nil, fmt.Errorf("%w: read helm render cache: %w", manifest.ErrFlux, err)
		}
		frame, err := decodeFrame(data)
		if err != nil || frame.header.Key != key || frame.header.Analysis == nil {
			return nil, nil
		}
		frame.lookup, frame.identity = lookup, identity
		record = &renderRecord{key: key, identity: identity, Output: frame.output, Analysis: frame.header.Analysis, frame: frame}
	}
	if record.Analysis.RequiresLoader {
		if err := c.decodeChart(ctx, loaded); err != nil {
			return nil, err
		}
	}
	if loaded.oci != nil && record != loaded.record {
		if _, err := ociChartVersion(record.Analysis.Version, loaded.artifact.Revision); err != nil {
			return nil, err
		}
	}
	return record, nil
}

// PreviousRender returns complete accepted history without running an action or acquiring a chart.
func (c *Client) PreviousRender(ctx context.Context, hr *manifest.HelmRelease, hrValues map[string]any, input RenderOptions) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if ctx.Err() != nil || !c.cache.CanPersistRenders() {
		return nil, false
	}
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, input)
	if err != nil || prepared.key == "" || prepared.loaded.valuesErr != nil || prepared.capabilityErr != nil {
		return nil, false
	}
	record, err := c.lookupRender(ctx, &prepared.loaded, prepared.key)
	if err != nil || record == nil || record.frame.header.Binding != manifest.Fingerprint(prepared.binding) {
		return nil, false
	}
	if _, err := manifest.DecodeRender(record.Output); err != nil {
		return nil, false
	}
	if ctx.Err() != nil {
		return nil, false
	}
	records, complete := record.frame.history(hr.Named())
	if complete && record != prepared.loaded.record {
		c.rememberDescription(prepared.loaded, record)
	}
	return records, complete
}
