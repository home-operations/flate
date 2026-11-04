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
	Name string `json:"Name"`
	Data []byte `json:"Data"`
}

type chartAnalysis struct {
	Version        string              `json:"Version"`
	Templates      string              `json:"Templates"`
	Allowed        map[string]struct{} `json:"Allowed"`
	RequiresLoader bool                `json:"RequiresLoader"`
}

type renderRecord struct {
	key      string
	identity string
	Output   []byte         `json:"Output"`
	Analysis *chartAnalysis `json:"Analysis"`
	frame    renderFrame
	receipt  *manifest.RenderReceipt
	binding  templateBinding
}

type ociChartContent struct {
	Digest          string `json:"Digest"`
	Revision        string `json:"Revision"`
	DisableTracking bool   `json:"DisableTracking"`
}

// CommonMetadata is stamped after the cached pre-stamp render. Unsupported
// content returns an empty key, which MUST bypass lookup and publication.
func computeTemplateKey(
	loaded *chartDescription,
	releaseValues map[string]any,
	opts Options,
	hr *manifest.HelmRelease,
) string {
	if loaded.fingerprint == "" {
		return ""
	}
	type keyHR struct {
		ReleaseName              string                `json:"ReleaseName"`
		ReleaseNamespace         string                `json:"ReleaseNamespace"`
		CRDsPolicy               helmv2.CRDsPolicy     `json:"CRDsPolicy"`
		DisableSchemaValidation  bool                  `json:"DisableSchemaValidation"`
		DisableOpenAPIValidation bool                  `json:"DisableOpenAPIValidation"`
		InstallDisableHooks      bool                  `json:"InstallDisableHooks"`
		InstallReplace           bool                  `json:"InstallReplace"`
		TestEnable               bool                  `json:"TestEnable"`
		ChartVersion             string                `json:"ChartVersion"`
		ChartValuesFiles         []string              `json:"ChartValuesFiles"`
		IgnoreMissingValuesFiles bool                  `json:"IgnoreMissingValuesFiles"`
		PostRenderers            []helmv2.PostRenderer `json:"PostRenderers"`
	}
	policy := (&helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}).GetInstall()
	khr := keyHR{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		CRDsPolicy:               cmp.Or(policy.CRDs, helmv2.Create),
		DisableSchemaValidation:  schemaValidationSkipped(hr, nil),
		DisableOpenAPIValidation: policy.DisableOpenAPIValidation,
		ChartVersion:             hr.Chart.Version,
		ChartValuesFiles:         hr.ChartValuesFiles,
		IgnoreMissingValuesFiles: hr.IgnoreMissingValuesFiles,
		PostRenderers:            hr.PostRenderers,
	}
	if hr.Install != nil {
		khr.InstallDisableHooks = hr.Install.DisableHooks
		khr.InstallReplace = hr.Install.Replace
	}
	if hr.Test != nil {
		khr.TestEnable = hr.Test.Enable
	}

	inputs := loaded.values.fingerprint
	type keyOptions struct {
		SkipCRDs    bool   `json:"SkipCRDs"`
		SkipTests   bool   `json:"SkipTests"`
		KubeVersion string `json:"KubeVersion"`
		APIVersions string `json:"APIVersions"`
		EnableDNS   bool   `json:"EnableDNS"`
	}
	return manifest.Fingerprint(struct {
		Chart   string           `json:"Chart"`
		OCI     *ociChartContent `json:"OCI"`
		Inputs  *string          `json:"Inputs"`
		Values  map[string]any   `json:"Values"`
		Options keyOptions       `json:"Options"`
		Release keyHR            `json:"Release"`
	}{
		Chart: loaded.fingerprint, OCI: loaded.oci, Inputs: inputs, Values: releaseValues,
		Options: keyOptions{opts.SkipCRDs, opts.SkipTests, opts.KubeVersion, opts.APIVersions, opts.EnableDNS},
		Release: khr,
	})
}

const frameDomain = "flatehr2"

type renderHeader struct {
	Key      string         `json:"Key"`
	Binding  string         `json:"Binding"`
	Output   string         `json:"Output"`
	Size     int            `json:"Size"`
	Analysis *chartAnalysis `json:"Analysis"`
}

const frameHeaderLimit = 16 << 20

type templateBinding struct {
	Policy       manifest.HelmReleaseHistoryPolicy `json:"Policy"`
	Source       manifest.NamedResource            `json:"Source"`
	Kind         string                            `json:"Kind"`
	URL          string                            `json:"URL"`
	Path         string                            `json:"Path"`
	Root         string                            `json:"Root"`
	Key          string                            `json:"Key"`
	Revision     string                            `json:"Revision"`
	Digest       string                            `json:"Digest"`
	Alias        bool                              `json:"Alias"`
	Options      Options                           `json:"Options"`
	Wipe         bool                              `json:"Wipe"`
	Dependencies values.DependencySubstitutions    `json:"Dependencies"`
}

func (a *templateBinding) same(b *templateBinding) bool {
	if !a.Policy.Equal(b.Policy) || a.Source != b.Source || a.Kind != b.Kind || a.URL != b.URL || a.Path != b.Path ||
		a.Root != b.Root ||
		a.Key != b.Key ||
		a.Revision != b.Revision ||
		a.Digest != b.Digest ||
		a.Alias != b.Alias ||
		a.Wipe != b.Wipe ||
		!a.Dependencies.Equal(b.Dependencies) {
		return false
	}
	x, y := a.Options, b.Options
	return x.DisableChartDigestTracking == y.DisableChartDigestTracking && x.SkipCRDs == y.SkipCRDs &&
		x.SkipTests == y.SkipTests &&
		x.KubeVersion == y.KubeVersion &&
		x.APIVersions == y.APIVersions &&
		x.EnableDNS == y.EnableDNS &&
		slices.Equal(x.SkipKinds, y.SkipKinds)
}

type renderFrame struct {
	header          renderHeader
	binding         templateBinding
	chart           *chart.Chart
	lookup, raw     string
	output, encoded []byte
	accepted        *acceptedRender
	acceptedKey     string
}

func equalRenderFrame(a, b any) bool {
	left, ok := a.(renderFrame)
	right, other := b.(renderFrame)
	if !ok || !other || left.header.Key != right.header.Key || !bytes.Equal(left.output, right.output) {
		return false
	}
	return left.binding.same(&right.binding)
}

type acceptedRoot struct {
	Source manifest.NamedResource `json:"Source"`
	Paths  []string               `json:"Paths"`
}

type acceptedRecord struct {
	ID     manifest.NamedResource   `json:"ID"`
	Record manifest.DiscoveryRecord `json:"Record"`
	Roots  []acceptedRoot           `json:"Roots"`
}

type acceptedRender struct {
	Version  int                    `json:"Version"`
	Complete bool                   `json:"Complete"`
	Owner    manifest.NamedResource `json:"Owner"`
	Raw      string                 `json:"Raw"`
	Records  []acceptedRecord       `json:"Records"`
}

func (a *acceptedRender) equal(b *acceptedRender) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.Complete != b.Complete || a.Owner != b.Owner || a.Raw != b.Raw {
		return false
	}
	if (a.Records == nil) != (b.Records == nil) || len(a.Records) != len(b.Records) {
		return false
	}
	{
		xValues := a.Records
		for i := range xValues {
			x := xValues[i]
			y := b.Records[i]
			if x.ID != y.ID || !x.Record.Equal(y.Record) || (x.Roots == nil) != (y.Roots == nil) ||
				len(x.Roots) != len(y.Roots) {
				return false
			}
			for j, root := range x.Roots {
				other := y.Roots[j]
				if root.Source != other.Source || (root.Paths == nil) != (other.Paths == nil) ||
					!slices.Equal(root.Paths, other.Paths) {
					return false
				}
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
		return nil, fmt.Errorf("encode render frame: %w", err)
	}
	if len(data) > frameHeaderLimit {
		return nil, errors.New("render header exceeds frame limit")
	}
	frame := make([]byte, framePrefixSize+len(data)+len(output))
	copy(frame, frameDomain)
	binary.BigEndian.PutUint32(
		frame[8:framePrefixSize],
		uint32(len(data)), //nolint:gosec // frameHeaderLimit bounds the header length below uint32 capacity.
	)
	copy(frame[framePrefixSize:], data)
	copy(frame[framePrefixSize+len(data):], output)
	return frame, nil
}

func decodeFrame(data []byte) (renderFrame, error) {
	if len(data) < framePrefixSize || string(data[:8]) != frameDomain {
		return renderFrame{}, errors.New("invalid render frame domain or version")
	}
	size := int(binary.BigEndian.Uint32(data[8:framePrefixSize]))
	if size > frameHeaderLimit || size > len(data)-framePrefixSize {
		return renderFrame{}, errors.New("invalid render header length")
	}
	var header renderHeader
	if err := json.Unmarshal(data[framePrefixSize:framePrefixSize+size], &header); err != nil {
		return renderFrame{}, fmt.Errorf("decode render frame: %w", err)
	}
	begin := framePrefixSize + size
	if header.Size < 0 || header.Size > len(data)-begin {
		return renderFrame{}, errors.New("invalid render payload length")
	}
	end := begin + header.Size
	output := data[begin:end]
	if header.Output == "" || manifest.Fingerprint(output) != header.Output {
		return renderFrame{}, errors.New("invalid render payload identity")
	}
	frame := renderFrame{
		header: header,
		output: bytes.Clone(output),
		raw:    manifest.Fingerprint(data[framePrefixSize:begin]),
	}
	tail := data[end:]
	if len(tail) == 0 || len(tail) > frameHeaderLimit {
		return frame, nil
	}
	var accepted acceptedRender
	if err := json.Unmarshal(
		tail,
		&accepted,
	); err != nil || accepted.Version != 1 || !accepted.Complete ||
		accepted.Raw != frame.raw {
		//nolint:nilerr // Invalid cache metadata must remain a miss so the renderer can rebuild.
		return frame, nil
	}
	if !accepted.ordered() {
		return frame, nil
	}
	frame.accepted, frame.acceptedKey = &accepted, manifest.Fingerprint(accepted)
	return frame, nil
}

func prepareAccepted(
	owner manifest.NamedResource,
	frame *renderFrame,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
) (acceptedRender, error) {
	accepted := acceptedRender{Version: 1, Complete: true, Owner: owner, Raw: frame.raw}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for id, record := range records {
		item := acceptedRecord{ID: id, Record: record}
		item.Record.ContentRoots = nil
		item.Record.Inputs = slices.Clone(record.Inputs)
		item.Record.Refs = slices.Clone(record.Refs)
		slices.SortFunc(item.Record.Refs, manifest.NamedResource.Compare)
		item.Record.Refs = slices.Compact(item.Record.Refs)
		item.Record.Productions = slices.Clone(record.Productions)
		slices.SortFunc(item.Record.Productions, func(a, b manifest.Production) int {
			return cmp.Or(
				a.Owner.Compare(b.Owner),
				a.Declaration.Compare(b.Declaration),
				a.Authored.Compare(b.Authored),
				a.Source.Compare(b.Source),
				cmp.Compare(a.Path, b.Path),
				cmp.Compare(a.File, b.File),
			)
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

//nolint:gocritic // Keep immutable value-receiver semantics.
func (f renderFrame) history(
	owner manifest.NamedResource,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if f.accepted == nil || f.accepted.Owner != owner {
		return nil, false
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		{
			itemValues2 := f.accepted.Records
			for itemIndex := range itemValues2 {
				item := itemValues2[itemIndex]
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
		}
	}, true
}

type renderPublication struct {
	frame    renderFrame
	accepted acceptedRender
}

// WriteTo writes the render frame and accepted inventory.
//
//nolint:gocritic // A publication is a value snapshot so encoding never mutates its accepted frame.
func (p renderPublication) WriteTo(w io.Writer) (int64, error) {
	if p.frame.encoded == nil {
		encoded, err := encodeFrame(p.frame.header, p.frame.output)
		if err != nil {
			return 0, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode render frame: %w", err), "")
		}
		p.frame.encoded = encoded
	}
	if !p.accepted.Complete {
		n, err := io.Copy(w, bytes.NewReader(p.frame.encoded))
		if err != nil {
			return n, fmt.Errorf("write helm render frame: %w", err)
		}
		return n, nil
	}
	data, err := json.Marshal(p.accepted)
	if err != nil {
		return 0, manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode accepted helm render: %w", err), "")
	}
	if len(data) > frameHeaderLimit {
		return 0, manifest.NewError(manifest.ErrFlux, nil, "accepted helm render exceeds frame limit")
	}
	n, err := io.Copy(w, io.MultiReader(bytes.NewReader(p.frame.encoded), bytes.NewReader(data)))
	if err != nil {
		return n, fmt.Errorf("write accepted helm render: %w", err)
	}
	return n, nil
}

// RenderAcceptance binds an accepted inventory to its source paths.
type RenderAcceptance struct {
	Owner   manifest.NamedResource                                      `json:"Owner"`
	Receipt *manifest.RenderReceipt                                     `json:"Receipt"`
	Records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] `json:"Records"`
	Sources []manifest.SourceEvidence                                   `json:"Sources"`
}

// RecordRender persists a receipt only with a complete accepted inventory.
//
//nolint:gocritic // Preserve the public value API and its accepted-inventory snapshot.
func (c *Client) RecordRender(ctx context.Context, acceptance RenderAcceptance) error {
	owner, receipt, records := acceptance.Owner, acceptance.Receipt, acceptance.Records
	if receipt == nil || records == nil {
		return nil
	}
	frame, ok := receipt.Value().(renderFrame)
	if !ok || frame.header.Key == "" || frame.header.Analysis == nil && frame.chart == nil {
		return manifest.NewError(manifest.ErrFlux, nil, "invalid helm receipt")
	}
	if !c.cache.CanPersistRenders() {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return ctx.Err()
	}
	var err error
	if frame.header.Binding == "" {
		frame.header.Binding = manifest.Fingerprint(frame.binding)
	}
	if frame.header.Analysis == nil {
		frame.header.Analysis = (&chartDescription{chart: frame.chart}).analyze()
	}
	if frame.encoded == nil && frame.raw == "" {
		frame.encoded, err = encodeFrame(frame.header, frame.output)
		if err != nil {
			return manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode render frame: %w", err), "")
		}
	}
	if frame.raw == "" {
		frame.raw = manifest.Fingerprint(
			frame.encoded[12 : framePrefixSize+int(binary.BigEndian.Uint32(frame.encoded[8:framePrefixSize]))],
		)
	}
	var accepted acceptedRender
	content := frame.raw
	if acceptance.Sources != nil {
		accepted, err = prepareAccepted(owner, &frame, records)
		if err != nil {
			return manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode accepted helm render: %w", err), "")
		}
		content = frame.acceptedKey
		if frame.accepted == nil || content == "" || !frame.accepted.equal(&accepted) {
			content = manifest.Fingerprint(accepted)
		}
	}
	return cache.RecordInventory(
		ctx,
		c.cache,
		cache.AcceptedInventory[renderPublication]{
			Key:        frame.header.Key,
			Lookup:     frame.lookup,
			Acceptance: content,
			History:    cache.RenderHistory{Owner: owner, Sources: acceptance.Sources},
			Content:    renderPublication{frame: frame, accepted: accepted},
		},
	)
}

func (c *Client) lookupRender(ctx context.Context, loaded *chartDescription, key string) (*renderRecord, error) {
	record := loaded.record
	if record != nil && record.key == key {
		renderMetadataResult1, renderMetadataResult1Err := c.cache.RenderMetadata(ctx, key, record.frame.lookup)
		_, identity, enabled, err := renderMetadataResult1.Descriptor, renderMetadataResult1.Identity, renderMetadataResult1.Enabled, renderMetadataResult1Err
		if errors.Is(err, errdef.ErrNotFound) {
			//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
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
		var err error
		record, err = c.readRender(ctx, key)
		if err != nil || record == nil {
			return nil, err
		}
	}
	var chartErr error
	if record.Analysis.RequiresLoader {
		chartErr = c.decodeChart(ctx, loaded)
	}
	if chartErr != nil {
		return nil, chartErr
	}
	if loaded.oci != nil && record != loaded.record {
		if _, err := ociChartVersion(record.Analysis.Version, loaded.artifact.Revision); err != nil {
			return nil, err
		}
	}
	return record, nil
}

// PreviousRender returns complete accepted history without running an action or acquiring a chart.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func (c *Client) PreviousRender(
	ctx context.Context,
	hr *manifest.HelmRelease,
	hrValues map[string]any,
	input RenderOptions,
) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
	if ctx.Err() != nil || !c.cache.CanPersistRenders() {
		return nil, false
	}
	prepared, err := c.prepareTemplate(ctx, hr, hrValues, &input)
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
		c.rememberDescription(&prepared.loaded, record)
	}
	return records, complete
}

const framePrefixSize = 12

func (a *acceptedRender) ordered() bool {
	itemValues := a.Records
	for i := range itemValues {
		item := itemValues[i]
		if i > 0 && a.Records[i-1].ID.Compare(item.ID) >= 0 {
			return false
		}
		for j, root := range item.Roots {
			if j > 0 && item.Roots[j-1].Source.Compare(root.Source) >= 0 {
				return false
			}
		}
	}
	return true
}

func (c *Client) readRender(ctx context.Context, key string) (*renderRecord, error) {
	reader, _, lookup, err := c.cache.Blob(
		ctx,
		cache.BlobKey{Content: key, Render: cache.RenderAccepted},
		cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	if errors.Is(err, errdef.ErrNotFound) {
		//nolint:nilnil // An unavailable receipt is a cache miss, not a render failure.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup helm render cache: %w", err)
	}
	identity := cache.AcquisitionIdentity(reader)
	data, err := fs.ReadFile(reader, "content")
	if err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read helm render cache: %w", err), "")
	}
	frame, err := decodeFrame(data)
	if err != nil || frame.header.Key != key || frame.header.Analysis == nil {
		//nolint:nilerr,nilnil // Invalid frame metadata is an ordinary cache miss.
		return nil, nil
	}
	frame.lookup = lookup
	record := &renderRecord{
		key:      key,
		identity: identity,
		Output:   frame.output,
		Analysis: frame.header.Analysis,
		frame:    frame,
	}
	return record, nil
}
