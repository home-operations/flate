package helm

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

type chartFileContent struct {
	Name string
	Data []byte
}

type chartContent struct {
	Metadata     *chart.Metadata
	Templates    []*chartFileContent
	Files        []*chartFileContent
	Schema       []byte
	Values       map[string]any
	Dependencies []*chartContent
}

// Dependencies are unexported on chart.Chart; modification times and Lock do
// not define the cached render. Projections borrow immutable canonical content.
func projectChart(ch *chart.Chart) *chartContent {
	if ch == nil {
		return nil
	}
	content := &chartContent{Metadata: ch.Metadata, Schema: ch.Schema, Values: ch.Values}
	content.Templates = projectChartFiles(ch.Templates)
	content.Files = projectChartFiles(ch.Files)
	if dependencies := ch.Dependencies(); dependencies != nil {
		content.Dependencies = make([]*chartContent, len(dependencies))
		for i, sub := range dependencies {
			content.Dependencies[i] = projectChart(sub)
		}
	}
	return content
}

func projectChartFiles(files []*common.File) []*chartFileContent {
	if files == nil {
		return nil
	}
	out := make([]*chartFileContent, len(files))
	for i, f := range files {
		if f != nil {
			out[i] = &chartFileContent{Name: f.Name, Data: f.Data}
		}
	}
	return out
}

type ociChartContent struct {
	Digest          string
	Revision        string
	DisableTracking bool
}

// CommonMetadata is stamped after the cached pre-stamp render. Unsupported
// content returns an empty key, which MUST bypass lookup and publication.
func computeTemplateKey(loaded chartLoadResult, finalValues map[string]any, opts Options, hr *manifest.HelmRelease) string {
	if loaded.fingerprint == "" {
		return ""
	}
	type keyHR struct {
		ReleaseName              string
		ReleaseNamespace         string
		CRDsPolicy               string
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
		Values  map[string]any
		Options keyOptions
		Release keyHR
	}{
		Chart: loaded.fingerprint, OCI: loaded.oci, Values: finalValues,
		Options: keyOptions{opts.SkipCRDs, opts.SkipTests, opts.KubeVersion, opts.APIVersions, opts.IsUpgrade, opts.NoHooks, opts.ShowOnly, opts.EnableDNS},
		Release: khr,
	})
}
