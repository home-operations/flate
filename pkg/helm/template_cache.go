package helm

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

type chartFileContent struct {
	Name string
	Data []byte
}

type chartAnalysis struct {
	Templates      string
	Allowed        map[string]struct{}
	RequiresLoader bool
}

type renderRecord struct {
	key      string
	identity string
	Output   string
	Analysis *chartAnalysis
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
