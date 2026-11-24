package helm

import (
	"context"
	"fmt"
	"io/fs"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/pkg/manifest"
)

type capturedChart struct {
	files        []*archive.BufferedFile
	archive      []byte
	dependencies *chartinput.Description
	parent       *chart.Chart
}

func (c *capturedChart) dependencyInputs() []chartinput.Dependency {
	if c.dependencies == nil {
		return nil
	}
	return c.dependencies.Dependencies
}

func pendingDependency(dependencies []chartinput.Dependency) *chartinput.Dependency {
	for i := range dependencies {
		if dependencies[i].Capture.Empty() {
			return &dependencies[i]
		}
	}
	return nil
}

func unresolvedDependency(dep *chartinput.Dependency) error {
	return manifest.NewError(
		manifest.ErrInput,
		nil,
		fmt.Sprintf("unresolved chart dependency %s (%s)", dep.Name, dep.Declaration.Repository),
	)
}

func (c *capturedChart) fingerprint() string {
	var parent string
	if c.files == nil {
		parent = manifest.Fingerprint(c.archive)
	} else {
		content := make([]chartFileContent, len(c.files))
		for i, f := range c.files {
			content[i] = chartFileContent{Name: f.Name, Data: f.Data}
		}
		parent = manifest.Fingerprint(content)
	}
	dependencies := c.dependencyInputs()
	if len(dependencies) == 0 {
		return parent
	}
	type child struct {
		Name    string
		Content string
	}
	children := make([]child, len(dependencies))
	for i := range dependencies {
		dep := &dependencies[i]
		content := capturedChart{files: dep.Capture.Files, archive: dep.Capture.Archive}
		children[i] = child{Name: dep.Name, Content: content.fingerprint()}
	}
	return manifest.Fingerprint(struct {
		Parent       string
		Dependencies []child
	}{parent, children})
}

func (c *capturedChart) load() (*chart.Chart, error) {
	dependencies := c.dependencyInputs()
	parent := c.parent
	if parent == nil {
		var err error
		parent, err = (chartinput.Capture{Files: c.files, Archive: c.archive}).Load()
		if err != nil {
			return nil, err
		}
	}
	if len(dependencies) != 0 && c.parent != nil {
		parent = cloneChartForRender(parent)
	}
	for i := range dependencies {
		dep := &dependencies[i]
		if dep.Capture.Empty() {
			return nil, unresolvedDependency(dep)
		}
		child, err := dep.Capture.Load()
		if err != nil {
			return nil, fmt.Errorf("dependency %s: %w", dep.Name, err)
		}
		if dep.Declaration.Alias != "" {
			child.Metadata.Name = dep.Declaration.Alias
		}
		parent.AddDependency(child)
	}
	return parent, nil
}

func captureInput(ctx context.Context, input fs.FS, name string, archiveShape bool) (capturedChart, bool, error) {
	parent, _, missing, err := chartinput.CaptureAt(
		ctx,
		input,
		chartinput.Options{Name: name, Archive: archiveShape},
		nil,
	)
	if err != nil {
		if missing {
			err = fmt.Errorf("chart not found at %s: %w", name, err)
		}
		err = manifest.NewError(manifest.ErrInput, err, "")
	}
	return capturedChart{files: parent.Files, archive: parent.Archive}, missing, err
}

func (c *Client) completeDescription(
	ctx context.Context,
	input fs.FS,
	description *chartDescription,
	fresh bool,
) (chartDescription, error) {
	d := *description
	built := *description.captured.dependencies
	if !fresh {
		var err error
		built, err = description.captured.dependencies.Complete(ctx, input, d.location)
		if err != nil {
			return chartDescription{}, manifest.NewError(manifest.ErrInput, err, "")
		}
	}
	d.captured = capturedChart{
		files:        built.Parent.Files,
		archive:      built.Parent.Archive,
		dependencies: &built,
		parent:       built.Chart,
	}
	d.fingerprint = d.captured.fingerprint()
	if d.fingerprint == description.fingerprint {
		return d, nil
	}
	d.key = d.fingerprint
	d.needsBuild = false
	c.chartMu.RLock()
	cached, found := c.chartCache[d.key]
	c.chartMu.RUnlock()
	if found {
		cached.location = d.location
		return cached, nil
	}
	return c.rememberDescription(&d, nil), nil
}
