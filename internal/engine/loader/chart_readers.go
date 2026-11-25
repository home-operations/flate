package loader

import (
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
)

// ChartReadPaths returns the same chart inputs that reader finalization indexes.
func (*Loader) ChartReadPaths(
	ctx context.Context,
	input *store.SourceArtifact,
	chart string,
) selection.SourceReadPaths {
	if input == nil || input.FS == nil {
		return selection.SourceReadPaths{}
	}
	chart = path.Clean(chart)
	names, _, _ := chartFiles(ctx, fileInput{fs: input.FS, name: chart}, chart, false)
	return selection.SourceReadPaths{Files: names.names, Roots: names.missing}
}

type chartReaders struct {
	names   []string
	missing []string
	remote  bool
}

func (rf *readerFinalizer) chartNames(ctx context.Context, location, chartPath string) (chartReaders, error) {
	if names, seen := rf.charts[location]; seen {
		return names, nil
	}
	input := new(rf.loader.documents).input(location)
	names, templates, err := chartFiles(ctx, input, chartPath, true)
	if err != nil {
		return chartReaders{}, err
	}
	rf.templateBodies[location] = templates
	rf.charts[location] = names
	return names, nil
}

func chartFiles(
	ctx context.Context,
	input fileInput,
	chartPath string,
	readTemplates bool,
) (chartReaders, [][]byte, error) {
	var names chartReaders
	var templates [][]byte
	description, parentMissing, _ := chartinput.Describe(ctx, input.fs, chartinput.Options{
		Name: input.name,
	}, func(file string, _ fs.FileInfo, data []byte) {
		names.names = append(names.names, path.Join(chartPath, file))
		if readTemplates && (strings.HasPrefix(file, "templates/") || strings.Contains(file, "/templates/")) {
			if data == nil {
				data, _ = fs.ReadFile(input.fs, path.Join(input.name, file))
			}
			if data != nil {
				templates = append(templates, data)
			}
		}
	})
	if !parentMissing {
		if description.Parent.Files != nil {
			names.names = append(names.names, path.Join(chartPath, ".helmignore"))
		} else if description.Parent.Archive != nil {
			names.names = append(names.names, chartPath)
		}
	}
	if len(description.Dependencies) != 0 {
		templates = names.addDependencies(&description, func(name string) string {
			relative, _ := filepath.Rel(filepath.FromSlash(input.name), filepath.FromSlash(name))
			return path.Join(chartPath, filepath.ToSlash(relative))
		}, templates, readTemplates)
	}
	return names, templates, ctx.Err()
}

func (r *chartReaders) addDependencies(
	description *chartinput.Description,
	coordinate func(string) string,
	bodies [][]byte,
	readTemplates bool,
) [][]byte {
	capture := func(input chartinput.Capture, origin string) {
		if input.Files == nil {
			r.names = append(r.names, coordinate(origin))
			return
		}
		r.names = append(r.names, coordinate(path.Join(origin, ".helmignore")))
		for _, file := range input.Files {
			r.names = append(r.names, coordinate(path.Join(origin, file.Name)))
			if readTemplates &&
				(strings.HasPrefix(file.Name, "templates/") || strings.Contains(file.Name, "/templates/")) {
				bodies = append(bodies, file.Data)
			}
		}
	}
	for i := range description.Dependencies {
		dependency := &description.Dependencies[i]
		if dependency.Remote() {
			r.remote = true
			continue
		}
		if dependency.Path == "" {
			continue
		}
		if dependency.Capture.Empty() {
			r.missing = append(r.missing, coordinate(dependency.Path))
			continue
		}
		capture(dependency.Capture, dependency.Path)
	}
	return bodies
}
