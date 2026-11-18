package loader

import (
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/engine/source/safepath"
)

type chartReaders struct {
	names   []string
	missing []string
	remote  bool
}

func (rf *readerFinalizer) chartNames(ctx context.Context, location, chartPath string) (chartReaders, error) {
	if names, seen := rf.charts[location]; seen {
		return names, nil
	}
	inputFile := new(rf.loader.documents).input(location)
	var names chartReaders
	description, parentMissing, _ := chartinput.Describe(ctx, inputFile.fs, chartinput.Options{
		Name: inputFile.name,
		Validate: func(name string) error {
			_, err := safepath.ArchiveName(name)
			return err
		},
	}, func(name string, _ fs.FileInfo, data []byte) {
		names.names = append(names.names, path.Join(chartPath, name))
		if strings.HasPrefix(name, "templates/") || strings.Contains(name, "/templates/") {
			if data == nil {
				data, _ = fs.ReadFile(inputFile.fs, path.Join(inputFile.name, name))
			}
			if data != nil {
				rf.templateBodies[location] = append(rf.templateBodies[location], data)
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
		rf.templateBodies[location] = names.addDependencies(
			&description, inputFile.name, chartPath, rf.templateBodies[location],
		)
	}
	if err := ctx.Err(); err != nil {
		return chartReaders{}, err
	}
	rf.charts[location] = names
	return names, nil
}

func (r *chartReaders) addDependencies(
	description *chartinput.Description,
	inputName, chartPath string,
	bodies [][]byte,
) [][]byte {
	coordinate := func(name string) string {
		relative, _ := filepath.Rel(filepath.FromSlash(inputName), filepath.FromSlash(name))
		return path.Join(chartPath, filepath.ToSlash(relative))
	}
	capture := func(input chartinput.Capture, origin string) {
		if input.Files == nil {
			r.names = append(r.names, coordinate(origin))
			return
		}
		r.names = append(r.names, coordinate(path.Join(origin, ".helmignore")))
		for _, file := range input.Files {
			r.names = append(r.names, coordinate(path.Join(origin, file.Name)))
			if strings.HasPrefix(file.Name, "templates/") || strings.Contains(file.Name, "/templates/") {
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
		if _, err := safepath.ArchiveName(dependency.Path); err != nil {
			continue
		}
		if dependency.Capture.Files == nil && dependency.Capture.Archive == nil {
			r.missing = append(r.missing, coordinate(dependency.Path))
			continue
		}
		capture(dependency.Capture, dependency.Path)
	}
	return bodies
}
