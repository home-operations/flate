package chartinput

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Masterminds/semver/v3"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
)

// Capture MUST retain immutable bytes until all chart consumers finish.
type Capture struct {
	Files   []*archive.BufferedFile
	Archive []byte
}

// Dependency MUST retain the effective name and artifact-relative origin with its bytes.
type Dependency struct {
	Declaration chart.Dependency
	Name        string
	Path        string
	Capture     Capture
}

// Remote classifies the declaration even when local path resolution fails.
func (d *Dependency) Remote() bool {
	repository := d.Declaration.Repository
	return repository != "" && !strings.HasPrefix(repository, "file://")
}

// Description MUST preserve existing children while adding only selected missing dependencies.
type Description struct {
	Parent       Capture
	Dependencies []Dependency
	Chart        *chart.Chart
}

// Load MUST preserve Helm diagnostics for the chart owner to classify.
//
//nolint:wrapcheck // The chart owner adds the structural error category without changing Helm diagnostics.
func (c Capture) Load() (*chart.Chart, error) {
	if c.Files != nil {
		return loader.LoadFiles(c.Files)
	}
	return loader.LoadArchive(bytes.NewReader(c.Archive))
}

// Options MUST retain the artifact-relative location and its owner's path boundary.
// Filesystem calls make Options escape as an aggregate; visitors MUST stay separate to keep their state on the stack.
type Options struct {
	Name     string
	Archive  bool
	Validate func(string) error
	Parent   []*archive.BufferedFile
}

// Describe retains partial dependency coordinates when reconciliation must report a read failure.
func Describe(
	ctx context.Context,
	input fs.FS,
	options Options,
	visit func(string, fs.FileInfo, []byte),
) (Description, bool, error) {
	var parent Capture
	var directory, missing bool
	var err error
	if options.Parent != nil {
		parent.Files = options.Parent
		directory = true
	} else {
		parent, directory, missing, err = CaptureAt(ctx, input, options, visit)
	}
	d := Description{Parent: parent}
	if err != nil {
		return d, missing, err
	}
	if !directory || options.Archive || !mayDeclareDependencies(parent.Files) {
		return d, false, nil
	}
	if visit != nil {
		full := options
		full.Parent = nil
		parent, _, _, err = CaptureAt(ctx, input, full, nil)
		if err != nil {
			return d, false, err
		}
		d.Parent = parent
	}
	d.Chart, err = parent.Load()
	if err != nil {
		// Invalid metadata MUST retain lazy decoding and its chart-owner diagnostics.
		d.Chart = nil
		//nolint:nilerr // The chart owner must classify the eventual decode failure.
		return d, false, nil
	}
	d.Dependencies = missingDependencies(d.Chart)
	return d.complete(ctx, input, options)
}

// Complete MUST recapture sibling bytes while retaining the parent's immutable selection.
func (d *Description) Complete(ctx context.Context, input fs.FS, options Options) (Description, bool, error) {
	completed := *d
	completed.Dependencies = slices.Clone(d.Dependencies)
	return completed.complete(ctx, input, options)
}

func (d *Description) complete(ctx context.Context, input fs.FS, options Options) (Description, bool, error) {
	var firstError error
	for i := range d.Dependencies {
		dep := &d.Dependencies[i]
		if dep.Remote() {
			continue
		}
		dep.Capture = Capture{}
		captureErr := dep.captureLocal(ctx, input, options)
		if firstError == nil {
			firstError = captureErr
		}
	}
	return *d, false, firstError
}

func isMetadata(name string) bool {
	switch name {
	case "Chart.yaml", "Chart.lock", "requirements.yaml", "requirements.lock":
		return true
	default:
		return false
	}
}

func mayDeclareDependencies(files []*archive.BufferedFile) bool {
	for _, file := range files {
		if !isMetadata(file.Name) {
			continue
		}
		// Encoded keys MUST reach the YAML decoder; this is only a negative byte filter.
		for _, b := range file.Data {
			if b >= utf8.RuneSelf || b == '\\' || b == '!' {
				return true
			}
		}
		for i := 0; i+len("dependencies") <= len(file.Data); i++ {
			if bytes.EqualFold(file.Data[i:i+len("dependencies")], []byte("dependencies")) {
				return true
			}
		}
	}
	return false
}

func missingDependencies(loaded *chart.Chart) []Dependency {
	requirements := loaded.Metadata.Dependencies
	if loaded.Lock != nil {
		requirements = loaded.Lock.Dependencies
	}
	current := loaded.Dependencies()
	// source-controller's equal-count shortcut deliberately ignores dependency names.
	if len(current) == len(requirements) {
		return nil
	}
	missing := map[string]*chart.Dependency{}
	for _, dep := range requirements {
		name := dep.Name
		if dep.Alias != "" {
			name = dep.Alias
		}
		if !slices.ContainsFunc(current, func(c *chart.Chart) bool { return c.Name() == name }) {
			missing[name] = dep
		}
	}
	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	slices.Sort(names)
	dependencies := make([]Dependency, 0, len(names))
	for _, name := range names {
		dependencies = append(dependencies, Dependency{Declaration: *missing[name], Name: name})
	}
	return dependencies
}

func (d *Dependency) captureLocal(ctx context.Context, input fs.FS, options Options) error {
	parent := options.Name
	dep := &d.Declaration
	local := path.Join(parent, "charts", dep.Name)
	if dep.Repository != "" {
		u, err := url.Parse(dep.Repository)
		if err != nil {
			return fmt.Errorf("dependency %s repository: %w", d.Name, err)
		}
		local = path.Join(parent, u.Host, u.Path)
	}
	d.Path = local
	dependencyError := func(err error) error {
		return fmt.Errorf("dependency %s (%s): %w", d.Name, dep.Repository, err)
	}
	if err := options.Validate(local); err != nil {
		return dependencyError(err)
	}
	// Flux archives omit symlinks. Inspect entries before any lookup can follow a link.
	for dir, remaining := ".", local; remaining != "" && remaining != "."; {
		part, rest, _ := strings.Cut(remaining, "/")
		entries, err := fs.ReadDir(input, dir)
		if err != nil {
			return dependencyError(err)
		}
		i, found := slices.BinarySearchFunc(entries, part, func(entry fs.DirEntry, name string) int {
			return strings.Compare(entry.Name(), name)
		})
		if !found || entries[i].Type()&fs.ModeSymlink != 0 {
			return dependencyError(&fs.PathError{Op: "stat", Path: local, Err: fs.ErrNotExist})
		}
		dir, remaining = path.Join(dir, part), rest
	}
	child, _, _, err := CaptureAt(ctx, input, Options{Name: local}, nil)
	if err != nil {
		return fmt.Errorf("dependency %s at %s (%s): %w", d.Name, local, dep.Repository, err)
	}
	constraint, err := semver.NewConstraint(dep.Version)
	if err != nil {
		return fmt.Errorf("dependency %s constraint %s: %w", d.Name, dep.Version, err)
	}
	ch, err := child.Load()
	if err != nil {
		return fmt.Errorf("dependency %s at %s: %w", d.Name, local, err)
	}
	version, err := semver.NewVersion(ch.Metadata.Version)
	if err != nil {
		return fmt.Errorf("dependency %s version: %w", d.Name, err)
	}
	if !constraint.Check(version) {
		return fmt.Errorf(
			"dependency %s at %s: version %s does not match constraint %s",
			d.Name,
			local,
			version,
			dep.Version,
		)
	}
	d.Capture = child
	return nil
}

// CaptureAt MUST retain partial coordinates independently of payload readability.
//
//nolint:wrapcheck,gocritic // The chart owner classifies filesystem diagnostics; the booleans distinguish shape and absence.
func CaptureAt(
	ctx context.Context,
	input fs.FS,
	options Options,
	visit func(string, fs.FileInfo, []byte),
) (Capture, bool, bool, error) {
	name, archiveShape := options.Name, options.Archive
	if err := ctx.Err(); err != nil {
		return Capture{}, false, false, err
	}
	info, err := fs.Stat(input, name)
	if err != nil {
		return Capture{}, false, errors.Is(err, fs.ErrNotExist), err
	}
	if info.IsDir() {
		sub, subErr := fs.Sub(input, name)
		if subErr != nil {
			return Capture{}, true, false, subErr
		}
		files, captureErr := filesInFS(ctx, sub, archiveShape, visit)
		return Capture{Files: files}, true, false, captureErr
	}
	if visit != nil && info.Mode().IsRegular() {
		return Capture{Archive: []byte{}}, false, false, nil
	}
	reader, err := input.Open(name)
	if err != nil {
		return Capture{}, false, false, err
	}
	data, readErr := io.ReadAll(reader)
	return Capture{Archive: data}, false, false, errors.Join(readErr, reader.Close())
}

// Files MUST apply the same Helm file policy to reader discovery and chart capture.
func Files(ctx context.Context, input fs.FS, archiveShape bool) ([]*archive.BufferedFile, error) {
	return filesInFS(ctx, input, archiveShape, nil)
}

func filesInFS(
	ctx context.Context,
	input fs.FS,
	archiveShape bool,
	projection func(string, fs.FileInfo, []byte),
) ([]*archive.BufferedFile, error) {
	files := []*archive.BufferedFile{}
	remaining := archive.MaxDecompressedChartSize
	var firstError error
	visit := func(name string, info fs.FileInfo, data []byte) error {
		if projection != nil {
			projection(name, info, data)
			if info.Size() == 0 || !isMetadata(name) {
				return nil
			}
		}
		rooted := name
		if archiveShape {
			var err error
			rooted, err = archiveFileName(name)
			if err != nil {
				return err
			}
		}
		file := &archive.BufferedFile{Name: rooted, ModTime: info.ModTime()}
		files = append(files, file)
		// Failed capture MUST retain admitted coordinates for change detection without reading more payloads.
		if firstError != nil {
			//nolint:nilerr // Capture MUST retain all coordinates before returning its read failure.
			return nil
		}
		data, firstError = readFile(input, name, data, remaining)
		remaining -= int64(len(data))
		if remaining < 0 || archiveShape && remaining == 0 {
			firstError = fmt.Errorf("chart exceeds maximum decompressed size")
		}
		file.Data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
		return nil
	}
	err := Walk(ctx, input, !archiveShape, visit)
	if firstError != nil {
		return files, firstError
	}
	return files, err
}

//nolint:wrapcheck // The chart owner retains filesystem causes and Helm-compatible diagnostics.
func readFile(input fs.FS, name string, data []byte, remaining int64) ([]byte, error) {
	if data != nil {
		return data, nil
	}
	reader, err := input.Open(name)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, remaining+1))
	return data, errors.Join(readErr, reader.Close())
}

func archiveFileName(name string) (string, error) {
	delimiter := "/"
	if strings.ContainsRune(name, '\\') {
		delimiter = "\\"
	}
	base, relative, _ := strings.Cut(name, delimiter)
	relative = strings.ReplaceAll(relative, delimiter, "/")
	if path.IsAbs(relative) {
		return "", errors.New("chart illegally contains absolute paths")
	}
	relative = path.Clean(relative)
	if relative == "." {
		return "", fmt.Errorf("chart illegally contains content outside the base directory: %q", name)
	}
	if strings.HasPrefix(relative, "..") {
		return "", errors.New("chart illegally references parent directory")
	}
	if len(relative) >= 3 && relative[1:3] == ":/" &&
		(relative[0] >= 'a' && relative[0] <= 'z' || relative[0] >= 'A' && relative[0] <= 'Z') {
		return "", errors.New("chart contains illegally named files")
	}
	if base == "Chart.yaml" {
		return "", errors.New("chart yaml not in base directory")
	}
	return relative, nil
}
