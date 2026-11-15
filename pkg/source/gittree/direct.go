package gittree

import "io/fs"

type directView struct{ fs.FS }

type scopedView struct {
	directView

	parent *FS
	path   string
}

// Sub admits a subtree while retaining source-owned content provenance.
func Sub(input fs.FS, dir string) (fs.FS, error) {
	g, native := input.(*FS)
	input, err := fs.Sub(input, dir)
	if err != nil || !native {
		return input, err //nolint:wrapcheck // Sub forwards fs.Sub errors and its returned filesystem, including on failure.
	}
	sub := &struct {
		FS
		scopedView

		policyState localPolicy
	}{scopedView: scopedView{directView{input}, g, dir}}
	sub.view = &sub.scopedView
	sub.location = g.location
	sub.snapshot = &sub.policyState
	return &sub.FS, nil
}

func (g *directView) Stat(
	name string,
) (fs.FileInfo, error) {
	//nolint:wrapcheck // Stat forwards fs.Stat PathError fields for the underlying filesystem.
	return fs.Stat(g.FS, name)
}

func (g *directView) Lstat(
	name string,
) (fs.FileInfo, error) {
	//nolint:wrapcheck // Lstat forwards fs.Lstat PathError fields without resolving the final alias.
	return fs.Lstat(g.FS, name)
}

func (g *directView) ReadDir(
	name string,
) ([]fs.DirEntry, error) {
	//nolint:wrapcheck // ReadDir forwards fs.ReadDir entries and PathError fields.
	return fs.ReadDir(g.FS, name)
}

func (g *directView) ReadFile(
	name string,
) ([]byte, error) {
	//nolint:wrapcheck // ReadFile forwards fs.ReadFile bytes and PathError fields.
	return fs.ReadFile(g.FS, name)
}

func (g *directView) ReadLink(
	name string,
) (string, error) {
	//nolint:wrapcheck // ReadLink forwards fs.ReadLink targets and PathError fields.
	return fs.ReadLink(g.FS, name)
}

func (g *directView) OpenRegular(
	name string,
) (fs.File, error) {
	//nolint:wrapcheck // OpenRegular forwards the underlying Open file and PathError fields.
	return g.Open(name)
}
