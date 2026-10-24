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
		return input, err
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

func (g *directView) Stat(name string) (fs.FileInfo, error)      { return fs.Stat(g.FS, name) }
func (g *directView) Lstat(name string) (fs.FileInfo, error)     { return fs.Lstat(g.FS, name) }
func (g *directView) ReadDir(name string) ([]fs.DirEntry, error) { return fs.ReadDir(g.FS, name) }
func (g *directView) ReadFile(name string) ([]byte, error)       { return fs.ReadFile(g.FS, name) }
func (g *directView) ReadLink(name string) (string, error)       { return fs.ReadLink(g.FS, name) }
func (g *directView) OpenRegular(name string) (fs.File, error)   { return g.Open(name) }
