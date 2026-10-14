package gittree

import "io/fs"

type directView struct{ fs.FS }

func (g *directView) Stat(name string) (fs.FileInfo, error)      { return fs.Stat(g.FS, name) }
func (g *directView) Lstat(name string) (fs.FileInfo, error)     { return fs.Lstat(g.FS, name) }
func (g *directView) ReadDir(name string) ([]fs.DirEntry, error) { return fs.ReadDir(g.FS, name) }
func (g *directView) ReadFile(name string) ([]byte, error)       { return fs.ReadFile(g.FS, name) }
func (g *directView) ReadLink(name string) (string, error)       { return fs.ReadLink(g.FS, name) }
func (g *directView) OpenRegular(name string) (fs.File, error)   { return g.Open(name) }
