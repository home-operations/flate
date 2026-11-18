package diff

import "github.com/home-operations/flate/pkg/manifest"

type prepared struct {
	Subjects                [2]map[manifest.NamedResource]manifest.DiagnosticIdentity
	Pairs                   []pair
	Warnings                []manifest.Warning
	leftCopied, rightCopied bool
}
