package loader

import "github.com/home-operations/flate/internal/selection"

func retainedErrorsForTest(l *Loader) map[string]error {
	errors := map[string]error{}
	for file := range l.documents.cache {
		doc := l.documents.cache[file]
		if doc.selected || doc.eligible == selection.RootInactive {
			continue
		}
		for i := range doc.objects {
			child := doc.objects[i]
			if child.outcome.Err != nil {
				errors[l.fileCoordinate(new(l.documents).localPath(file))] = child.outcome.Err
				break
			}
		}
	}
	return errors
}
