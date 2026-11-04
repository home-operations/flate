package loader

func retainedErrorsForTest(l *Loader) map[string]error {
	errors := map[string]error{}
	for file := range l.documents.cache {
		doc := l.documents.cache[file]
		if doc.selected || doc.eligible == fileInactive {
			continue
		}
		for i := range doc.objects {
			child := doc.objects[i]
			if child.outcome.Err != nil {
				errors[l.fileCoordinate(l.documents.localPath(file))] = child.outcome.Err
				break
			}
		}
	}
	return errors
}
