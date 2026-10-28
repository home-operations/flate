package loader

func retainedErrorsForTest(l *Loader) map[string]error {
	errors := map[string]error{}
	for file, doc := range l.documents.cache {
		if doc.selected || doc.eligible == fileInactive {
			continue
		}
		for _, child := range doc.objects {
			if child.outcome.Err != nil {
				errors[l.fileCoordinate(l.documents.localPath(file))] = child.outcome.Err
				break
			}
		}
	}
	return errors
}
