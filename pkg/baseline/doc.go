// Package baseline resolves one base revision and admits lazy input views.
// Metadata and missing changed bodies use the shared cache owner, while
// unchanged baseline bodies reuse admitted current bytes. The checkout and
// its Git object store are read-only throughout selection and rendering.
package baseline
