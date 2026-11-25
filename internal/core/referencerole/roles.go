// Package referencerole gives the input cache access to private manifest reference roles.
package referencerole

// Roles packs the private source facts into a cache byte.
type Roles uint8

// BindSource and ReadSourceTree represent independent source roles.
const (
	BindSource Roles = 1 << iota
	ReadSourceTree
)

type reference interface {
	referenceRoles() *Roles
}

func (r *Roles) referenceRoles() *Roles { return r }

// Capture reads stored bits because artifact references can derive roles from other fields.
func Capture(ref reference) Roles { return *ref.referenceRoles() }

// Restore requires a reference that the caller has not published.
func Restore(ref reference, roles Roles) { *ref.referenceRoles() = roles }
