package cli

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
)

func bindOutput[F ~string](fs *pflag.FlagSet, target *string, values ...F) {
	formats := make([]output.Format, len(values))
	for i, value := range values {
		formats[i] = output.Format(value)
	}
	*target = string(formats[0])
	fs.VarP(&outputValue{target: target, allowed: formats}, "output", "o", "Emit: "+joinOutputs(formats))
}

func joinOutputs(formats []output.Format) string {
	names := make([]string, len(formats))
	for i, format := range formats {
		names[i] = string(format)
	}
	return strings.Join(names, ", ")
}

type outputValue struct {
	target  *string
	allowed []output.Format
}

func (o *outputValue) String() string { return *o.target }
func (o *outputValue) Type() string   { return "string" }

func (o *outputValue) Set(v string) error {
	if slices.Contains(o.allowed, output.Format(v)) {
		*o.target = v
		return nil
	}
	return fmt.Errorf("must be one of: %s", joinOutputs(o.allowed))
}
