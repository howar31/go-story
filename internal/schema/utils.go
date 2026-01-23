package schema

import (
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// GetRequestedFields traverses the selection set and returns a map of requested specific fields
func GetRequestedFields(p graphql.ResolveParams) map[string]bool {
	fields := make(map[string]bool)
	fieldASTs := p.Info.FieldASTs
	if len(fieldASTs) == 0 {
		return fields
	}

	for _, field := range fieldASTs {
		if field.SelectionSet == nil {
			continue
		}
		for _, selection := range field.SelectionSet.Selections {
			if f, ok := selection.(*ast.Field); ok {
				fields[f.Name.Value] = true
			}
			// Fragments are ignored for simplicity as per current requirement
		}
	}
	return fields
}
