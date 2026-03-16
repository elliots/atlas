// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.
//
// Modifications Copyright 2026 Elliot Shepherd

//go:build !ent

package sqlx

import (
	"fmt"
	"slices"

	"ariga.io/atlas/sql/schema"
)

// sortViewChanges sorts view changes by their dependencies using topological sort.
func sortViewChanges(changes []schema.Change) ([]schema.Change, error) {
	if len(changes) <= 1 {
		return changes, nil
	}
	// Build a map of view name → change for lookup.
	byName := make(map[string]schema.Change, len(changes))
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.AddView:
			byName[c.V.Name] = c
		case *schema.DropView:
			byName[c.V.Name] = c
		case *schema.ModifyView:
			byName[c.To.Name] = c
		}
	}
	// deps[c] = list of changes that must come before c.
	deps := make(map[schema.Change][]schema.Change)
	for _, c := range changes {
		var viewDeps []schema.Object
		switch c := c.(type) {
		case *schema.AddView:
			viewDeps = c.V.Deps
		case *schema.ModifyView:
			viewDeps = c.To.Deps
		case *schema.DropView:
			// Drop order is reversed: if v1 depends on v2, drop v1 first.
			for _, other := range changes {
				if other == c {
					continue
				}
				if drop, ok := other.(*schema.DropView); ok {
					for _, d := range drop.V.Deps {
						if v, ok := d.(*schema.View); ok && v.Name == c.V.Name {
							deps[c] = append(deps[c], other)
						}
					}
				}
			}
			continue
		}
		for _, d := range viewDeps {
			if v, ok := d.(*schema.View); ok {
				if dep, ok := byName[v.Name]; ok && dep != c {
					deps[c] = append(deps[c], dep)
				}
			}
		}
	}
	// Kahn's topological sort.
	inDeg := make(map[schema.Change]int, len(changes))
	for _, c := range changes {
		inDeg[c] = len(deps[c])
	}
	var queue []schema.Change
	for _, c := range changes {
		if inDeg[c] == 0 {
			queue = append(queue, c)
		}
	}
	sorted := make([]schema.Change, 0, len(changes))
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		sorted = append(sorted, c)
		for other, ds := range deps {
			if slices.Contains(ds, c) {
				inDeg[other]--
				if inDeg[other] == 0 {
					queue = append(queue, other)
				}
			}
		}
	}
	if len(sorted) != len(changes) {
		return nil, fmt.Errorf("cycle detected in view dependencies")
	}
	return sorted, nil
}

func (*Diff) triggerDiff(from, to interface {
	Trigger(string) (*schema.Trigger, bool)
}, fromT, toT []*schema.Trigger, opts *schema.DiffOptions) ([]schema.Change, error) {
	var changes []schema.Change
	// Drop or modify triggers.
	for _, t1 := range fromT {
		t2, ok := to.Trigger(t1.Name)
		if !ok {
			changes = append(changes, &schema.DropTrigger{T: t1})
			continue
		}
		if BodyDefChanged(t1.Body, t2.Body) {
			changes = append(changes, &schema.ModifyTrigger{From: t1, To: t2})
		}
	}
	// Add triggers.
	for _, t1 := range toT {
		if _, ok := from.Trigger(t1.Name); !ok {
			changes = append(changes, &schema.AddTrigger{T: t1})
		}
	}
	return changes, nil
}

// funcDep returns true if f1 depends on f2.
func funcDep(_, _ *schema.Func, _ SortOptions) bool {
	return false // unimplemented.
}

// procDep returns true if p1 depends on p2.
func procDep(_, _ *schema.Proc, _ SortOptions) bool {
	return false // unimplemented.
}

func tableDepFunc(*schema.Table, *schema.Func, SortOptions) bool {
	return false // unimplemented.
}

// askForColumns detects column renames by matching DropColumn+AddColumn pairs
// with compatible types. Requires DiffOptions.AskFunc to confirm interactively;
// without it, changes pass through unchanged (safe default for CI/non-interactive).
func (*Diff) askForColumns(from *schema.Table, changes []schema.Change, opts *schema.DiffOptions) ([]schema.Change, error) {
	if opts == nil || opts.AskFunc == nil {
		return changes, nil
	}
	var drops []*schema.DropColumn
	var adds []*schema.AddColumn
	var other []schema.Change
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.DropColumn:
			drops = append(drops, c)
		case *schema.AddColumn:
			adds = append(adds, c)
		default:
			other = append(other, c)
		}
	}
	if len(drops) == 0 || len(adds) == 0 {
		return changes, nil
	}
	usedDrops := make(map[int]bool)
	usedAdds := make(map[int]bool)
	var renames []schema.Change
	for i, drop := range drops {
		if usedDrops[i] {
			continue
		}
		for j, add := range adds {
			if usedAdds[j] {
				continue
			}
			if !columnsTypeEqual(drop.C, add.C) {
				continue
			}
			ans, err := opts.AskFunc(
				fmt.Sprintf("Did you rename column %q to %q on table %q?", drop.C.Name, add.C.Name, from.Name),
				[]string{"Yes", "No"},
			)
			if err != nil {
				return nil, err
			}
			if ans != "Yes" {
				continue
			}
			renames = append(renames, &schema.RenameColumn{From: drop.C, To: add.C})
			usedDrops[i] = true
			usedAdds[j] = true
			break
		}
	}
	if len(renames) == 0 {
		return changes, nil
	}
	// Rebuild: other changes + unmatched drops + renames + unmatched adds.
	result := make([]schema.Change, 0, len(changes))
	result = append(result, other...)
	for i, d := range drops {
		if !usedDrops[i] {
			result = append(result, d)
		}
	}
	result = append(result, renames...)
	for j, a := range adds {
		if !usedAdds[j] {
			result = append(result, a)
		}
	}
	return result, nil
}

// columnsTypeEqual reports whether two columns have the same type.
func columnsTypeEqual(c1, c2 *schema.Column) bool {
	if c1.Type == nil || c2.Type == nil {
		return c1.Type == c2.Type
	}
	return c1.Type.Raw == c2.Type.Raw
}

// askForIndexes detects index renames by matching DropIndex+AddIndex pairs
// with the same column set. Requires DiffOptions.AskFunc to confirm interactively;
// without it, changes pass through unchanged (safe default for CI/non-interactive).
func (*Diff) askForIndexes(tableName string, changes []schema.Change, opts *schema.DiffOptions) ([]schema.Change, error) {
	if opts == nil || opts.AskFunc == nil {
		return changes, nil
	}
	var drops []*schema.DropIndex
	var adds []*schema.AddIndex
	var other []schema.Change
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.DropIndex:
			drops = append(drops, c)
		case *schema.AddIndex:
			adds = append(adds, c)
		default:
			other = append(other, c)
		}
	}
	if len(drops) == 0 || len(adds) == 0 {
		return changes, nil
	}
	usedDrops := make(map[int]bool)
	usedAdds := make(map[int]bool)
	var renames []schema.Change
	for i, drop := range drops {
		if usedDrops[i] {
			continue
		}
		for j, add := range adds {
			if usedAdds[j] {
				continue
			}
			if !indexPartsEqual(drop.I, add.I) {
				continue
			}
			ans, err := opts.AskFunc(
				fmt.Sprintf("Did you rename index %q to %q on table %q?", drop.I.Name, add.I.Name, tableName),
				[]string{"Yes", "No"},
			)
			if err != nil {
				return nil, err
			}
			if ans != "Yes" {
				continue
			}
			renames = append(renames, &schema.RenameIndex{From: drop.I, To: add.I})
			usedDrops[i] = true
			usedAdds[j] = true
			break
		}
	}
	if len(renames) == 0 {
		return changes, nil
	}
	result := make([]schema.Change, 0, len(changes))
	result = append(result, other...)
	for i, d := range drops {
		if !usedDrops[i] {
			result = append(result, d)
		}
	}
	result = append(result, renames...)
	for j, a := range adds {
		if !usedAdds[j] {
			result = append(result, a)
		}
	}
	return result, nil
}

// indexPartsEqual reports whether two indexes cover the same columns
// in the same order with the same uniqueness.
func indexPartsEqual(i1, i2 *schema.Index) bool {
	if i1.Unique != i2.Unique || len(i1.Parts) != len(i2.Parts) {
		return false
	}
	for k, p1 := range i1.Parts {
		p2 := i2.Parts[k]
		switch {
		case p1.C != nil && p2.C != nil:
			if p1.C.Name != p2.C.Name {
				return false
			}
		case p1.X != nil && p2.X != nil:
			if p1.X.(*schema.RawExpr).X != p2.X.(*schema.RawExpr).X {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// fixRenames is a no-op in OSS. Table rename detection requires interactive
// confirmation (AskFunc) which is not available at this call site.
// The enterprise version handles this with interactive prompts.
func (*Diff) fixRenames(changes schema.Changes) schema.Changes {
	return changes
}

// dependsOn reports if the given change depends on the other change.
func dependsOn(c1, c2 schema.Change, _ SortOptions) bool {
	if dependOnOf(c1, c2) {
		return true
	}
	switch c1 := c1.(type) {
	case *schema.DropSchema:
		switch c2 := c2.(type) {
		case *schema.DropTable:
			// Schema must be dropped after all its tables and references to them.
			return SameSchema(c1.S, c2.T.Schema) || slices.ContainsFunc(c2.T.ForeignKeys, func(fk *schema.ForeignKey) bool {
				return SameSchema(c1.S, fk.RefTable.Schema)
			})
		case *schema.ModifyTable:
			return SameSchema(c1.S, c2.T.Schema) || slices.ContainsFunc(c2.Changes, func(c schema.Change) bool {
				fk, ok := c.(*schema.DropForeignKey)
				return ok && SameSchema(c1.S, fk.F.RefTable.Schema)
			})
		}
	case *schema.AddTable:
		switch c2 := c2.(type) {
		case *schema.AddSchema:
			return c1.T.Schema.Name == c2.S.Name
		case *schema.DropTable:
			// Table recreation.
			return c1.T.Name == c2.T.Name && SameSchema(c1.T.Schema, c2.T.Schema)
		case *schema.AddTable:
			if refTo(c1.T.ForeignKeys, c2.T) {
				return true
			}
			if slices.ContainsFunc(c1.T.Columns, func(c *schema.Column) bool {
				return c.Type != nil && typeDependsOnT(c.Type.Type, c2.T)
			}) {
				return true
			}
		case *schema.ModifyTable:
			if (c1.T.Name != c2.T.Name || !SameSchema(c1.T.Schema, c2.T.Schema)) && refTo(c1.T.ForeignKeys, c2.T) {
				return true
			}
		case *schema.AddObject:
			t, ok := c2.O.(schema.Type)
			if ok && slices.ContainsFunc(c1.T.Columns, func(c *schema.Column) bool {
				return schema.IsType(c.Type.Type, t)
			}) {
				return true
			}
		}
		return depOfAdd(c1.T.Deps, c2)
	case *schema.DropTable:
		// If it is a drop of a table, the change must occur
		// after all resources that rely on it will be dropped.
		switch c2 := c2.(type) {
		case *schema.DropTable:
			// References to this table, must be dropped first.
			if refTo(c2.T.ForeignKeys, c1.T) {
				return true
			}
			if slices.ContainsFunc(c2.T.Columns, func(c *schema.Column) bool {
				return c.Type != nil && typeDependsOnT(c.Type.Type, c1.T)
			}) {
				return true
			}
		case *schema.ModifyTable:
			if slices.ContainsFunc(c2.Changes, func(c schema.Change) bool {
				switch c := c.(type) {
				case *schema.DropForeignKey:
					return refTo([]*schema.ForeignKey{c.F}, c1.T)
				case *schema.DropColumn:
					return c.C.Type != nil && typeDependsOnT(c.C.Type.Type, c1.T)
				}
				return false
			}) {
				return true
			}
		}
		return depOfDrop(c1.T, c2)
	case *schema.ModifyTable:
		switch c2 := c2.(type) {
		case *schema.AddTable:
			// Table modification relies on its creation.
			if c1.T.Name == c2.T.Name && SameSchema(c1.T.Schema, c2.T.Schema) {
				return true
			}
			// Tables need to be created before referencing them.
			if slices.ContainsFunc(c1.Changes, func(c schema.Change) bool {
				switch c := c.(type) {
				case *schema.AddForeignKey:
					return refTo([]*schema.ForeignKey{c.F}, c2.T)
				case *schema.AddColumn:
					return c.C.Type != nil && typeDependsOnT(c.C.Type.Type, c2.T)
				case *schema.ModifyColumn:
					return c.To.Type != nil && typeDependsOnT(c.To.Type.Type, c2.T)
				}
				return false
			}) {
				return true
			}
		case *schema.ModifyTable:
			if c1.T != c2.T {
				addC := make(map[*schema.Column]bool)
				for _, c := range c2.Changes {
					if add, ok := c.(*schema.AddColumn); ok {
						addC[add.C] = true
					}
				}
				return slices.ContainsFunc(c1.Changes, func(c schema.Change) bool {
					fk, ok := c.(*schema.AddForeignKey)
					return ok && refTo([]*schema.ForeignKey{fk.F}, c2.T) && slices.ContainsFunc(fk.F.Columns, func(c *schema.Column) bool { return addC[c] })
				})
			}
		case *schema.AddObject:
			t, ok := c2.O.(schema.Type)
			if ok && slices.ContainsFunc(c1.Changes, func(c schema.Change) bool {
				switch c := c.(type) {
				case *schema.AddColumn:
					return schema.IsType(c.C.Type.Type, t)
				case *schema.ModifyColumn:
					return schema.IsType(c.To.Type.Type, t)
				default:
					return false
				}
			}) {
				return true
			}
		}
		return depOfAdd(c1.T.Deps, c2)
	case *schema.AddView:
		return depOfAdd(c1.V.Deps, c2)
	case *schema.DropView:
		return depOfDrop(c1.V, c2)
	case *schema.AddFunc:
		return depOfAdd(c1.F.Deps, c2)
	case *schema.DropFunc:
		return depOfDrop(c1.F, c2)
	case *schema.AddProc:
		return depOfAdd(c1.P.Deps, c2)
	case *schema.DropProc:
		return depOfDrop(c1.P, c2)
	case *schema.AddTrigger:
		return depOfAdd(c1.T.Deps, c2)
	case *schema.DropTrigger:
		return depOfDrop(c1.T, c2)
	case *schema.DropObject:
		t, ok := c1.O.(schema.Type)
		if !ok {
			return false
		}
		// Dropping a type must occur after all its usage were dropped.
		switch c2 := c2.(type) {
		case *schema.DropTable:
			// Dropping a table also drops its triggers and might depend on the type.
			if slices.ContainsFunc(c2.T.Triggers, func(tg *schema.Trigger) bool {
				return slices.Contains(tg.Deps, c1.O)
			}) {
				return true
			}
			if slices.ContainsFunc(c2.T.Columns, func(c *schema.Column) bool {
				return schema.IsType(c.Type.Type, t)
			}) {
				return true
			}
		case *schema.ModifyTable:
			return slices.ContainsFunc(c2.Changes, func(c schema.Change) bool {
				d, ok := c.(*schema.DropColumn)
				return ok && schema.IsType(d.C.Type.Type, t)
			})
		}
	}
	return false
}
