// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
)

// CreateScope is not implemented
func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// TruncateScope is not implemented
func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// DropScope is not implemented
func (c *Connector) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

// RepairableSchemaMismatchError is an error describing what can be added to make
// this schema current. It might include a lot of tables or columns
type RepairableSchemaMismatchError struct {
	MissingColumns []MissingColumn
	MissingTables  []string
}

// MissingColumn describes a column that is missing
type MissingColumn struct {
	Column    dosa.ColumnDefinition
	Tablename string
}

// HasMissing returns true if there are missing columns
func (m *RepairableSchemaMismatchError) HasMissing() bool {
	return m.MissingColumns != nil || m.MissingTables != nil
}

// Error prints a human-readable error message describing the first missing table or column
func (m *RepairableSchemaMismatchError) Error() string {
	if m.MissingTables != nil {
		return fmt.Sprintf("Missing %d tables (first is %q)", len(m.MissingTables), m.MissingTables[0])
	}
	return fmt.Sprintf("Missing %d columns (first is %q in table %q)", len(m.MissingColumns), m.MissingColumns[0].Column.Name, m.MissingColumns[0].Tablename)
}

// compareStructToSchema compares a dosa EntityDefinition to the gocql TableMetadata
// There are two main cases, one that we can fix by adding some columns and all the other mismatches that we can't fix
func compareStructToSchema(ed *dosa.EntityDefinition, md *gocql.TableMetadata, schemaErrors *RepairableSchemaMismatchError) error {
	// Check partition keys
	if len(ed.Key.PartitionKeys) != len(md.PartitionKey) {
		return fmt.Errorf("Table %q primary key length mismatch (was %d should be %d)", ed.Name, len(md.PartitionKey), len(ed.Key.PartitionKeys))
	}
	for i, pk := range ed.Key.PartitionKeys {
		if md.PartitionKey[i].Name != pk {
			return fmt.Errorf("Table %q primary key mismatch (should be %q)", ed.Name, ed.Key.PartitionKeys)
		}
	}

	// Check clustering keys
	if len(ed.Key.ClusteringKeys) != len(md.ClusteringColumns) {
		return fmt.Errorf("Table %q clustering key length mismatch (should be %q)", ed.Name, ed.Key.ClusteringKeys)
	}
	for i, ck := range ed.Key.ClusteringKeys {
		if md.ClusteringColumns[i].Name != ck.Name {
			return fmt.Errorf("Table %q clustering key mismatch (column %d should be %q)", ed.Name, i+1, ck.Name)
		}
	}

	// Check each column
	for _, col := range ed.Columns {
		_, ok := md.Columns[col.Name]
		if !ok {
			schemaErrors.MissingColumns = append(schemaErrors.MissingColumns, MissingColumn{Column: *col, Tablename: ed.Name})
		}
		// TODO: check column type
	}

	return nil

}

// CheckSchema verifies that the schema passed in the registered entities matches the database
func (c *Connector) CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (int32, error) {
	schemaErrors := new(RepairableSchemaMismatchError)

	// TODO: unfortunately, gocql doesn't have a way to pass the context to this operation :(
	km, err := c.Session.KeyspaceMetadata(c.keyspace)
	if err != nil {
		return 0, err
	}
	for _, ed := range ed {
		tableMetadata, ok := km.Tables[ed.Name]
		if !ok {
			schemaErrors.MissingTables = append(schemaErrors.MissingTables, ed.Name)
			continue
		}
		if err := compareStructToSchema(ed, tableMetadata, schemaErrors); err != nil {
			return 0, err
		}

	}
	if schemaErrors.HasMissing() {
		return 0, schemaErrors
	}
	return int32(1), nil
}

// UpsertSchema checks the schema and then updates it
func (c *Connector) UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (*dosa.SchemaStatus, error) {
	sr, err := c.CheckSchema(ctx, scope, namePrefix, ed)
	if _, ok := err.(*RepairableSchemaMismatchError); ok {
		// TODO: this is recoverable by adding the columns/tables in the error message
		return nil, err
	}
	return &dosa.SchemaStatus{sr, "ready"}, err
}

// ScopeExists is not implemented
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

// CheckSchemaStatus is not implemented
func (c *Connector) CheckSchemaStatus(context.Context, string, string, int32) (*dosa.SchemaStatus, error) {
	panic("not implemented")
}
