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
	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
	"context"
	"bytes"
	"fmt"
	"strings"
)

type Connector struct {
	Cluster *gocql.ClusterConfig
	Session *gocql.Session
	keyspace string
}

func (y *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	panic("not implemented")
}

func (y *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (values map[string]dosa.FieldValue, err error) {
	panic("not implemented")
}

func (y *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) (result []*dosa.FieldValuesOrError, err error) {
	panic("not implemented")
}

func (y *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	var stmt bytes.Buffer
	var bound []interface{}

	for name, value := range values {
		if stmt.Len() == 0 {
			fmt.Fprintf(&stmt, "insert into %s (", ei.Def.Name)
		} else {
			stmt.WriteByte(',')
		}
		fmt.Fprintf(&stmt, `"%s"`, name)
		bound = append(bound, value)
	}
	stmt.WriteString(") values (")
	for inx := range bound {
		if inx > 0 {
			stmt.WriteByte(',')
		}
		stmt.WriteByte('?')
	}
	stmt.WriteByte(')')

	err := y.Session.Query(stmt.String(), bound...).WithContext(ctx).Exec()
	return err
}

func (y *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func (y *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	panic("not implemented")
}

func (y *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func (y *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, conditions map[string][]*dosa.Condition, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

func (y *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, FieldNameValuePair dosa.FieldNameValuePair, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

func (y *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

type RepairableSchemaMismatchError struct {
	MissingColumns []MissingColumn
	MissingTables []string
}
type MissingColumn struct {
	Column dosa.ColumnDefinition
	Tablename string
}
func (m *RepairableSchemaMismatchError) HasMissing() bool {
	return m.MissingColumns != nil || m.MissingTables != nil
}
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
			return fmt.Errorf("Table %q clustering key mismatch (column %d should be %q)", ed.Name, i + 1, ck.Name)
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


func (y *Connector) CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (versions []int32, err error) {
	schemaErrors := new(RepairableSchemaMismatchError)

	// TODO: unfortunately, gocql doesn't have a way to pass the context to this operation :(
	km, err := y.Session.KeyspaceMetadata(y.keyspace)
	if err != nil {
		return nil, err
	}
	refs := make([]int32, len(ed))
	for num, ed := range ed {
		tableMetadata, ok := km.Tables[ed.Name]
		if !ok {
			schemaErrors.MissingTables = append(schemaErrors.MissingTables, ed.Name)
			continue
		}
		refs[num] = 1
		if err := compareStructToSchema(ed, tableMetadata, schemaErrors); err != nil {
			return nil, err
		}

	}
	if schemaErrors.HasMissing() {
		return nil, schemaErrors
	}
	return refs, nil
}

func (y *Connector) UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (versions []int32, err error) {
	sr, err := y.CheckSchema(ctx, scope, namePrefix, ed)
	if _, ok := err.(*RepairableSchemaMismatchError); ok {
		// TODO: this is recoverable by adding the columns in the error message
		return nil, err
	}
	return sr, err
}

func (y *Connector) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func (y *Connector) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func (y *Connector) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func (y *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

func (y *Connector) Shutdown() error {
	panic("not implemented")
}

const (
	defaultContactPoints = "127.0.0.1"
	defaultKeyspace = "test"
	defaultConsistency = gocql.LocalQuorum
)

func init() {
	dosa.RegisterConnector("cassandra", func(opts map[string]interface{}) (dosa.Connector, error) {
		c := new(Connector)
		contactPoints := defaultContactPoints
		if _, ok := opts["contact_points"]; ok {
			contactPoints = opts["contact_points"].(string)
		}

		c.Cluster = gocql.NewCluster(strings.Split(contactPoints, ",")...)

		if _, ok := opts["keyspace"]; ok {
			c.keyspace = opts["keyspace"].(string)
		} else {
			c.keyspace = defaultKeyspace
		}
		c.Cluster.Keyspace = c.keyspace
		// TODO: support consistency level overrides
		c.Cluster.Consistency = defaultConsistency
		var err error
		c.Session, err = c.Cluster.CreateSession()
		if err != nil {
			return nil, err
		}
		return c, nil
	})
}