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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
	"io"
	"reflect"
	"strings"
)

type Connector struct {
	Cluster  *gocql.ClusterConfig
	Session  *gocql.Session
	keyspace string
}

func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.upsertInternal(ctx, ei, values, true)
}

func (c *Connector) Read(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue, fieldsToRead []string) (values map[string]dosa.FieldValue, err error) {
	b := NewSelectBuilder(ei.Def)
	b.Project(fieldsToRead)
	b.WhereEquals(keys)



	var stmt bytes.Buffer
	stmt.WriteString("select ")
	queryResults := makeQueryResults(&stmt, ei, fieldsToRead)
	fmt.Fprintf(&stmt, ` from "%s" where `, ei.Def.Name)
	var bound []interface{}
	needAnd := false
	for field, value := range keys {
		if needAnd {
			stmt.WriteString(" and ")
		}
		needAnd = true
		fmt.Fprintf(&stmt, `"%s" = ?`, field)
		bound = append(bound, value)
	}

	err = c.Session.Query(stmt.String(), bound...).WithContext(ctx).Scan(queryResults...)
	if err != nil {
		return nil, err
	}
	values = map[string]dosa.FieldValue{}
	for inx, field := range fieldsToRead {
		values[field] = dosa.FieldValue(reflect.ValueOf(queryResults[inx]).Elem().Interface())
	}
	return values, err
}

func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) (results []*dosa.FieldValuesOrError, err error) {
	panic("not implemented")
}

func (c *Connector) Upsert(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.upsertInternal(ctx, ei, values, false)
}
func (c *Connector) upsertInternal(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue, dolwt bool) error {
	var stmt bytes.Buffer
	var bound []interface{}

	if len(values) == 0 {
		// no values specified is a no-op
		return nil
	}

	for name, value := range values {
		if stmt.Len() == 0 {
			fmt.Fprintf(&stmt, "insert into %s (", ei.Def.Name)
		} else {
			stmt.WriteByte(',')
		}
		fmt.Fprintf(&stmt, `"%s"`, name)
		if uuid, ok := value.(dosa.UUID); ok {
			value = string(uuid)
		}
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

	if dolwt {
		stmt.WriteString(" IF NOT EXISTS")
	}

	err := c.Session.Query(stmt.String(), bound...).WithContext(ctx).Exec()
	return err
}

func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func (c *Connector) Remove(ctx context.Context, ei *dosa.EntityInfo, keys map[string]dosa.FieldValue) error {
	var stmt bytes.Buffer
	var bound []interface{}

	for name, value := range keys {
		if stmt.Len() == 0 {
			fmt.Fprintf(&stmt, "delete from %s where ", ei.Def.Name)
		} else {
			stmt.WriteString(" AND ")
		}
		fmt.Fprintf(&stmt, `"%s"=?`, name)
		if uuid, ok := value.(dosa.UUID); ok {
			value = string(uuid)
		}
		bound = append(bound, value)
	}

	err := c.Session.Query(stmt.String(), bound...).WithContext(ctx).Exec()
	return err
}

func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}
func makeQueryResults(stmt io.Writer, ei *dosa.EntityInfo, fieldsToRead []string) []interface{} {
	var queryResults = make([]interface{}, len(fieldsToRead))
	for inx, field := range fieldsToRead {

		if inx > 0 {
			stmt.Write([]byte{','})
		}
		fmt.Fprintf(stmt, `"%s"`, field)
		coldef := ei.Def.FindColumnDefinition(field)
		switch coldef.Type {
		case dosa.Bool:
			queryResults[inx] = reflect.New(reflect.TypeOf(false)).Interface()
		case dosa.Int32:
			queryResults[inx] = reflect.New(reflect.TypeOf(int32(0))).Interface()
		case dosa.Int64:
			queryResults[inx] = reflect.New(reflect.TypeOf(int64(0))).Interface()
		default:
			panic(fmt.Sprintf("FIXME not implemented %v", coldef.Type)) // FIXME
		}
	}
	return queryResults
}

func (c *Connector) Range(ctx context.Context, ei *dosa.EntityInfo, columnConditions map[string][]*dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	var stmt bytes.Buffer
	stmt.WriteString("select ")
	queryResults := makeQueryResults(&stmt, ei, fieldsToRead)

	fmt.Fprintf(&stmt, ` from "%s" where `, ei.Def.Name)
	var bound []interface{}
	needAnd := false
	for field, conds := range columnConditions {
		if needAnd {
			stmt.WriteString(" and ")
		}
		needAnd = true
		for _, cond := range conds {
			fmt.Fprintf(&stmt, `"%s"`, field)
			switch cond.Op {
			case dosa.Eq:
				fmt.Fprintf(&stmt, "=?")
			case dosa.Gt:
				fmt.Fprintf(&stmt, ">?")
			case dosa.Lt:
				fmt.Fprintf(&stmt, "<?")
			case dosa.LtOrEq:
				fmt.Fprintf(&stmt, "<=?")
			case dosa.GtOrEq:
				fmt.Fprintf(&stmt, ">=?")
			default:
				panic("Invalid operator")
			}
			bound = append(bound, cond.Value)
		}
	}

	q := c.Session.Query(stmt.String(), bound...).WithContext(ctx).PageSize(limit)
	if token != "" {
		data, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return nil, "", err
		}
		q = q.PageState(data)
	}
	iter := q.Iter()
	err := iter.Scan(queryResults...)
	if err != nil {
		return nil, "", err
	}
	values := map[string]dosa.FieldValue{}
	for inx, field := range fieldsToRead {
		values[field] = dosa.FieldValue(reflect.ValueOf(queryResults[inx]).Elem().Interface())
	}

	nextToken := base64.StdEncoding.EncodeToString(iter.PageState())

	return values, nextToken, err
	// func (iter *Iter) PageState() []byte
	// func (q *Query) PageState(state []byte) *Query
}

func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

func (c *Connector) CreateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func (c *Connector) TruncateScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

func (c *Connector) DropScope(ctx context.Context, scope string) error {
	panic("not implemented")
}

type RepairableSchemaMismatchError struct {
	MissingColumns []MissingColumn
	MissingTables  []string
}
type MissingColumn struct {
	Column    dosa.ColumnDefinition
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
func (c *Connector) CheckSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (versions []int32, err error) {
	schemaErrors := new(RepairableSchemaMismatchError)

	// TODO: unfortunately, gocql doesn't have a way to pass the context to this operation :(
	km, err := c.Session.KeyspaceMetadata(c.keyspace)
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

func (c *Connector) UpsertSchema(ctx context.Context, scope string, namePrefix string, ed []*dosa.EntityDefinition) (versions []int32, err error) {
	sr, err := c.CheckSchema(ctx, scope, namePrefix, ed)
	if _, ok := err.(*RepairableSchemaMismatchError); ok {
		// TODO: this is recoverable by adding the columns/tables in the error message
		return nil, err
	}
	return sr, err
}

const (
	defaultContactPoints = "127.0.0.1"
	defaultKeyspace      = "test"
	defaultConsistency   = gocql.LocalQuorum
)

func init() {
	dosa.RegisterConnector("cassandra", func(opts map[string]interface{}) (dosa.Connector, error) {
		c := new(Connector)
		contactPoints := defaultContactPoints
		if ival, ok := opts["contact_points"]; ok {
			contactPoints = ival.(string)
		}
		c.Cluster = gocql.NewCluster(strings.Split(contactPoints, ",")...)

		c.keyspace = defaultKeyspace
		if ival, ok := opts["keyspace"]; ok {
			c.keyspace = ival.(string)
		}
		c.Cluster.Keyspace = c.keyspace
		// TODO: support consistency level overrides
		c.Cluster.Consistency = defaultConsistency

		c.Cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
		var err error
		c.Session, err = c.Cluster.CreateSession()
		if err != nil {
			return nil, err
		}
		return c, nil
	})
}
func (c *Connector) ScopeExists(ctx context.Context, scope string) (bool, error) {
	panic("not implemented")
}

func (c *Connector) Shutdown() error {
	panic("not implemented")
}
