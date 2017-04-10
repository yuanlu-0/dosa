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
	"io"
	"reflect"
	"strings"

	"time"

	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
)

// Connector holds the connection information for this cassandra connector
type Connector struct {
	Cluster  *gocql.ClusterConfig
	Session  *gocql.Session
	keyspace string
}

// CreateIfNotExists implements the interface for gocql
func (c *Connector) CreateIfNotExists(ctx context.Context, ei *dosa.EntityInfo, values map[string]dosa.FieldValue) error {
	return c.upsertInternal(ctx, ei, values, true)
}

// Read implements the interface for gocql
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

// MultiRead is not implemented
func (c *Connector) MultiRead(ctx context.Context, ei *dosa.EntityInfo, keys []map[string]dosa.FieldValue, fieldsToRead []string) (results []*dosa.FieldValuesOrError, err error) {
	panic("not implemented")
}

// Upsert implements the interface for gocql
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

// MultiUpsert is not implemented
func (c *Connector) MultiUpsert(ctx context.Context, ei *dosa.EntityInfo, multiValues []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

// Remove implements the interface for gocql
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

// MultiRemove is not implemented
func (c *Connector) MultiRemove(ctx context.Context, ei *dosa.EntityInfo, multiKeys []map[string]dosa.FieldValue) (result []error, err error) {
	panic("not implemented")
}

func makeQueryResults(stmt io.Writer, ei *dosa.EntityInfo, fieldsToRead []string) []interface{} {
	var queryResults = make([]interface{}, len(fieldsToRead))
	for inx, field := range fieldsToRead {
		if inx > 0 {
			_, _ = stmt.Write([]byte{','})
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
		case dosa.Blob:
			queryResults[inx] = reflect.New(reflect.TypeOf([]byte{})).Interface()
		case dosa.Double:
			queryResults[inx] = reflect.New(reflect.TypeOf(float64(0.0))).Interface()
		case dosa.TUUID, dosa.String:
			queryResults[inx] = reflect.New(reflect.TypeOf("")).Interface()
		case dosa.Timestamp:
			queryResults[inx] = reflect.New(reflect.TypeOf(time.Time{})).Interface()
		default:
			panic(fmt.Sprintf("invalid type %v", coldef.Type))
		}
	}
	return queryResults
}

// Range implements the interface
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
	var rval []map[string]dosa.FieldValue
	for iter.Scan(queryResults...) {
		values := map[string]dosa.FieldValue{}
		for inx, field := range fieldsToRead {
			values[field] = dosa.FieldValue(reflect.ValueOf(queryResults[inx]).Elem().Interface())
		}

		limit = limit - 1
		if limit <= 0 {
			break
		}

		rval = append(rval, values)
	}
	var nextToken string
	if limit == 0 {
		nextToken = base64.StdEncoding.EncodeToString(iter.PageState())
	}
	err := iter.Close()
	if err != nil {
		return nil, "", err
	}
	if len(rval) == 0 {
		return nil, "", &dosa.ErrNotFound{}
	}
	return rval, nextToken, nil
}

// Search is not implemented
func (c *Connector) Search(ctx context.Context, ei *dosa.EntityInfo, fieldPairs dosa.FieldNameValuePair, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
}

// Scan is not implemented
func (c *Connector) Scan(ctx context.Context, ei *dosa.EntityInfo, fieldsToRead []string, token string, limit int) (multiValues []map[string]dosa.FieldValue, nextToken string, err error) {
	panic("not implemented")
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

// Shutdown is not implemented
func (c *Connector) Shutdown() error {
	panic("not implemented")
}
