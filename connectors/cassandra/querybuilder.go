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
	"fmt"
	"reflect"
	"time"

	"github.com/uber-go/dosa"
)

type state int

const (
	stateInitial state = iota + 1
	stateProjected
	stateWhereClause
)

// QueryBuilder holds the information while building a query
type QueryBuilder struct {
	Statement        bytes.Buffer
	BoundVariables   []interface{}
	ProjectedResults []interface{}
	EntityDefinition *dosa.EntityDefinition
	qbstate          state
}

// NewSelectBuilder starts building a new select statement
func NewSelectBuilder(ed *dosa.EntityDefinition) *QueryBuilder {
	builder := newBuilder("select")
	builder.EntityDefinition = ed
	return builder
}

// newBuilder is an internal method to start building a statement
func newBuilder(clause string) *QueryBuilder {
	qb := new(QueryBuilder)
	fmt.Fprintf(&qb.Statement, "%s ", clause)
	qb.qbstate = stateInitial
	return qb
}

// Project adds a list of columns to the projection list
// NOTE: The columns must be valid inside the entity definition or this will
// panic, since it needs the type information
func (qb *QueryBuilder) Project(cols []string) {
	if qb.qbstate != stateInitial {
		panic("You can only project once, at the beginning")
	}
	qb.ProjectedResults = make([]interface{}, len(cols))
	for inx, field := range cols {
		if inx > 0 {
			qb.Statement.Write([]byte{','})
		}
		fmt.Fprintf(&qb.Statement, `"%s"`, field)
		coldef := qb.EntityDefinition.FindColumnDefinition(field)
		switch coldef.Type {
		case dosa.Bool:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf(false)).Interface()
		case dosa.Int32:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf(int32(0))).Interface()
		case dosa.Int64:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf(int64(0))).Interface()
		case dosa.Blob:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf([]byte{})).Interface()
		case dosa.Double:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf(float64(0.0))).Interface()
		case dosa.TUUID, dosa.String:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf("")).Interface()
		case dosa.Timestamp:
			qb.ProjectedResults[inx] = reflect.New(reflect.TypeOf(time.Time{})).Interface()
		default:
			panic(fmt.Sprintf("Invalid type %v", coldef.Type))
		}
	}
	qb.qbstate = stateProjected
}

// WhereEquals adds a where clause on an equal value for a set of columns
func (qb *QueryBuilder) WhereEquals(cols map[string]dosa.FieldValue) {
	switch qb.qbstate {
	case stateProjected:
		qb.qbstate = stateWhereClause
		fmt.Fprintf(&qb.Statement, ` from "%s" where `, qb.EntityDefinition.Name)
	case stateWhereClause:
		// this is okay, you can call WhereEquals more than once
		qb.Statement.WriteByte(' ')
	default:
		panic("querybuilder is in invalid state")
	}
}

// GetStatement returns the statement in string format
func (qb *QueryBuilder) GetStatement() string {
	if qb.qbstate != stateWhereClause {
		panic("invalid state for GetStatement")
	}
	return qb.Statement.String()
}

// GetBoundVariables returns the bound variable list
func (qb *QueryBuilder) GetBoundVariables() []interface{} {
	return qb.BoundVariables
}
