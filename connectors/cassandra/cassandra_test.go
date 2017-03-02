package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/uber-go/dosa"
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestCompareStructToSchemaWrongPk(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"p1"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			&dosa.ColumnDefinition{Name: "p1", Type: dosa.String},
			&dosa.ColumnDefinition{Name: "c1", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{PartitionKey: []*gocql.ColumnMetadata{
		{Name:"c1", Type: TestType{typ: gocql.TypeVarchar}},
	},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": &gocql.ColumnMetadata{Name:"p1", Type: TestType{typ: gocql.TypeVarchar}},

		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `"test"`)
}
func TestCompareStructToSchemaMissingColumn(t *testing.T) {
	ed := dosa.EntityDefinition{Key: &dosa.PrimaryKey{
		PartitionKeys: []string{"p1"}},
		Name: "test",
		Columns: []*dosa.ColumnDefinition{
			&dosa.ColumnDefinition{Name: "p1", Type: dosa.String},
			&dosa.ColumnDefinition{Name: "c1", Type: dosa.String},
		},
	}
	md := gocql.TableMetadata{PartitionKey: []*gocql.ColumnMetadata{
		&gocql.ColumnMetadata{Name:"p1", Type: TestType{typ: gocql.TypeVarchar}},
	},
		Columns: map[string]*gocql.ColumnMetadata{
			"p1": &gocql.ColumnMetadata{Name:"p1", Type: TestType{typ: gocql.TypeVarchar}},

		}}
	missing := RepairableSchemaMismatchError{}
	err := compareStructToSchema(&ed, &md, &missing)
	assert.Nil(t, err)
	assert.True(t, missing.HasMissing())
	assert.Equal(t, 1, len(missing.MissingColumns))
	assert.Equal(t, "test", missing.MissingColumns[0].Tablename)
	assert.Equal(t, "c1", missing.MissingColumns[0].Column.Name)
}


type TestType struct {
	typ gocql.Type
}

func (t TestType) New() interface{} {
	panic("not implemented")
}
func (t TestType) Version() byte {
	panic("not implemented")
}
func (t TestType) Custom() string {
	panic("not implemented")
}
func (t TestType) Type() gocql.Type {
	return t.typ
}