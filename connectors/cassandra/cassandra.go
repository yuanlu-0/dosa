package cassandra

import (
	"context"
	"github.com/uber-go/dosa"
	"github.com/gocql/gocql"
	"fmt"
	"strings"
	"bytes"
)

type Connector struct {
	Cluster *gocql.ClusterConfig
	Session *gocql.Session
	keyspace string
}
func (c *Connector) CreateIfNotExists(ctx context.Context, sr dosa.SchemaReference, values map[string]dosa.FieldValue) error {
	panic("not implemented")
}

func (c *Connector) Read(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue, fieldsToRead []string) (map[string]dosa.FieldValue, error) {
	panic("not implemented")
}

func (c *Connector) BatchRead(ctx context.Context, sr dosa.SchemaReference, keys []map[string]dosa.FieldValue, fieldsToRead []string) ([]dosa.FieldValuesOrError, error) {
	panic("not implemented")
}

func (c *Connector) Upsert(ctx context.Context, sr dosa.SchemaReference, data map[string]dosa.FieldValue, fieldsToUpdate []string) error {
	var stmt bytes.Buffer
	var bound []interface{}

	for name, value := range data {
		if stmt.Len() == 0 {
			fmt.Fprintf(&stmt, "insert into %s (", sr)
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

	err := c.Session.Query(stmt.String(), bound...).WithContext(ctx).Exec()
	return err
}

func (c *Connector) BatchUpsert(ctx context.Context, sr dosa.SchemaReference, keys []map[string]dosa.FieldValue, fieldsToUpdate []string) ([]error, error) {
	panic("not implemented")
}

func (c *Connector) Remove(ctx context.Context, sr dosa.SchemaReference, keys map[string]dosa.FieldValue) error {
	panic("not implemented")
}

func (c *Connector) Range(ctx context.Context, sr dosa.SchemaReference, conditions []dosa.Condition, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

func (c *Connector) Search(ctx context.Context, sr dosa.SchemaReference, FieldNameValuePair []string, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, string, error) {
	panic("not implemented")
}

func (c *Connector) Scan(ctx context.Context, sr dosa.SchemaReference, fieldsToRead []string, token string, limit int) ([]map[string]dosa.FieldValue, error) {
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
		return fmt.Errorf("Table %q primary key length mismatch (was %d should be %d)", ed.Name, len(md.PartitionKey), len(ed.Key.PartitionKeys) )
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

// CheckSchema verifies that the schema passed in the registered entities matches the database
func (c *Connector) CheckSchema(_ context.Context, eds []*dosa.EntityDefinition) ([]dosa.SchemaReference, error) {
	schemaErrors := new(RepairableSchemaMismatchError)

	// TODO: unfortunately, gocql doesn't have a way to pass the context to this operation :(
	km, err := c.Session.KeyspaceMetadata(c.keyspace)
	if err != nil {
		return nil, err
	}
	refs := make([]dosa.SchemaReference, len(eds))
	for num, ed := range eds {
		tableMetadata, ok := km.Tables[ed.Name]
		if !ok {
			schemaErrors.MissingTables = append(schemaErrors.MissingTables, ed.Name)
			continue
		}
		refs[num] = dosa.SchemaReference(ed.Name)
		if err := compareStructToSchema(ed, tableMetadata, schemaErrors); err != nil {
			return nil, err
		}

	}
	if schemaErrors.HasMissing() {
		return nil, schemaErrors
	}
	return refs, nil
}

func (c *Connector) UpsertSchema(ctx context.Context, ed []*dosa.EntityDefinition) ([]dosa.SchemaReference, error) {
	sr, err := c.CheckSchema(ctx, ed)
	if _, ok := err.(*RepairableSchemaMismatchError); ok {
		// TODO: this is recoverable by adding the columns in the error message
		return nil, err
	}
	return sr, err
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

const (
	defaultContactPoints = "127.0.0.1"
	defaultKeyspace = "test"
	defaultConsistency = gocql.LocalQuorum
)

func init() {
	dosa.RegisterConnector("cassandra", func(opts map[string]string) (dosa.Connector, error) {
		c := new(Connector)
		var contactPoints string
		if contactPoints = opts["contact_points"]; contactPoints == ""  {
			contactPoints = defaultContactPoints
		}
		c.Cluster = gocql.NewCluster(strings.Split(contactPoints, ",")...)

		if c.keyspace = opts["keyspace"]; c.keyspace == "" {
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
