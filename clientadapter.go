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

package dosa

import (
	"context"
	"fmt"
	"reflect"
)

func NewClient(connectorName string, opts map[string]string, objects ...DomainObject) (Client, error) {
	// first, validate the connector name
	connectorCreator, ok := registeredConnectors[connectorName]
	if !ok {
		return nil, fmt.Errorf("Invalid connector name %q", connectorName)
	}
	newClient := new(ClientAdapter)
	newClient.opts = opts

	var err error
	newClient.conn, err = connectorCreator(opts)
	if err != nil {
		return nil, err
	}

	// TODO: Get the service name, or get it from an opts
	if newClient.registry, err = NewRegistrar("servicename"); err != nil {
		return nil, err
	}
	if err = newClient.registry.Register(objects...); err != nil {
		return nil, err
	}

	return newClient, nil
}

type ClientAdapter struct {
	conn Connector
	registry Registrar
	opts map[string]string
}

func (ca *ClientAdapter) Initialize(_ context.Context) error {
	// TODO: We should defer creating the connector until here
	return nil
}

func (ca *ClientAdapter) CreateIfNotExists(context.Context, DomainObject) error {
	panic("not implemented")
}

func (ca *ClientAdapter) Read(ctx context.Context, fieldsToRead []string, o DomainObject) error {
	r := reflect.ValueOf(o).Elem()
	ed, fqn, err := ca.registry.lookupByType(o) // TODO: Pass the refected object into the registry
	if err != nil {
		return err
	}
	sr := SchemaReference(fqn)

	// extract the keys from the structure
	var keys map[string]FieldValue
	for _, pk := range ed.Key.PartitionKeys {
		value := r.FieldByName(pk)
		if !value.IsValid() {
			// this should never happen
			panic("Field " + pk + " was not found in " + ed.Name)
		}
		keys[pk] = value.Interface()
	}

	results, err := ca.conn.Read(ctx, sr, keys, fieldsToRead)
	if err != nil {
		return err
	}

	// Fill in the fields in the passed-in structure
	for fieldname, value := range results {
		v := r.FieldByName(fieldname)
		if v.IsValid() {
			newValue := reflect.ValueOf(value)
			v.Set(newValue)
		}
	}

	return nil
}

func (ca *ClientAdapter) BatchRead(context.Context, []string, ...DomainObject) (BatchReadResult, error) {
	panic("not implemented")
}

func (ca *ClientAdapter) Upsert(context.Context, []string, ...DomainObject) error {
	panic("not implemented")
}

func (ca *ClientAdapter) Delete(context.Context, ...DomainObject) error {
	panic("not implemented")
}

func (ca *ClientAdapter) Range(context.Context, *RangeOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

func (ca *ClientAdapter) Search(context.Context, *SearchOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

func (ca *ClientAdapter) ScanEverything(context.Context, *ScanOp) ([]DomainObject, string, error) {
	panic("not implemented")
}

