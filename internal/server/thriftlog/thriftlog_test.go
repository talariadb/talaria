// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package thriftlog

import (
	"testing"

	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

func TestThriftLog(t *testing.T) {
	assert.NotPanics(t, func() {
		tl := Service{
			Service: new(noopPrestoThrift),
			Monitor: monitor.NewNoop(),
		}

		_, err := tl.PrestoGetIndexSplits(nil, nil, nil, nil, nil, 0, nil)
		assert.NoError(t, err)

		_, err = tl.PrestoGetSplits(nil, nil, nil, 0, nil)
		assert.NoError(t, err)

		_, err = tl.PrestoGetRows(nil, nil, 0, nil)
		assert.NoError(t, err)

		_, err = tl.PrestoGetTableMetadata(nil)
		assert.NoError(t, err)

		_, err = tl.PrestoListSchemaNames()
		assert.NoError(t, err)

		_, err = tl.PrestoListTables(nil)
		assert.NoError(t, err)
	})
}

type noopPrestoThrift struct{}

// PrestoGetIndexSplits returns a batch of index splits for the given batch of keys.
func (s *noopPrestoThrift) PrestoGetIndexSplits(schemaTableName *presto.PrestoThriftSchemaTableName, indexColumnNames []string, outputColumnNames []string, keys *presto.PrestoThriftPageResult, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	return nil, nil
}

// PrestoGetSplits returns a batch of splits.
func (s *noopPrestoThrift) PrestoGetSplits(schemaTableName *presto.PrestoThriftSchemaTableName, desiredColumns *presto.PrestoThriftNullableColumnSet, outputConstraint *presto.PrestoThriftTupleDomain, maxSplitCount int32, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftSplitBatch, error) {
	return nil, nil
}

// PrestoGetRows returns a batch of rows for the given split.
func (s *noopPrestoThrift) PrestoGetRows(splitID *presto.PrestoThriftId, columns []string, maxBytes int64, nextToken *presto.PrestoThriftNullableToken) (*presto.PrestoThriftPageResult, error) {
	return nil, nil
}

// PrestoGetTableMetadata returns metadata for a given table.
func (s *noopPrestoThrift) PrestoGetTableMetadata(schemaTableName *presto.PrestoThriftSchemaTableName) (*presto.PrestoThriftNullableTableMetadata, error) {
	return nil, nil
}

// PrestoListSchemaNames returns available schema names.
func (s *noopPrestoThrift) PrestoListSchemaNames() ([]string, error) {
	return nil, nil
}

// PrestoListTables returns tables for the given schema name.
func (s *noopPrestoThrift) PrestoListTables(schemaNameOrNull *presto.PrestoThriftNullableSchemaName) ([]*presto.PrestoThriftSchemaTableName, error) {
	return nil, nil
}
