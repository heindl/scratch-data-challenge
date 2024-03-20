package internal_test

import (
	"context"
	"scratch/internal"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreReadWrite(t *testing.T) {
	store, err := internal.NewDuckDBStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})
	require.NoError(t, store.Insert(context.Background(), &internal.InsertStatement{
		Table: "test_table",
		Columns: map[string]any{
			"column_a": 1,
			"column_b": "2",
		},
	}))

	rows, err := store.Query(context.Background(), &internal.QueryStatement{
		Query: "select * from test_table",
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Len(t, rows[0], 2)
}

func TestStoreColumnAddition(t *testing.T) {
	store, err := internal.NewDuckDBStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})

	for _, stmt := range []*internal.InsertStatement{
		{
			Columns: map[string]any{
				"column_a": 1,
				"column_b": "2",
			},
		},
		{
			Columns: map[string]any{
				"column_a": 2,
				"column_b": "3",
				"column_c": true,
				"column_d": 4.0001,
			},
		},
		{
			Columns: map[string]any{
				"column_d": 5.0001,
			},
		},
	} {
		stmt.Table = "test_table"
		require.NoError(t, store.Insert(context.Background(), stmt))
	}

	rows, err := store.Query(context.Background(), &internal.QueryStatement{
		Query: "select * from test_table",
	})
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

func TestStoreDataTypes(t *testing.T) {
	store, err := internal.NewDuckDBStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})

	require.NoError(t, store.Insert(context.Background(), &internal.InsertStatement{
		Table: "test_table",
		Columns: map[string]any{
			"str":   "str test",
			"bool":  false,
			"float": 1.2458,
			"int":   9123,
		},
	}))

	for _, stmt := range []*internal.InsertStatement{
		{
			Columns: map[string]any{
				"bool": "1.2345",
			},
		},
		{
			Columns: map[string]any{
				"float": "test",
			},
		},
		{
			Columns: map[string]any{
				"int": "test",
			},
		},
	} {
		stmt.Table = "test_table"
		require.Error(t, store.Insert(context.Background(), stmt))
	}
}
