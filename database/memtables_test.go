package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Memtables_SimpleInsert(t *testing.T) {
	memTables := NewMemTables(100)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key4"),
	}

	for _, key := range keys {
		memTables.Insert(key, append(key, []byte("-value")...))
	}

	for _, key := range keys {
		value, ok := memTables.Get(key)

		require.True(t, ok)

		expectValue := append(key, []byte("-value")...)

		require.Equal(t, string(expectValue), string(value))
	}
}
