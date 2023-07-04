package database

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_FlushToSSTable_Simple(t *testing.T) {
	memTables := NewMemTables(100)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	for _, key := range keys {
		memTables.Insert(key, append(key, []byte("-value")...))
	}

	memTables.Insert([]byte("key4"), []byte("new-value"))

	ssTableManager := NewSSTableManager(5)

	ssTableManager.FlushToSSTable(memTables)

	value, exist := ssTableManager.Get([]byte("key1"))

	require.True(t, exist)

	data := ByteJsonToDataObject(value)

	require.Equal(t, "key1-value", string(data.Value))
	require.Equal(t, "key1", string(data.Key))

	_, exist = ssTableManager.Get([]byte("key6"))
	require.False(t, exist)

	value, exist = ssTableManager.Get([]byte("key4"))
	require.True(t, exist)

	data = ByteJsonToDataObject(value)

	require.Equal(t, "new-value", string(data.Value))
	require.Equal(t, "key4", string(data.Key))

	os.Remove(defaultSSTTableDir + ssTableManager.ssTableMetaTable[0].ssTable.tableFileName)
}

func Test_FlushToSSTable_ManyMemTable(t *testing.T) {

	memTables := NewMemTables(100)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	for _, key := range keys {
		memTables.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager := NewSSTableManager(5)

	defer func() {
		iter := ssTableManager.ssTableMetaTable[0].ssTable

		for iter != nil {
			os.Remove(defaultSSTTableDir + iter.tableFileName)
			iter = iter.next
		}
	}()

	ssTableManager.FlushToSSTable(memTables)

	newMemTableOne := NewMemTables(100)

	keys = [][]byte{
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
	}

	for _, key := range keys {
		newMemTableOne.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager.FlushToSSTable(newMemTableOne)

	newMemTableTwo := NewMemTables(100)

	keys = [][]byte{
		[]byte("key9"),
		[]byte("key10"),
		[]byte("key11"),
		[]byte("key12"),
	}

	for _, key := range keys {
		newMemTableTwo.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager.FlushToSSTable(newMemTableTwo)

	value, exist := ssTableManager.Get([]byte("key11"))

	data := ByteJsonToDataObject(value)
	require.True(t, exist)
	require.Equal(t, "key11-value", string(data.Value))

	value, exist = ssTableManager.Get([]byte("key4"))

	data = ByteJsonToDataObject(value)
	require.True(t, exist)
	require.Equal(t, "key4-value", string(data.Value))

	require.Equal(t, 3, ssTableManager.GetSSTableNum(0))
}

func Test_Merge_Simple(t *testing.T) {
	memTables := NewMemTables(100)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	for _, key := range keys {
		memTables.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager := NewSSTableManager(5)

	defer func() {

		for i := 0; i < ssTableManager.level; i++ {
			iter := ssTableManager.ssTableMetaTable[i].ssTable

			for iter != nil {
				os.Remove(defaultSSTTableDir + iter.tableFileName)
				iter = iter.next
			}
		}

	}()

	ssTableManager.FlushToSSTable(memTables)

	newMemTableOne := NewMemTables(100)

	keys = [][]byte{
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
	}

	for _, key := range keys {
		newMemTableOne.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager.FlushToSSTable(newMemTableOne)

	newMemTableTwo := NewMemTables(100)

	keys = [][]byte{
		[]byte("key9"),
		[]byte("key10"),
		[]byte("key11"),
		[]byte("key12"),
	}

	for _, key := range keys {
		newMemTableTwo.Insert(key, append(key, []byte("-value")...))
	}

	ssTableManager.FlushToSSTable(newMemTableTwo)

	ssTableManager.Merge(0)

	value, exist := ssTableManager.Get([]byte("key1"))

	data := ByteJsonToDataObject(value)

	require.True(t, exist)
	require.Equal(t, "key1-value", string(data.Value))

	value, exist = ssTableManager.Get([]byte("key5"))

	data = ByteJsonToDataObject(value)

	require.True(t, exist)
	require.Equal(t, "key5-value", string(data.Value))

	value, exist = ssTableManager.Get([]byte("key9"))

	data = ByteJsonToDataObject(value)

	require.True(t, exist)
	require.Equal(t, "key9-value", string(data.Value))

}

func Test_Merge_MultiLevel(t *testing.T) {

	ssTableManager := NewSSTableManager(5)

	testDataSet := make([][]byte, 0)

	index := 0

	defer func() {

		for i := 0; i < ssTableManager.level; i++ {
			iter := ssTableManager.ssTableMetaTable[i].ssTable

			for iter != nil {
				os.Remove(defaultSSTTableDir + iter.tableFileName)
				iter = iter.next
			}
		}

	}()

	for i := 0; i < 10; i++ {
		memTables := NewMemTables(100)

		for j := 1; j <= 10; j++ {
			key := []byte(fmt.Sprintf("key%d", index))

			testDataSet = append(testDataSet, key)

			memTables.Insert(key, append(key, []byte("-value")...))

			index++
		}

		ssTableManager.FlushToSSTable(memTables)
	}

	ssTableManager.Merge(0)

	ssTableManager.Merge(0)

	ssTableManager.Merge(1)

	for _, key := range testDataSet {
		value, exist := ssTableManager.Get(key)
		require.True(t, exist, key)

		data := ByteJsonToDataObject(value)

		require.Equal(t, string(key)+"-value", string(data.Value))
	}
}
