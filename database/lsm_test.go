package database

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SimpleLSM(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lsmChan := make(chan *MemTables, 1)

	ssTable := NewSSTableManager(5)

	LogMergeTreeBackGroundWorker(ctx, lsmChan, ssTable)

	index := 0

	defer func() {

		for i := 0; i < ssTable.level; i++ {
			iter := ssTable.ssTableMetaTable[i].ssTable

			for iter != nil {
				os.Remove(defaultSSTTableDir + iter.tableFileName)
				iter = iter.next
			}
		}

	}()

	keySet := make([]string, 0)
	valueSet := make([]string, 0)

	for i := 0; i < 1000; i++ {
		memTables := NewMemTables(100)
		for j := 0; j < 100; j++ {

			key := fmt.Sprintf("key%d", index)

			keySet = append(keySet, key)

			value := fmt.Sprintf("value%d", index)

			valueSet = append(valueSet, value)

			memTables.Insert([]byte(key), []byte(value))

			index++
		}

		lsmChan <- memTables
	}

	for i := 0; i < 5; i++ {
		ssTable.mergeRequest <- struct{}{}
	}

	for i := 0; i < len(keySet)/1000; i++ {
		data, exist := ssTable.Get([]byte(keySet[i]))
		require.True(t, exist)
		require.Equal(t, valueSet[i], string(data.Value))
	}

}
