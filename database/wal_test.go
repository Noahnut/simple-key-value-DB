package database

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_CreateNewWAL(t *testing.T) {
	wal := NewWAL()
	wal.CreateNewWAL()

	defer func() {
		os.Remove(defaultWALDir + wal.fileName)
	}()

	if wal.walFile == nil {
		t.Error("walFile is nil")
	}

	if wal.fileName == "" {
		t.Error("fileName is empty")
	}

	fils, err := os.ReadDir(defaultWALDir)

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, 1, len(fils))

	wal.Delete()
}

func Test_WriteToWAL(t *testing.T) {
	wal := NewWAL()
	wal.CreateNewWAL()

	defer wal.Delete()

	keyDataSet := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	valueDataSet := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
	}

	for i := 0; i < len(keyDataSet); i++ {
		wal.Write(keyDataSet[i], valueDataSet[i])
	}

	reader := bufio.NewReader(wal.walFile)

	readerData := []string{}

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			break
		}

		require.NotEmpty(t, line)
		readerData = append(readerData, line)
	}

	for i := 0; i < len(readerData); i++ {
		require.Equal(t, string(keyDataSet[i])+"-"+string(valueDataSet[i])+"\n", readerData[i])
	}
}

func Test_RestoreOldData(t *testing.T) {
	wal := NewWAL()

	_, _, noOldData := wal.RestoreOldData()

	require.False(t, noOldData)

	wal.CreateNewWAL()

	defer wal.Delete()

	keyDataSet := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	valueDataSet := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("value4"),
	}

	for i := 0; i < len(keyDataSet); i++ {
		wal.Write(keyDataSet[i], valueDataSet[i])
	}

	keys, values, haveOldData := wal.RestoreOldData()

	require.True(t, haveOldData)

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keyDataSet[i], keys[i])
		require.Equal(t, valueDataSet[i], values[i])
	}
}
