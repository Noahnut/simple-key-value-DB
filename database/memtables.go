package database

import (
	"bytes"
	"sync"
	"time"
)

const memTablesSizeLimit = 2 * 1024 * 1024

type byteComparator struct {
}

func (c *byteComparator) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

type MemTables struct {
	sizeLimit int64
	skipList  *SkipList[[]byte, *valueType]
	rw        sync.RWMutex
	wal       *WAL
}

func NewMemTables(sizeLimit int64) *MemTables {

	comparator := &byteComparator{}

	memTable := &MemTables{
		sizeLimit: sizeLimit,
		skipList:  NewSkipList[[]byte, *valueType](comparator),
	}

	memTable.wal = NewWAL()

	keys, values, haveOldData := memTable.wal.RestoreOldData()

	if haveOldData {
		for i := 0; i < len(keys); i++ {
			memTable.Insert(keys[i], values[i])
		}
	} else {
		memTable.wal.CreateNewWAL()
	}

	return memTable
}

func (m *MemTables) GetSizeLimit() int64 {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.sizeLimit
}

func (m *MemTables) GetSize() int64 {
	m.rw.RLock()
	defer m.rw.RUnlock()
	return m.skipList.GetSize()
}

func (m *MemTables) Insert(key []byte, value []byte) bool {
	m.rw.Lock()
	defer m.rw.Unlock()
	internalValue := newInternalValue(value)

	return m.skipList.Insert(key, &internalValue)
}

func (m *MemTables) Get(key []byte) ([]byte, bool) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	value, exist := m.skipList.Get(key)

	if !exist {
		return nil, false
	}

	if value.value == nil {
		return nil, false
	}

	return value.value, true
}

func (m *MemTables) GetAll() ([][]byte, []*valueType) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	keys, values := m.skipList.GetAll()

	return keys, values
}

type valueType struct {
	timeStamp int64
	value     []byte
}

func newInternalValue(value []byte) valueType {
	return valueType{
		timeStamp: time.Now().UnixMicro(),
		value:     value,
	}
}
