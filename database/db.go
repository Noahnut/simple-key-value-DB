package database

import (
	"context"
	"errors"
	"sync"
)

type DB struct {
	ctx        context.Context
	cancel     context.CancelFunc
	memTable   *MemTables
	ssTable    *SSTableManager
	lsmChannel chan *MemTables
	rw         sync.RWMutex
}

func Open() (*DB, error) {

	ctx, cancel := context.WithCancel(context.Background())

	db := &DB{
		ctx:        ctx,
		cancel:     cancel,
		memTable:   NewMemTables(memTablesSizeLimit),
		lsmChannel: make(chan *MemTables, 10),
		ssTable:    NewSSTableManager(5),
	}

	LogMergeTreeBackGroundWorker(ctx, db.lsmChannel, db.ssTable)

	return db, nil
}

func (d *DB) Close() {
	d.cancel()
}

func (d *DB) Get(key string) ([]byte, bool) {

	var (
		value []byte
		exist bool
	)

	value, exist = d.memTable.Get([]byte(key))

	if !exist {
		ssTableValue, exist := d.ssTable.Get([]byte(key))

		if exist {
			value = ssTableValue.Value
		}
	}

	return value, exist
}

func (d *DB) Put(key, value string) error {
	return d.insert([]byte(key), []byte(value))
}

func (d *DB) Delete(key string) {
	d.insert([]byte(key), nil)
}

func (d *DB) Insert(key string, value string) error {
	return d.insert([]byte(key), []byte(value))
}

func (d *DB) insert(key, value []byte) error {
	if success := d.memTable.Insert(key, value); !success {
		return errors.New("")
	}

	if d.memTable.GetSize() >= d.memTable.GetSizeLimit() {
		d.rw.Lock()
		d.lsmChannel <- d.memTable
		d.memTable = NewMemTables(memTablesSizeLimit)
		d.rw.Unlock()
	}

	return nil
}
