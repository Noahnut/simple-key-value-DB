package database

import (
	"context"
	"time"
)

type LogStructuredMerge struct {
	ctx            context.Context
	memTableCh     chan *MemTables
	mergeTimer     *time.Ticker
	ssTableManager *SSTableManager
}

func LogMergeTreeBackGroundWorker(context context.Context, ch chan *MemTables, ssTableManager *SSTableManager) {
	lsm := &LogStructuredMerge{
		ctx:            context,
		memTableCh:     ch,
		mergeTimer:     time.NewTicker(1 * time.Minute),
		ssTableManager: ssTableManager,
	}

	go lsm.memTableBg()
	go lsm.ssTableBg()
}

func (l *LogStructuredMerge) memTableBg() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case flushMemTable := <-l.memTableCh:
			l.ssTableManager.FlushToSSTable(flushMemTable)
		}
	}
}

func (l *LogStructuredMerge) ssTableBg() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.ssTableManager.mergeRequest:
			// merge the ssTable

			level := 0

			l.ssTableManager.Merge(level)

			level++

			for l.ssTableManager.GetLevel() >= level &&
				(l.ssTableManager.GetSSTableNum(level) >= l.ssTableManager.GetSSTableLimitNum(level)) && level < l.ssTableManager.GetLevel()-1 {
				l.ssTableManager.Merge(level)
				level++
			}

		case <-l.mergeTimer.C:
			// check the ssTable whether need to merge
		}
	}
}
