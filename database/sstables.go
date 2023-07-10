package database

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const ssTableFileNameFormat = "level_%d_%s.sst"
const defaultSSTTableDir = "./bin/sst/"

type ssTable struct {
	prev          *ssTable
	next          *ssTable
	tableFileName string
	size          int
}

type ssTableMeta struct {
	rw       sync.RWMutex
	ssTable  *ssTable
	tail     *ssTable
	tableNum int
}

type SSTableManager struct {
	level                int
	defaultLevelTableNum int
	ssTableMetaTable     []ssTableMeta
	mergeRequest         chan struct{}
}

func NewSSTableManager(level int) *SSTableManager {

	return &SSTableManager{
		level:                level,
		defaultLevelTableNum: 1 << (level + 2),
		ssTableMetaTable:     make([]ssTableMeta, level),
		mergeRequest:         make(chan struct{}, 1),
	}
}

func (s *SSTableManager) RebuildFromOldFile() {
	files, err := ioutil.ReadDir(defaultSSTTableDir)

	if err != nil {
		fmt.Println(err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var (
			level int
		)

		fileName := file.Name()

		levelStr := strings.Split(fileName, "_")[1]

		fmt.Sscanf(levelStr, "%d", &level)

		newSSTable := &ssTable{
			tableFileName: file.Name(),
			size:          int(file.Size()),
		}

		s.insertToSSTable(level, newSSTable)
	}
}

func (s *SSTableManager) Get(key []byte) (*DataObject, bool) {
	iterLevel := 0

	for iterLevel < s.level {
		s.ssTableMetaTable[iterLevel].rw.RLock()

		if s.ssTableMetaTable[iterLevel].ssTable == nil {
			s.ssTableMetaTable[iterLevel].rw.RUnlock()
			iterLevel++
			continue
		}

		currentSSTable := s.ssTableMetaTable[iterLevel].ssTable

		for currentSSTable != nil {

			objectData, err := s.readSSTable(currentSSTable.tableFileName)

			if err != nil {
				s.ssTableMetaTable[iterLevel].rw.RUnlock()
				fmt.Println(err)
				return nil, false
			}

			if err != nil {
				s.ssTableMetaTable[iterLevel].rw.RUnlock()
				fmt.Println(err)
				return nil, false
			}

			for _, data := range objectData {
				if bytes.Compare(data.Key, key) == 0 {
					s.ssTableMetaTable[iterLevel].rw.RUnlock()
					return data, true
				}
			}

			currentSSTable = currentSSTable.next

		}

		s.ssTableMetaTable[iterLevel].rw.RUnlock()

		iterLevel++
	}

	return nil, false
}

// should to be the level one
func (s *SSTableManager) FlushToSSTable(memTable *MemTables) {
	newSSTable := &ssTable{}

	const levelOne = 0

	s.ssTableMetaTable[levelOne].rw.Lock()

	if s.ssTableMetaTable[levelOne].ssTable == nil {
		s.ssTableMetaTable[levelOne].ssTable = newSSTable
		s.ssTableMetaTable[levelOne].tail = newSSTable
	} else {
		s.ssTableMetaTable[levelOne].tail.next = newSSTable
		newSSTable.prev = s.ssTableMetaTable[levelOne].tail
		s.ssTableMetaTable[levelOne].tail = newSSTable
	}

	s.ssTableMetaTable[levelOne].tableNum++

	currentTableNum := s.ssTableMetaTable[levelOne].tableNum

	s.ssTableMetaTable[levelOne].rw.Unlock()

	newSSTable.tableFileName = fmt.Sprintf(ssTableFileNameFormat, 0, uuid.New().String())

	keys, values := memTable.GetAll()

	jsonBytes := make([][]byte, 0, len(keys))

	dupMap := make(map[string]int)

	for i, key := range keys {
		dataObject := NewDataObject(key, values[i].value, values[i].timeStamp)

		if v, exist := dupMap[string(key)]; exist {
			if values[v].timeStamp < values[i].timeStamp {
				jsonBytes[v] = dataObject.ToBinary()
			} else {
				continue
			}
		} else {
			jsonBytes = append(jsonBytes, dataObject.ToBinary())
			dupMap[string(key)] = i
		}
	}

	// write to disk
	newSSTable.size = len(jsonBytes)

	if err := os.MkdirAll(defaultSSTTableDir, 0755); err != nil {
		fmt.Println(err)
		return
	}

	file, err := os.OpenFile(defaultSSTTableDir+newSSTable.tableFileName, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	for _, jsonByte := range jsonBytes {
		file.Write(jsonByte)
	}

	file.Sync()

	memTable.wal.Delete()

	if currentTableNum >= (s.defaultLevelTableNum >> levelOne) {
		s.mergeRequest <- struct{}{}
	}
}

func (s *SSTableManager) MergeRequest() <-chan struct{} {
	return s.mergeRequest
}

func (s *SSTableManager) GetSSTableNum(level int) int {
	if level > s.level {
		return -1
	}

	s.ssTableMetaTable[level].rw.RLock()
	defer s.ssTableMetaTable[level].rw.RUnlock()

	return s.ssTableMetaTable[level].tableNum
}

func (s *SSTableManager) GetSSTableLimitNum(level int) int {
	if level > s.level {
		return -1
	}

	return s.defaultLevelTableNum >> level
}

func (s *SSTableManager) Merge(level int) {

	if level > s.level {
		return
	}

	// cut the list from the level
	s.ssTableMetaTable[level].rw.Lock()

	fastPoint, slowPoint := s.ssTableMetaTable[level].ssTable.next, s.ssTableMetaTable[level].ssTable

	cutPointSize := 0
	for fastPoint != nil && fastPoint.next != nil {
		fastPoint = fastPoint.next.next
		slowPoint = slowPoint.next
		cutPointSize++
	}

	if cutPointSize%2 != 0 {
		slowPoint = slowPoint.next
		cutPointSize++
	}

	slowPoint.prev.next = nil // cut the list
	slowPoint.prev = nil

	mergeSSTablePtr := s.ssTableMetaTable[level].ssTable

	s.ssTableMetaTable[level].ssTable = slowPoint

	s.ssTableMetaTable[level].tableNum -= cutPointSize

	s.ssTableMetaTable[level].rw.Unlock()

	// merge the list
	for mergeSSTablePtr != nil && mergeSSTablePtr.next != nil {
		s.mergeTwoSSTable(mergeSSTablePtr, mergeSSTablePtr.next, level+1)

		os.Remove(defaultSSTTableDir + mergeSSTablePtr.tableFileName)
		os.Remove(defaultSSTTableDir + mergeSSTablePtr.next.tableFileName)

		mergeSSTablePtr = mergeSSTablePtr.next.next
	}
}

func (s *SSTableManager) mergeTwoSSTable(s1, s2 *ssTable, level int) {
	if level > s.level {
		return
	}

	newSSTable := &ssTable{}

	s.insertToSSTable(level, newSSTable)

	s1Data, err := s.readSSTable(s1.tableFileName)

	if err != nil {
		fmt.Println(err)
		return
	}

	s2Data, err := s.readSSTable(s2.tableFileName)

	if err != nil {
		fmt.Println(err)
		return
	}

	mergeData := append(s1Data, s2Data...)

	sort.Slice(mergeData, func(i, j int) bool {
		return bytes.Compare(mergeData[i].Key, mergeData[j].Key) < 0
	})

	newSSTable.size = len(mergeData)

	file, err := os.OpenFile(defaultSSTTableDir+newSSTable.tableFileName, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	for _, m := range mergeData {
		file.Write(m.ToBinary())
	}

	file.Sync()
}

func (s *SSTableManager) insertToSSTable(level int, ss *ssTable) {
	s.ssTableMetaTable[level].rw.Lock()
	defer s.ssTableMetaTable[level].rw.Unlock()

	if level > s.level {
		return
	}

	s.ssTableMetaTable[level].tableNum++

	if ss.tableFileName == "" {
		ss.tableFileName = fmt.Sprintf(ssTableFileNameFormat, level, uuid.New().String())
	}

	if s.ssTableMetaTable[level].ssTable == nil {
		s.ssTableMetaTable[level].ssTable = ss
		s.ssTableMetaTable[level].tail = ss
	} else {
		s.ssTableMetaTable[level].tail.next = ss
		ss.prev = s.ssTableMetaTable[level].tail
		s.ssTableMetaTable[level].tail = ss
	}
}

func (s *SSTableManager) GetLevel() int {
	return s.level
}

func (s *SSTableManager) PrintLevelLinkedList(level int) {
	if level > s.level {
		return
	}

	s.ssTableMetaTable[level].rw.RLock()
	defer s.ssTableMetaTable[level].rw.RUnlock()

	ss := s.ssTableMetaTable[level].ssTable

	for ss != nil {
		fmt.Println(ss.tableFileName)
		ss = ss.next
	}
}

func (s *SSTableManager) PrintLevelData(level int) {
	if level > s.level {
		return
	}

	s.ssTableMetaTable[level].rw.RLock()
	defer s.ssTableMetaTable[level].rw.RUnlock()

	ss := s.ssTableMetaTable[level].ssTable

	for ss != nil {
		file, err := os.Open(defaultSSTTableDir + ss.tableFileName)

		if err != nil {
			fmt.Println(err)
			return
		}

		reader := bufio.NewReader(file)

		for {
			line, err := reader.ReadBytes('\n')

			if err == io.EOF {
				break
			}

			if err != nil {
				fmt.Println(err)
				return
			}

			data := ByteJsonToDataObject(line)

			fmt.Println(string(data.Key), string(data.Value))
		}

		ss = ss.next
	}

}

func (s *SSTableManager) readSSTable(fileName string) ([]*DataObject, error) {
	file, err := os.Open(defaultSSTTableDir + fileName)
	defer file.Close()

	if err != nil {
		return nil, err
	}

	dataOffset := 0

	dataList := make([]*DataObject, 0)

	for {

		lengthByte := make([]byte, totalLengthSize)
		_, err := file.Read(lengthByte)

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		length := binary.BigEndian.Uint32(lengthByte)

		byteData := make([]byte, int(length))

		_, err = file.Read(byteData)

		dataOffset = dataOffset + int(totalLengthSize) + int(length)

		binaryData := append(lengthByte, byteData...)

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		dataList = append(dataList, BinaryToDataObject(binaryData))

	}

	return dataList, nil
}
