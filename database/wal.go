package database

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sort"
	"time"
)

const defaultWALDir = "./bin/wal/"
const walFileNameFormat = "%d.wal"

type WAL struct {
	fileName string
	walFile  *os.File
}

func NewWAL() *WAL {
	w := &WAL{}

	if err := os.MkdirAll(defaultWALDir, 0755); err != nil {
		panic(err)
	}

	return w
}

func (w *WAL) RestoreOldData() ([][]byte, [][]byte, bool) {

	haveOldData := false

	files, err := os.ReadDir(defaultWALDir)

	if err != nil {
		panic(err)
	}

	keys, values := [][]byte{}, [][]byte{}

	if len(files) > 0 {
		haveOldData = true

		sort.Slice(files, func(i, j int) bool {
			return files[i].Name() > files[j].Name()
		})

		w.fileName = files[0].Name()
		files = files[1:]

		for _, file := range files {
			os.Remove(defaultWALDir + file.Name())
		}

		if w.walFile, err = os.OpenFile(defaultWALDir+w.fileName, os.O_CREATE|os.O_RDWR, 0644); err != nil {
			return nil, nil, false
		}

		reader := bufio.NewReader(w.walFile)

		for {

			line, err := reader.ReadString('\n')

			if err != nil {
				break
			}

			line = line[:len(line)-1]

			split := bytes.Split([]byte(line), []byte("-"))

			keys = append(keys, split[0])
			values = append(values, split[1])
		}

	} else {
		return nil, nil, haveOldData
	}

	return keys, values, haveOldData
}

func (w *WAL) CreateNewWAL() {
	var err error

	w.fileName = fmt.Sprintf(walFileNameFormat, time.Now().UnixNano())

	if w.walFile, err = os.OpenFile(defaultWALDir+w.fileName, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		panic(err)
	}
}

func (w *WAL) Delete() {
	w.walFile.Close()
	os.Remove(defaultWALDir + w.fileName)
}

func (w *WAL) Write(key []byte, value []byte) {
	walLog := fmt.Sprintf("%v-%v\n", string(key), string(value))

	w.walFile.Write([]byte(walLog))

	w.walFile.Sync()
}
