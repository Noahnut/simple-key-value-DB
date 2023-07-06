package database

import (
	"encoding/binary"
	"encoding/json"
	"unsafe"
)

// | totalLength | key length | key 		| value length | value 		 | timestamp |
// | 4 bytes     | 4 bytes    | n           | 4 bytes      | n     | 8 bytes   |

const totalLengthSize = uint32(unsafe.Sizeof(uint32(0)))
const keyLengthSize = uint32(unsafe.Sizeof(uint32(0)))
const valueLengthSize = uint32(unsafe.Sizeof(uint32(0)))
const timestampSize = uint32(unsafe.Sizeof(uint64(0)))

type DataObject struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

func NewDataObject(key []byte, value []byte, timestamp int64) *DataObject {
	return &DataObject{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}
}

func (d *DataObject) ToJsonByte() []byte {
	v, _ := json.Marshal(d)

	return v
}

func (d *DataObject) ToBinary() []byte {

	totalLength := make([]byte, totalLengthSize)
	binary.BigEndian.PutUint32(totalLength, uint32(len(d.Key)+len(d.Value)+16))

	keyLength := make([]byte, keyLengthSize)
	binary.BigEndian.PutUint32(keyLength, uint32(len(d.Key)))

	valueLength := make([]byte, valueLengthSize)
	binary.BigEndian.PutUint32(valueLength, uint32(len(d.Value)))

	timestamp := make([]byte, timestampSize)
	binary.BigEndian.PutUint64(timestamp, uint64(d.Timestamp))

	return append(append(append(append(totalLength, keyLength...), d.Key...), append(valueLength, d.Value...)...), timestamp...)
}

func ByteJsonToDataObject(data []byte) *DataObject {
	d := &DataObject{}
	json.Unmarshal(data, d)

	return d
}

func BinaryToDataObject(data []byte) *DataObject {
	d := &DataObject{}

	keyLengthOffset := totalLengthSize + keyLengthSize
	keyLength := binary.BigEndian.Uint32(data[totalLengthSize:keyLengthOffset])

	keyOffset := totalLengthSize + keyLengthSize + keyLength
	d.Key = data[keyLengthOffset:keyOffset]

	valueLengthOffset := keyOffset + valueLengthSize
	valueLength := binary.BigEndian.Uint32(data[keyOffset:valueLengthOffset])

	valueOffset := valueLengthOffset + valueLength
	d.Value = data[valueLengthOffset:valueOffset]

	timestampOffset := valueOffset + timestampSize
	d.Timestamp = int64(binary.BigEndian.Uint64(data[valueOffset:timestampOffset]))

	return d
}
