package database

import "encoding/json"

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

func ByteJsonToDataObject(data []byte) *DataObject {
	d := &DataObject{}
	json.Unmarshal(data, d)

	return d
}
