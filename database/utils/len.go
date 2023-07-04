package utils

import "unsafe"

func Int32Size() int {
	return int(unsafe.Sizeof(int32(0)))
}
