package database

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SkipList_int(t *testing.T) {

	var insertResult bool

	skipList := NewSkipList[int, int](OrderedComparator[int]{})

	numberArray := []int{
		10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
	}

	for _, num := range numberArray {
		insertResult = skipList.Insert(num, num*10)
		require.True(t, insertResult)
	}

	for _, num := range numberArray {
		value, ok := skipList.Get(num)

		require.True(t, ok)

		require.Equal(t, num*10, value)
	}
}

func Test_SkipList_String(t *testing.T) {

	var insertResult bool

	skipList := NewSkipList[string, string](OrderedComparator[string]{})

	stringArray := []string{
		"abc", "xcv", "ase", "qweqw", "5eqweqw", "eqw", "231232", "ewqq", "qwed", "vvv",
	}

	for _, str := range stringArray {
		insertResult = skipList.Insert(str, str+"-value")
		require.True(t, insertResult)
	}

	for _, str := range stringArray {
		value, ok := skipList.Get(str)

		require.True(t, ok)

		require.Equal(t, str+"-value", value)
	}
}

func Test_SkipList_GetAllValue(t *testing.T) {
	var insertResult bool

	skipList := NewSkipList[string, string](OrderedComparator[string]{})

	stringArray := []string{
		"abc", "xcv", "ase", "qweqw", "5eqweqw", "eqw", "231232", "ewqq", "qwed", "vvv",
	}

	for _, str := range stringArray {
		insertResult = skipList.Insert(str, str+"-value")
		require.True(t, insertResult)
	}

	sort.Strings(stringArray)

	keys, values := skipList.GetAll()

	require.Equal(t, len(stringArray), len(keys))
	require.Equal(t, len(stringArray), len(values))

	for i, str := range stringArray {
		require.Equal(t, str, keys[i])
		require.Equal(t, str+"-value", values[i])
	}

}
