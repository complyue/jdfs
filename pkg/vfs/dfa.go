package vfs

import (
	"unsafe"
)

// direct data file access

type DataFileHandle int

type DataFileList struct {
	Sizes    []int64
	PathFlat []byte
	PathEpos []uint32
}

func (dfl *DataFileList) Len() int {
	return len(dfl.Sizes)
}

func (dfl *DataFileList) Get(i int) (size int64, path string) {
	size = dfl.Sizes[i]
	var sp uint32
	if i > 0 {
		sp = dfl.PathEpos[i-1]
	}
	ep := dfl.PathEpos[i]
	path = string(dfl.PathFlat[sp:ep])
	return
}

func (dfl *DataFileList) Add(size int64, path string) {
	dfl.Sizes = append(dfl.Sizes, size)
	dfl.PathFlat = append(dfl.PathFlat, path...)
	dfl.PathEpos = append(dfl.PathEpos, uint32(len(dfl.PathFlat)))
}

func (dfl *DataFileList) ToSend() (listLen int, pathFlatLen int, payload [][]byte) {
	listLen = len(dfl.Sizes)
	if listLen <= 0 {
		return // keep all zeros
	}
	pathFlatLen = len(dfl.PathFlat)
	sizesBytes := int64(listLen) * int64(unsafe.Sizeof(dfl.Sizes[0]))
	pathEposBytes := int64(listLen) * int64(unsafe.Sizeof(dfl.PathEpos[0]))
	payload = [][]byte{
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.Sizes[0]))[0:sizesBytes:sizesBytes],
		dfl.PathFlat,
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.PathEpos[0]))[0:pathEposBytes:pathEposBytes],
	}
	return
}

func ToReceiveDataFileList(listLen int, pathFlatLen int) (dfl *DataFileList, payload [][]byte) {
	dfl = &DataFileList{}
	if listLen <= 0 {
		return
	}
	dfl.Sizes = make([]int64, listLen)
	dfl.PathFlat = make([]byte, pathFlatLen)
	dfl.PathEpos = make([]uint32, listLen)
	sizesBytes := int64(listLen) * int64(unsafe.Sizeof(dfl.Sizes[0]))
	pathEposBytes := int64(listLen) * int64(unsafe.Sizeof(dfl.PathEpos[0]))
	payload = [][]byte{
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.Sizes[0]))[0:sizesBytes:sizesBytes],
		dfl.PathFlat,
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.PathEpos[0]))[0:pathEposBytes:pathEposBytes],
	}
	return
}
