package vfs

import (
	"bytes"
	"unsafe"
)

// direct data file access

type DataFileHandle int

type DataFileList struct {
	Sizes    []uint64
	PathFlat []byte
	PathLens []uint32
}

func (dfl *DataFileList) Len() int {
	return len(dfl.Sizes)
}

func (dfl *DataFileList) EnumList(dfcb func(
	path string, size uint64,
) error) error {
	l := len(dfl.Sizes)
	var pathStart uint32
	for i := 0; i < l; i++ {
		pathEnd := pathStart + dfl.PathLens[i]
		path := string(dfl.PathFlat[pathStart:pathEnd])
		size := dfl.Sizes[i]
		if err := dfcb(path, size); err != nil {
			return err
		}
		pathStart = pathEnd
	}
	return nil
}

func DataFileListToFill(listLen int, pathFlatLen int) (dfl *DataFileList, bufs [][]byte) {
	dfl = &DataFileList{
		Sizes:    make([]uint64, listLen),
		PathFlat: make([]byte, pathFlatLen),
		PathLens: make([]uint32, listLen),
	}
	sizesBytes := uint64(listLen) * uint64(unsafe.Sizeof(dfl.Sizes[0]))
	pathLensBytes := uint64(listLen) * uint64(unsafe.Sizeof(dfl.PathLens[0]))
	bufs = [][]byte{
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.Sizes[0]))[0:sizesBytes:sizesBytes],
		dfl.PathFlat,
		(*[maxAllocSize]byte)(unsafe.Pointer(&dfl.PathLens[0]))[0:pathLensBytes:pathLensBytes],
	}
	return
}

type DataFileListBuilder struct {
	sizes       []uint64
	pathFlatBuf bytes.Buffer
	pathLens    []uint32
}

func (lb *DataFileListBuilder) Add(path string, size uint64) (err error) {
	var n int
	n, err = lb.pathFlatBuf.WriteString(path)
	if err != nil {
		return
	}
	lb.sizes = append(lb.sizes, size)
	lb.pathLens = append(lb.pathLens, uint32(n))
	return
}

func (lb *DataFileListBuilder) Len() int {
	return len(lb.sizes)
}

func (lb *DataFileListBuilder) PathFlatLen() int {
	return lb.pathFlatBuf.Len()
}

func (lb *DataFileListBuilder) ToSend() (listLen int, pathFlatLen int, payload [][]byte) {
	listLen = len(lb.sizes)
	pathFlatLen = lb.pathFlatBuf.Len()
	sizesBytes := uint64(listLen) * uint64(unsafe.Sizeof(lb.sizes[0]))
	pathLensBytes := uint64(listLen) * uint64(unsafe.Sizeof(lb.pathLens[0]))
	payload = [][]byte{
		(*[maxAllocSize]byte)(unsafe.Pointer(&lb.sizes[0]))[0:sizesBytes:sizesBytes],
		lb.pathFlatBuf.Bytes(),
		(*[maxAllocSize]byte)(unsafe.Pointer(&lb.pathLens[0]))[0:pathLensBytes:pathLensBytes],
	}
	return
}
