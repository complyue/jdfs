package vfs

// direct data file access

type DataFileHandle int

type DataFileList struct {
	PathFlat string
	PathLens []int
	Sizes    []uintptr
}

func (dfl DataFileList) Length() int {
	return len(dfl.Sizes)
}

func (dfl DataFileList) EnumDataFiles(dfcb func(
	path string, size uintptr,
) error) error {
	l := len(dfl.Sizes)
	pathStart := 0
	for i := 0; i < l; i++ {
		pathEnd := pathStart + dfl.PathLens[i]
		path := dfl.PathFlat[pathStart:pathEnd]
		size := dfl.Sizes[i]

		if err := dfcb(path, size); err != nil {
			return err
		}
		pathStart = pathEnd
	}
	return nil
}
