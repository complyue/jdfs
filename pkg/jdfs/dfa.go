package jdfs

// direct data file access methods

func (efs *exportedFileSystem) FindJDF(jdfGlob string) {

}

func (efs *exportedFileSystem) CreateJDF(jdfPath string,
	size uintptr, overwriteExisting bool) {

}

func (efs *exportedFileSystem) AllocJDF(jdfPath string,
	size uintptr, overwriteExisting bool) {

}

func (efs *exportedFileSystem) ReadJDF(jdfPath string,
	offset, size uintptr) {

}

func (efs *exportedFileSystem) WriteJDF(jdfPath string,
	offset, size uintptr) {

}
