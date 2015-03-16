package writermsg

// File contains data for the entire file
type File struct {
	FileSize             uint64 `json:"file-size"`
	EncodedFileMD5Digest string `json:"file-hash"`
	FileAdler32          int32  `json:"file-adler32"`
}
