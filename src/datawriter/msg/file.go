package msg

// File contains data for the entire file
type FileEntry struct {
	FileSize      uint64 `json:"file-size"`
	FileMD5Digest []byte `json:"file-hash"`
	FileAdler32   int32  `json:"file-adler32"`
}
