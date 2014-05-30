package tools

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"syscall"

	"fog"
)

type FileSpaceInfo interface {

	// FindMaxAvailSpaceID returns the space id with the most available space
	FindMaxAvailSpaceID(purpose string) (uint32, error)

	// SanityCheck verifies that the file info map is valid
	SanityCheck(repositoryPath string) error
}

type FileSpaceEntry struct {
	SpaceID uint32
	Path    string
}

type FileSpaceMap map[string][]FileSpaceEntry

const (
	FileSpaceJournal = "journal"
	FileSpaceStorage = "storage"
)

var (
	FileSpacePurpose = []string{FileSpaceJournal, FileSpaceStorage}
)

func NewFileSpaceInfo(sqlDB *sql.DB) (FileSpaceInfo, error) {
	fileSpaceMap := make(FileSpaceMap)

	rows, err := sqlDB.Query(
		`select space_id, purpose, path from nimbusio_node.file_space`)
	if err != nil {
		return fileSpaceMap, err
	}

	for rows.Next() {
		var entry FileSpaceEntry
		var purpose string
		if err := rows.Scan(&entry.SpaceID, &purpose, &entry.Path); err != nil {
			return fileSpaceMap, err
		}
		fileSpaceMap[purpose] = append(fileSpaceMap[purpose], entry)
	}

	if err := rows.Err(); err != nil {
		return fileSpaceMap, err
	}

	return fileSpaceMap, nil
}

// FindMaxAvailSpaceID returns the space id with the most space available
func (info FileSpaceMap) FindMaxAvailSpaceID(purpose string) (uint32, error) {
	var maxAvailSpace uint64
	var maxAvailSpaceID uint32
	var found bool
	var statfsBuffer syscall.Statfs_t

	for _, entry := range info[purpose] {
		if err := syscall.Statfs(entry.Path, &statfsBuffer); err != nil {
			return 0, fmt.Errorf("syscall.Statfs(%s, ...) %s", entry.Path, err)
		}
		availSpace := uint64(statfsBuffer.Bsize) * statfsBuffer.Bavail
		fog.Debug("(%d) %s available=%d", entry.SpaceID, entry.Path, availSpace)
		if availSpace > maxAvailSpace {
			found = true
			maxAvailSpace = availSpace
			maxAvailSpaceID = entry.SpaceID
		}
	}

	if !found {
		return 0, fmt.Errorf("no space found")
	}

	return maxAvailSpaceID, nil
}

// SanityCheck verifies that the file info map is valid
func (info FileSpaceMap) SanityCheck(repositoryPath string) error {
	for _, purpose := range FileSpacePurpose {
		for _, entry := range info[purpose] {
			fog.Debug("%s (%d) %s", purpose, entry.SpaceID, entry.Path)
			symlinkPath := path.Join(repositoryPath,
				fmt.Sprintf("%d", entry.SpaceID))
			destPath, err := os.Readlink(symlinkPath)
			if err != nil {
				return fmt.Errorf("(%d) os.Readlink(%s) %s", entry.SpaceID,
					symlinkPath, err)
			}
			if destPath != entry.Path {
				return fmt.Errorf("(%d) path mismatch %s != %s", entry.SpaceID,
					destPath, entry.Path)
			}
		}
	}
	return nil
}
