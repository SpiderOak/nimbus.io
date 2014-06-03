package tools

import (
	"fmt"
	"path"
)

// ComputeValueFilePath computes the path to a specific value file
func ComputeValueFilePath(repositoryPath string, spaceID, valueFileID uint32) string {
	return path.Join(repositoryPath, fmt.Sprintf("%d", spaceID),
		fmt.Sprintf("%03d", valueFileID%uint32(1000)),
		fmt.Sprintf("%08d", valueFileID))
}
