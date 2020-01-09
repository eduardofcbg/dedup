package file

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

type File struct {
	Path    string
	Size    int64
	ModTime time.Time
	info    *os.FileInfo
}

func Search(folderPath string) ([]File, error) {
	var files []File

	alreadyFound := func(info os.FileInfo) bool {
		for _, file := range files {
			//TODO: We should have a set of inodes
			if os.SameFile(info, *file.info) {
				return true
			}
		}
		return false
	}

	visit := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Print(err)
		}
		if !info.IsDir() && !alreadyFound(info) {
			_, openErr = os.Open(path)

			if openErr == nil {
				file := File{path, info.Size(), info.ModTime(), &info}
				files = append(files, file)
			} else {
				log.Print(openErr)
			}
		}

		return nil
	}

	err := filepath.Walk(folderPath, visit)
	if err != nil {
		return nil, err
	}

	return files, nil
}
