package main

import (
	"log"
	"os"
	"path/filepath"
)

func createWriterToFile(filename string) *os.File {
	writer, err := os.Create(filepath.Join(cfg.OutputDirectory, filename))
	if err != nil {
		log.Fatalf("File open error: %s", err)
	}
	return writer
}
