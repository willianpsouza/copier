package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FileJob struct {
	SourcePath string
	TargetPath string
	Size       int64
}

type FileStatus struct {
	Path string
	Size int64
	Done bool
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Uso: copier <origem> <destino>")
		os.Exit(1)
	}

	srcDir := os.Args[1]
	dstDir := os.Args[2]

	jobs := make(chan FileJob, 100)
	status := make(chan FileStatus, 100)

	var wg sync.WaitGroup

	// Workers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				if err := copyFile(job.SourcePath, job.TargetPath); err == nil {
					status <- FileStatus{Path: job.SourcePath, Size: job.Size, Done: true}
				} else {
					fmt.Printf("[ERRO] Worker %d falhou ao copiar %s: %v\n", id, job.SourcePath, err)
				}
			}
		}(i)
	}

	// Goroutine de progresso
	go func() {
		var totalFiles, totalBytes int64
		for s := range status {
			if s.Done {
				totalFiles++
				totalBytes += s.Size
				fmt.Printf("[OK] Copiado: %s (%.2f MB) | Total: %d arquivos, %.2f MB\n",
					s.Path, float64(s.Size)/1024/1024, totalFiles, float64(totalBytes)/1024/1024)
			}
		}
	}()

	// Alimentador de jobs
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		relPath, _ := filepath.Rel(srcDir, path)
		targetPath := filepath.Join(dstDir, relPath)
		os.MkdirAll(filepath.Dir(targetPath), 0755)
		jobs <- FileJob{
			SourcePath: path,
			TargetPath: targetPath,
			Size:       info.Size(),
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Erro ao listar arquivos: %v\n", err)
	}

	close(jobs)
	wg.Wait()
	close(status)
	fmt.Println("Sincronização concluída.")
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
