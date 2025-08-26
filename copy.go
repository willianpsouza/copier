package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	TotalFiles   = 3000
	PingMessage  = "__ping__"
	PongMessage  = "__pong__"
	CloseMessage = "__close__"
)

var sharedKey string

func main() {
	mode := flag.String("mode", "client", "Modo de operação: client, server ou dir")
	dir := flag.String("dir", "", "Diretório base para envio ou recebimento")
	host := flag.String("host", "localhost", "Endereço do servidor")
	port := flag.Int("port", 9090, "Porta para conexão/escuta")
	workers := flag.Int("workers", 16, "Número de conexões/goroutines paralelas")
	resume := flag.Bool("resume", false, "Habilita retomada de transferência")
	compress := flag.String("compress", "none", "Tipo de compressão: none, gzip ou lz4")
	checksum := flag.Bool("checksum", false, "Habilita verificação de integridade via MD5")
	bandwidth := flag.Int("max-bandwidth", 0, "Limite de banda em Mbps (0 = ilimitado)")
	key := flag.String("key", "", "Chave compartilhada obrigatória entre cliente e servidor")

	flag.Parse()

	if *mode != "client" && *mode != "server" && *mode != "dir" {
		log.Fatalf("Modo inválido: %s. Use 'client','server'  ou 'dir'", *mode)
	}

	if *mode != "dir" {
		if *dir == "" {
			log.Fatal("O parâmetro --dir é obrigatório")
		}

		if *key == "" {
			log.Fatal("A chave compartilhada (--key) é obrigatória")
		}

		sharedKey = *key

	}

	if *mode == "server" {
		startServer(*port, *dir, *workers, *compress, *checksum)
	} else if *mode == "client" {
		startClient(*host, *port, *dir, *workers, *resume, *compress, *checksum, *bandwidth)
	} else if *mode == "dir" {
		listFiles(*dir)
	}
}

func listFiles(dir string) {
	totalFiles := 0
	totalDirs := 0
	fmt.Printf("\033[1A\033[K")
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			totalFiles++
			rel, _ := filepath.Rel(dir, path)
			fmt.Printf("\033[K")
			fmt.Printf("%08d - %08d - %s \r", totalFiles, totalDirs, rel)

		} else {
			totalDirs++
		}
		return nil
	})
	if err != nil {
		log.Printf("Erro ao caminhar pelos arquivos: %v", err)
	}
	fmt.Printf("\n\n\nTotal files: %d\n", totalFiles)
}

func startServer(port int, dir string, workers int, compress string, checksum bool) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
	log.Printf("Servidor escutando na porta %d...", port)

	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Erro ao aceitar conexão: %v", err)
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { <-sem }()
			handleConnection(c, dir, compress, checksum)
		}(conn)
	}
}

func handleConnection(conn net.Conn, baseDir, compress string, checksum bool) {

	//TODO: support compression lz4/gzip
	if compress != "none" {
		log.Printf("in future we will support compress")
	}
	//TODO: support checksum MD5
	if checksum {
		log.Printf("in future we will support checksum")
	}

	defer func() { _ = conn.Close() }()
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		log.Println("Conexão recusada: não foi possível ler a chave inicial")
		return
	}
	clientKey := scanner.Text()
	if clientKey != sharedKey {
		log.Printf("Chave inválida, encerrando conexão Server: %s -- Client: %s ", sharedKey, clientKey)
		return
	}

	for scanner.Scan() {

		line := scanner.Text()
		
		if line == CloseMessage {
			log.Println("Sinal de encerramento recebido")
			return
		}
		if line == PingMessage {
			_, err := conn.Write([]byte(PongMessage + "\n"))
			if err != nil {
				log.Printf("Erro ao iniciar servidor: %v", err)
			}
			continue
		}

		relPath := line
		log.Printf("Recebendo arquivo: %s", relPath)
		absPath := filepath.Join(baseDir, relPath)
		err := os.MkdirAll(filepath.Dir(absPath), 0755)
		if err != nil {
			log.Printf("Erro ao criar diretórios de path: %v", err)
			return
		}

		out, errCreate := os.Create(absPath)
		if errCreate != nil {
			log.Printf("Erro ao criar arquivo: %v", errCreate)
			return
		}
		_, errCopy := io.Copy(out, conn)
		if errCopy != nil {
			log.Printf("Erro ao copiar conteúdo: %v", errCopy)
		}
		_ = out.Close()
		log.Printf("Arquivo %s salvo com sucesso", relPath)
	}
}

func startClient(host string, port int, dir string, workers int, resume bool, compress string, checksum bool, bandwidth int) {

	if bandwidth > 0 {
		//TODO: support bandwidth limit
		log.Printf("in future we will support bandwidth")
	}

	log.Printf("Cliente iniciado para enviar arquivos de '%s' para %s:%d", dir, host, port)

	if !testConnection(host, port) {
		log.Fatal("Não foi possível estabelecer conexão inicial com o servidor")
	}

	fileChan := make(chan string, TotalFiles)
	var wg sync.WaitGroup
	checkpoint := make(map[string]bool)
	checkpointFile := "checkpoint.log"

	if resume {
		f, err := os.Open(checkpointFile)
		if err == nil {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				checkpoint[scanner.Text()] = true
			}
			_ = f.Close()
		}
	}

	go func() {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				rel, _ := filepath.Rel(dir, path)
				if !checkpoint[rel] {
					fileChan <- path
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("Erro ao caminhar pelos arquivos: %v", err)
		}
		close(fileChan)
	}()

	tcpLock := sync.Mutex{}
	processed := 0
	totalBytes := int64(0)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for file := range fileChan {
				sent, err := sendFile(file, host, port, dir, compress, checksum)
				if err != nil {
					log.Printf("[worker %d] Erro ao enviar %s: %v", id, file, err)
					continue
				}
				totalBytes += sent
				rel, _ := filepath.Rel(dir, file)
				tcpLock.Lock()
				f, _ := os.OpenFile(checkpointFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				_, _ = f.WriteString(rel + "\n")
				_ = f.Close()
				processed++
				if processed%TotalFiles == 0 {
					log.Printf("Checkpoint: %d arquivos enviados", processed)
				}
				tcpLock.Unlock()
			}
		}(i)
	}

	wg.Wait()
	_ = sendCloseSignal(host, port)
	log.Printf("Transferência concluída. Arquivos: %d | Bytes: %d", processed, totalBytes)
}

func sendFile(filePath, host string, port int, baseDir, compress string, checksum bool) (int64, error) {

	if compress != "none" {
		//TODO: support compression lz4/gzip
		log.Printf("in future we will support compress")
	}

	if checksum {
		//TODO: support checksum
		log.Printf("in future we will support checksum")
	}

	relPath, _ := filepath.Rel(baseDir, filePath)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	_, _ = fmt.Fprintf(conn, "%s\n", sharedKey)
	_, _ = fmt.Fprintf(conn, "%s\n", relPath)

	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	return io.Copy(conn, f)
}

func testConnection(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 3*time.Second)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	_, err = fmt.Fprintf(conn, "%s\n%s\n", sharedKey, PingMessage)
	if err != nil {
		return false
	}
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() && scanner.Text() == PongMessage {
		return true
	}
	return false
}

func sendCloseSignal(host string, port int) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = fmt.Fprintf(conn, "%s\n%s\n", sharedKey, CloseMessage)
	return err
}
