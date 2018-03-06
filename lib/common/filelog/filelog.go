package filelog

import (
	"ddb/lib/common/logbackend"
	"fmt"
	"os"
	"sync"
)

var (
	ErrFileClosed = fmt.Errorf("File already closed")
)

type FileLog struct {
	logbackend.LogBackend
	file     *os.File
	filepath string
	lock     sync.RWMutex
}

func (lb *FileLog) Shutdown() {
	lb.lock.Lock()
	defer lb.lock.Unlock()

	if lb.filepath != "" {
		lb.file.Close()
	}
	lb.file = nil
}

func (lb *FileLog) Sync() error {
	return lb.file.Sync()
}

func (lb *FileLog) Println(timestamp int64, message string) error {
	lb.lock.RLock()
	defer lb.lock.RUnlock()

	if lb.file == nil {
		return ErrFileClosed
	}

	_, err := lb.file.WriteString(message)
	if err != nil {
		return err
	}

	_, err = lb.file.WriteString("\n")
	if err != nil {
		return err
	}

	return nil
}

func NewFileLog(filepath string) (logbackend.LogBackend, error) {
	lb := new(FileLog)
	lb.filepath = filepath

	file, err := os.OpenFile(lb.filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	lb.file = file
	return lb, nil
}

func NewFileLogWithFile(file *os.File) logbackend.LogBackend {
	lb := new(FileLog)
	lb.filepath = ""
	lb.file = file
	return lb
}
