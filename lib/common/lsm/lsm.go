package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrNotFound = fmt.Errorf("Not found")
)

const (
	logFileName = "lsm.log"
)

type Lsm struct {
	nodeMap     map[string]*LsmNode
	nodeMapLock sync.RWMutex
	rootPath    string
	logFile     *os.File
}

func (lsm *Lsm) compact() error {
	return nil
}

func (lsm *Lsm) logSet(key string, value string) error {
	n := newLsmNode(key, value)
	err := n.WriteTo(lsm.logFile)
	if err != nil {
		return err
	}
	return lsm.logFile.Sync()
}

func (lsm *Lsm) logDelete(key string) error {
	n := newLsmNode(key, "")
	n.deleted = true
	err := n.WriteTo(lsm.logFile)
	if err != nil {
		return err
	}
	return lsm.logFile.Sync()
}

func (lsm *Lsm) Set(key string, value string) error {
	lsm.nodeMapLock.Lock()
	defer lsm.nodeMapLock.Unlock()

	err := lsm.logSet(key, value)
	if err != nil {
		return err
	}

	node, ok := lsm.nodeMap[key]
	if ok {
		node.value = value
	} else {
		lsm.nodeMap[key] = newLsmNode(key, value)
	}

	go lsm.compact()

	return nil
}

func (lsm *Lsm) lookupTables(key string) (string, error) {
	return "", ErrNotFound
}

func (lsm *Lsm) Get(key string) (string, error) {
	lsm.nodeMapLock.RLock()
	defer lsm.nodeMapLock.RUnlock()
	node, ok := lsm.nodeMap[key]
	if ok {
		if node.deleted {
			return "", ErrNotFound
		}
		return node.value, nil
	}

	return lsm.lookupTables(key)
}

func (lsm *Lsm) Delete(key string) error {
	lsm.nodeMapLock.Lock()
	defer lsm.nodeMapLock.Unlock()

	err := lsm.logDelete(key)
	if err != nil {
		return err
	}

	node, ok := lsm.nodeMap[key]
	if ok {
		node.deleted = true
	} else {
		n := newLsmNode(key, "")
		n.deleted = true
		lsm.nodeMap[key] = n
	}

	return nil
}

func (lsm *Lsm) Close() {
	lsm.logFile.Close()
}

func NewLsm(rootPath string) (*Lsm, error) {
	rootPath, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(rootPath, 0700)
	if err != nil {
		return nil, err
	}

	logFile, err := os.OpenFile(filepath.Join(rootPath, logFileName), os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}

	lsm := new(Lsm)
	lsm.nodeMap = make(map[string]*LsmNode)
	lsm.rootPath = rootPath
	lsm.logFile = logFile

	return lsm, nil
}

func (lsm *Lsm) restore(logFile *os.File) error {
	for {
		n := newLsmNode("", "")
		err := n.ReadFrom(logFile)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		lsm.nodeMap[n.key] = n
	}

	return lsm.compact()
}

func OpenLsm(rootPath string) (*Lsm, error) {

	logFile, err := os.OpenFile(filepath.Join(rootPath, logFileName), os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	lsm := new(Lsm)
	lsm.nodeMap = make(map[string]*LsmNode)
	lsm.rootPath = rootPath

	err = lsm.restore(logFile)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	logFile.Close()

	logFile, err = os.OpenFile(filepath.Join(rootPath, logFileName), os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	lsm.logFile = logFile

	return lsm, nil
}
