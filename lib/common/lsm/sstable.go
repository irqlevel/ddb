package lsm

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

type SsTable struct {
	filePath string
	file     *os.File
	lock     sync.RWMutex
}

func newSsTable(filePath string, nodeMap map[string]*LsmNode) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	file, err := os.OpenFile(st.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		fmt.Printf("Create table %s error %v\n", st.filePath, err)
		return nil, err
	}

	keys := make([]string, len(nodeMap))
	i := 0
	for key := range nodeMap {
		keys[i] = key
		i++
	}
	sort.Strings(keys)

	for _, key := range keys {
		node := nodeMap[key]
		err = node.WriteTo(file)
		if err != nil {
			file.Close()
			os.Remove(st.filePath)
			return nil, err
		}
	}

	err = file.Sync()
	if err != nil {
		file.Close()
		os.Remove(st.filePath)
		return nil, err
	}
	st.file = file
	return st, nil
}

func openSsTable(filePath string) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	file, err := os.OpenFile(st.filePath, os.O_RDWR, 0600)
	if err != nil {
		fmt.Printf("Open table %s error %v\n", st.filePath, err)
		return nil, err
	}
	st.file = file
	return st, nil
}

func (st *SsTable) Get(key string) (string, error) {
	st.lock.RLock()
	defer st.lock.RUnlock()

	file, err := os.OpenFile(st.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()

	for {
		node := newLsmNode("", "")
		err = node.ReadFrom(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		if node.key == key {
			if node.deleted {
				return "", ErrNotFound
			}

			return node.value, nil
		}
	}

	return "", ErrNotFound
}

func (st *SsTable) Close() {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.file.Close()
	fmt.Printf("Close %s\n", st.filePath)
	st.file = nil
	st.filePath = ""
}

func (st *SsTable) Erase() {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.file.Close()
	fmt.Printf("Erase %s\n", st.filePath)
	os.Remove(st.filePath)
	st.file = nil
	st.filePath = ""
}

func mergeSsTable(prevSt *SsTable, currSt *SsTable, newFilePath string) (*SsTable, error) {
	prevSt.lock.RLock()
	defer prevSt.lock.RUnlock()
	currSt.lock.RLock()
	defer currSt.lock.RUnlock()

	var prevFile, currFile, newFile *os.File
	var err error

	defer func() {
		if prevFile != nil {
			prevFile.Close()
		}
		if currFile != nil {
			currFile.Close()
		}
		if err != nil {
			if newFile != nil {
				newFile.Close()
			}
			os.Remove(newFilePath)
		}
	}()

	prevFile, err = os.OpenFile(prevSt.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	currFile, err = os.OpenFile(currSt.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	newFile, err = os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}

	var prevNode, currNode, newNode *LsmNode

	for {
		if prevNode == nil && prevFile != nil {
			prevNode = new(LsmNode)
			err = prevNode.ReadFrom(prevFile)
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				prevFile.Close()
				prevFile = nil
				prevNode = nil
			}
		}

		if currNode == nil && currFile != nil {
			currNode = new(LsmNode)
			err = currNode.ReadFrom(currFile)
			if err != nil {
				if err != io.EOF {
					return nil, err
				}
				currFile.Close()
				currFile = nil
				currNode = nil
			}
		}

		if currNode == nil && prevNode == nil {
			break
		}

		if currNode != nil && prevNode == nil {
			newNode = currNode
			currNode = nil
		} else if prevNode != nil && currNode == nil {
			newNode = prevNode
			prevNode = nil
		} else {
			if prevNode.key == currNode.key {
				newNode = currNode
				currNode = nil
				prevNode = nil
			} else if prevNode.key < currNode.key {
				newNode = prevNode
				prevNode = nil
			} else {
				newNode = currNode
				currNode = nil
			}
		}

		err = newNode.WriteTo(newFile)
		if err != nil {
			return nil, err
		}
	}

	err = newFile.Sync()
	if err != nil {
		return nil, err
	}

	newSt := new(SsTable)
	newSt.filePath = newFilePath
	newSt.file = newFile
	return newSt, nil
}
