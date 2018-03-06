package lsm

import (
	log "ddb/lib/common/log"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	ErrDeleted = fmt.Errorf("Deleted")
)

const (
	keysPerIndex = 256
)

type SsTable struct {
	filePath string
	file     *os.File
	lock     sync.RWMutex

	keyToOffset map[string]int64
	keys        []string

	minKey *string
	maxKey *string
	log    log.LogInterface
}

func (st *SsTable) index() error {
	file, err := os.OpenFile(st.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	st.minKey = nil
	st.maxKey = nil

	i := int64(0)

	st.keys = make([]string, 0)
	st.keyToOffset = make(map[string]int64)

	for {
		node := new(LsmNode)
		offset, err := file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		err = node.ReadFrom(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if st.minKey == nil {
			st.minKey = &node.key
		} else if node.key < *st.minKey {
			st.minKey = &node.key
		}

		if st.maxKey == nil {
			st.maxKey = &node.key
		} else if node.key > *st.maxKey {
			st.maxKey = &node.key
		}

		if i%keysPerIndex == 0 {
			st.keys = append(st.keys, node.key)
			st.keyToOffset[node.key] = offset
		}
		i++
	}

	sort.Strings(st.keys)
	return nil
}

func newSsTable(log log.LogInterface, filePath string, nodeMap map[string]*LsmNode) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	st.log = log
	file, err := os.OpenFile(st.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Pf(0, "Create table %s error %v", st.filePath, err)
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

	err = st.index()
	if err != nil {
		file.Close()
		os.Remove(st.filePath)
		return nil, err
	}
	st.file = file
	return st, nil
}

func openSsTable(log log.LogInterface, filePath string) (*SsTable, error) {
	st := new(SsTable)
	st.filePath = filePath
	st.log = log
	file, err := os.OpenFile(st.filePath, os.O_RDWR, 0600)
	if err != nil {
		log.Pf(0, "Open table %s error %v", st.filePath, err)
		return nil, err
	}
	st.file = file
	err = st.index()
	if err != nil {
		st.file.Close()
		return nil, err
	}
	return st, nil
}

func (st *SsTable) Get(key string) (string, error) {
	st.lock.RLock()
	defer st.lock.RUnlock()

	if st.minKey != nil && key < *st.minKey {
		return "", ErrNotFound
	}

	if st.maxKey != nil && key > *st.maxKey {
		return "", ErrNotFound
	}

	file, err := os.OpenFile(st.filePath, os.O_RDONLY, 0600)
	if err != nil {
		return "", err
	}
	defer file.Close()

	offset := int64(0)
	if len(st.keys) > 0 {
		keyIndex := sort.SearchStrings(st.keys, key)
		if keyIndex > 0 {
			keyIndex--
		}

		offset = st.keyToOffset[st.keys[keyIndex]]
		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			return "", err
		}
	}

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
				return "", ErrDeleted
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
	st.log.Pf(0, "Close %s", st.filePath)
	st.file = nil
	st.filePath = ""
}

func (st *SsTable) Erase() {
	st.lock.Lock()
	defer st.lock.Unlock()
	st.file.Close()
	st.log.Pf(0, "Erase %s", st.filePath)
	os.Remove(st.filePath)
	st.file = nil
	st.filePath = ""
}

func mergeSsTable(log log.LogInterface, prevSt *SsTable, currSt *SsTable, newFilePath string) (*SsTable, error) {
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
	newSt.log = log
	newSt.filePath = newFilePath
	newSt.file = newFile
	err = newSt.index()
	if err != nil {
		return nil, err
	}
	return newSt, nil
}
