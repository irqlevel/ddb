package lsm

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound            = fmt.Errorf("Not found")
	ErrEmptyKey            = fmt.Errorf("Empty key")
	ErrEmptyValue          = fmt.Errorf("Empty value")
	ssTableFileNamePattern = regexp.MustCompile(`^lsm\_([0-9]+)\.sstable$`)
)

const (
	logFileName        = "lsm.log"
	maxMemoryNodeCount = 1000
	mergeTimeoutMs     = 1000
	compactTimeoutMs   = 100
)

type Lsm struct {
	nodeMap        map[string]*LsmNode
	nodeMapLock    sync.RWMutex
	rootPath       string
	logFile        *os.File
	ssTableMap     map[int64]*SsTable
	ssTableMapLock sync.RWMutex
	time           int64
	mergeTimer     *time.Ticker
	compactTimer   *time.Ticker
	compactChan    chan bool
	stopChan       chan bool
	closing        bool
	wg             sync.WaitGroup
}

func (lsm *Lsm) shouldCompact(force bool) bool {
	if force || (!lsm.closing && len(lsm.nodeMap) > maxMemoryNodeCount) {
		return true
	}
	return false
}

func (lsm *Lsm) compact(force bool, logTruncate bool) error {
	lsm.nodeMapLock.RLock()
	if !lsm.shouldCompact(force) {
		lsm.nodeMapLock.RUnlock()
		return nil
	}
	lsm.nodeMapLock.RUnlock()

	lsm.nodeMapLock.Lock()
	defer lsm.nodeMapLock.Unlock()
	if !lsm.shouldCompact(force) {
		lsm.nodeMapLock.RUnlock()
		return nil
	}

	time := atomic.AddInt64(&lsm.time, 1)
	fmt.Printf("Compacting %d size %d\n", time, len(lsm.nodeMap))
	st, err := newSsTable(lsm.getSsTablePath(time), lsm.nodeMap)
	if err != nil {
		return err
	}

	lsm.ssTableMapLock.Lock()
	lsm.ssTableMap[time] = st
	lsm.ssTableMapLock.Unlock()

	lsm.nodeMap = make(map[string]*LsmNode)

	if logTruncate {
		err = lsm.logFile.Truncate(0)
	}

	return err
}

func (lsm *Lsm) mergeSsTables() error {
	lsm.ssTableMapLock.RLock()
	if len(lsm.ssTableMap) <= 1 {
		lsm.ssTableMapLock.RUnlock()
		return nil
	}

	time := atomic.AddInt64(&lsm.time, 1)

	fmt.Printf("Merge %d\n", time)

	ids := make([]int64, len(lsm.ssTableMap))
	i := 0
	for id := range lsm.ssTableMap {
		ids[i] = id
		i++
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	prevStId := ids[len(ids)-2]
	currStId := ids[len(ids)-1]

	prevSt := lsm.ssTableMap[prevStId]
	currSt := lsm.ssTableMap[currStId]
	lsm.ssTableMapLock.RUnlock()

	st, err := mergeSsTable(prevSt, currSt, lsm.getSsTablePath(time))
	if err != nil {
		return err
	}

	lsm.ssTableMapLock.Lock()
	lsm.ssTableMap[time] = st
	delete(lsm.ssTableMap, prevStId)
	delete(lsm.ssTableMap, currStId)
	lsm.ssTableMapLock.Unlock()

	prevSt.Erase()
	currSt.Erase()

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
	if key == "" {
		return ErrEmptyKey
	}
	if value == "" {
		return ErrEmptyValue
	}

	lsm.nodeMapLock.Lock()
	defer func() {
		compact := lsm.shouldCompact(false)
		lsm.nodeMapLock.Unlock()
		if compact {
			lsm.compactChan <- true
		}
	}()

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

	return nil
}

func (lsm *Lsm) lookupSsTables(key string) (string, error) {
	lsm.ssTableMapLock.RLock()
	defer lsm.ssTableMapLock.RUnlock()

	ids := make([]int64, len(lsm.ssTableMap))
	i := 0
	for id := range lsm.ssTableMap {
		ids[i] = id
		i++
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	for _, id := range ids {
		st := lsm.ssTableMap[id]

		value, err := st.Get(key)
		if err == nil {
			return value, nil
		}

		if err == ErrDeleted {
			return "", ErrNotFound
		}

		if err != ErrNotFound {
			return "", err
		}
	}

	return "", ErrNotFound
}

func (lsm *Lsm) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	lsm.nodeMapLock.RLock()
	defer lsm.nodeMapLock.RUnlock()

	node, ok := lsm.nodeMap[key]
	if ok {
		if node.deleted {
			return "", ErrNotFound
		}
		return node.value, nil
	}

	return lsm.lookupSsTables(key)
}

func (lsm *Lsm) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	lsm.nodeMapLock.Lock()
	defer func() {
		compact := lsm.shouldCompact(false)
		lsm.nodeMapLock.Unlock()
		if compact {
			lsm.compactChan <- true
		}
	}()

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
	fmt.Printf("Close\n")

	lsm.nodeMapLock.Lock()
	lsm.closing = true
	lsm.nodeMapLock.Unlock()

	lsm.stopChan <- true

	lsm.mergeTimer.Stop()
	lsm.compactTimer.Stop()

	lsm.wg.Wait()

	lsm.nodeMapLock.Lock()
	defer lsm.nodeMapLock.Unlock()

	lsm.closeSsTables()
	lsm.logFile.Close()
}

func (lsm *Lsm) Background() {
	defer lsm.wg.Done()

	for {
		select {
		case <-lsm.mergeTimer.C:
			lsm.mergeSsTables()
		case <-lsm.compactTimer.C:
			lsm.compact(false, true)
			lsm.mergeSsTables()
		case <-lsm.compactChan:
			lsm.compact(false, true)
			lsm.mergeSsTables()
		case <-lsm.stopChan:
			return
		}
	}
}

func newLsm(rootPath string, logFile *os.File) *Lsm {
	lsm := new(Lsm)
	lsm.nodeMap = make(map[string]*LsmNode)
	lsm.ssTableMap = make(map[int64]*SsTable)
	lsm.rootPath = rootPath
	lsm.logFile = logFile
	lsm.stopChan = make(chan bool)
	lsm.compactChan = make(chan bool, 1)
	lsm.mergeTimer = time.NewTicker(mergeTimeoutMs * time.Millisecond)
	lsm.compactTimer = time.NewTicker(compactTimeoutMs * time.Millisecond)
	return lsm
}

func (lsm *Lsm) start() {
	lsm.wg.Add(1)
	go lsm.Background()
}

func NewLsm(rootPath string) (*Lsm, error) {
	fmt.Printf("New\n")
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

	lsm := newLsm(rootPath, logFile)
	lsm.start()
	return lsm, nil
}

func (lsm *Lsm) getSsTablePath(index int64) string {
	return path.Join(lsm.rootPath, "lsm_"+strconv.FormatInt(index, 10)+".sstable")
}

func (lsm *Lsm) closeSsTables() {
	for _, st := range lsm.ssTableMap {
		st.Close()
	}
}

func (lsm *Lsm) openSsTables() error {
	files, err := ioutil.ReadDir(lsm.rootPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		match := ssTableFileNamePattern.FindStringSubmatch(file.Name())
		if match == nil || len(match) == 1 {
			continue
		}

		index, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			continue
		}

		st, err := openSsTable(lsm.getSsTablePath(index))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		lsm.ssTableMap[index] = st
		if index > lsm.time {
			lsm.time = index
		}
	}

	return nil
}

func (lsm *Lsm) restoreFromLog(logFile *os.File) error {
	for {
		n := new(LsmNode)
		err := n.ReadFrom(logFile)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		lsm.nodeMap[n.key] = n
	}

	return lsm.compact(true, false)
}

func OpenLsm(rootPath string) (*Lsm, error) {
	fmt.Printf("Open\n")
	logFile, err := os.OpenFile(filepath.Join(rootPath, logFileName), os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("open log error %v\n", err)
		return nil, err
	}

	lsm := newLsm(rootPath, logFile)

	err = lsm.openSsTables()
	if err != nil {
		fmt.Printf("open tables error %v\n", err)
		return nil, err
	}

	err = lsm.restoreFromLog(logFile)
	if err != nil {
		fmt.Printf("restore error %v\n", err)
		lsm.closeSsTables()
		logFile.Close()
		return nil, err
	}
	logFile.Close()

	logFile, err = os.OpenFile(filepath.Join(rootPath, logFileName), os.O_APPEND|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		fmt.Printf("open log error %v\n", err)
		lsm.closeSsTables()
		return nil, err
	}
	lsm.logFile = logFile
	lsm.start()
	return lsm, nil
}
