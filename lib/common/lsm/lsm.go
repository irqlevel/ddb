package lsm

import (
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotFound = fmt.Errorf("Not found")
)

const (
	logFileName = "lsm.log"
)

type Lsm struct {
	nodeMap        map[string]*LsmNode
	lock           sync.RWMutex
	rootPath       string
	logFile        *os.File
	ssTableMap     map[int64]*SsTable
	time           int64
	mergeTimer     *time.Ticker
	mergeTimerStop chan bool
	closing        bool
	wg             sync.WaitGroup
}

func (lsm *Lsm) shouldCompact() bool {
	if !lsm.closing && len(lsm.nodeMap) > 10 {
		return true
	}
	return false
}

func (lsm *Lsm) compact(force bool, logTruncate bool) error {
	defer lsm.wg.Done()

	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	if !force && !lsm.shouldCompact() {
		return nil
	}

	lsm.time++
	fmt.Printf("Compacting %d\n", lsm.time)
	st, err := newSsTable(lsm.getSsTablePath(lsm.time), lsm.nodeMap)
	if err != nil {
		return err
	}

	lsm.ssTableMap[lsm.time] = st
	lsm.nodeMap = make(map[string]*LsmNode)
	if logTruncate {
		return lsm.logFile.Truncate(0)
	}
	return nil
}

func (lsm *Lsm) mergeSsTables() error {
	if len(lsm.ssTableMap) <= 1 {
		return nil
	}

	lsm.time++
	fmt.Printf("Merge %d\n", lsm.time)

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

	st, err := mergeSsTable(prevSt, currSt, lsm.getSsTablePath(lsm.time))
	if err != nil {
		return err
	}

	lsm.ssTableMap[lsm.time] = st
	delete(lsm.ssTableMap, prevStId)
	delete(lsm.ssTableMap, currStId)
	prevSt.Erase()
	currSt.Erase()
	return nil
}

func (lsm *Lsm) mergeSsTablesWithLock() error {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	return lsm.mergeSsTables()
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
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

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

	if lsm.shouldCompact() {
		lsm.wg.Add(1)
		go lsm.compact(false, true)
	}

	return nil
}

func (lsm *Lsm) lookupSsTables(key string) (string, error) {
	ids := make([]int64, len(lsm.ssTableMap))
	i := 0
	for id := range lsm.ssTableMap {
		ids[i] = id
		i++
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })

	for _, id := range ids {
		st := lsm.ssTableMap[id]

		fmt.Printf("lookup %s at %d\n", key, id)
		value, err := st.Get(key)
		if err == nil {
			return value, nil
		}
		if err != ErrNotFound {
			return "", err
		}
	}

	return "", ErrNotFound
}

func (lsm *Lsm) Get(key string) (string, error) {
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()
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
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

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
	lsm.lock.Lock()
	lsm.closing = true
	lsm.lock.Unlock()

	lsm.mergeTimerStop <- true

	lsm.mergeTimer.Stop()
	lsm.wg.Wait()

	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	fmt.Printf("Close\n")
	//lsm.mergeSsTables()
	lsm.closeSsTables()
	lsm.logFile.Close()
}

func (lsm *Lsm) Background() {
	defer lsm.wg.Done()

	for {
		select {
		case <-lsm.mergeTimer.C:
			lsm.mergeSsTablesWithLock()
		case <-lsm.mergeTimerStop:
			return
		}
	}
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

	lsm := new(Lsm)
	lsm.nodeMap = make(map[string]*LsmNode)
	lsm.ssTableMap = make(map[int64]*SsTable)
	lsm.rootPath = rootPath
	lsm.logFile = logFile
	lsm.mergeTimerStop = make(chan bool)
	lsm.mergeTimer = time.NewTicker(5 * time.Second)
	lsm.wg.Add(1)
	go lsm.Background()
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
	for i := int64(1); i < math.MaxInt64; i++ {
		st, err := openSsTable(lsm.getSsTablePath(i))
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		lsm.ssTableMap[i] = st
		lsm.time = i
	}
	return nil
}

func (lsm *Lsm) restoreFromLog(logFile *os.File) error {
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

	lsm.wg.Add(1)
	return lsm.compact(true, false)
}

func OpenLsm(rootPath string) (*Lsm, error) {
	fmt.Printf("Open\n")
	logFile, err := os.OpenFile(filepath.Join(rootPath, logFileName), os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("open log error %v\n", err)
		return nil, err
	}

	lsm := new(Lsm)
	lsm.nodeMap = make(map[string]*LsmNode)
	lsm.ssTableMap = make(map[int64]*SsTable)
	lsm.rootPath = rootPath

	err = lsm.openSsTables()
	if err != nil {
		fmt.Printf("open tables error %v\n", err)
		return nil, err
	}

	fmt.Printf("lsm time %d\n", lsm.time)

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
	lsm.mergeTimerStop = make(chan bool)
	lsm.mergeTimer = time.NewTicker(50000 * time.Second)
	lsm.wg.Add(1)
	go lsm.Background()
	return lsm, nil
}
