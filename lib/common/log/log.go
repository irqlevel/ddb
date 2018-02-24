package log

import (
	"bytes"
	"ddb/lib/common/logbackend"
	"ddb/lib/common/timestamp"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type LogInterface interface {
	Init(b logbackend.LogBackend)

	Sync()

	Shutdown()

	SetFramesToIgnore(framesToIgnore int)

	SetLevel(level int)

	Pf(level int, format string, v ...interface{})

	PfSync(level int, format string, v ...interface{})
}

type logMsg struct {
	complete  chan bool
	payload   bytes.Buffer
	timestamp int64
}

type Log struct {
	LogInterface

	level          int
	framesToIgnore int
	msgChan        chan *logMsg
	source         bool
	time           bool
	systemd        bool
	active         int64
	shutdownLock   sync.RWMutex
	wg             sync.WaitGroup
	backend        logbackend.LogBackend
}

var logMsgPool = sync.Pool{
	New: func() interface{} {
		return new(logMsg)
	},
}

func (log *Log) SetFramesToIgnore(framesToIgnore int) {
	log.framesToIgnore = framesToIgnore
}

func (log *Log) SetLevel(level int) {
	log.level = level
}

func NewLog(b logbackend.LogBackend) *Log {
	log := new(Log)
	log.backend = b
	log.source = true
	log.time = true
	log.systemd = true
	log.msgChan = make(chan *logMsg, 100)
	log.level = 0
	log.framesToIgnore = 3
	atomic.StoreInt64(&log.active, 1)

	log.wg.Add(1)
	go log.output()
	return log
}

func allocMsg() *logMsg {
	return logMsgPool.Get().(*logMsg)
}

func freeMsg(msg *logMsg) {
	msg.payload.Reset()
	logMsgPool.Put(msg)
}

func (log *Log) output() {
	defer log.wg.Done()

	for msg := range log.msgChan {
		if msg.complete != nil { //special msg is just for flush
			log.backend.Sync()
			msg.complete <- true
		} else {
			log.backend.Println(msg.timestamp, msg.payload.String())
			freeMsg(msg)
		}
	}
}

func (log *Log) Sync() {
	if atomic.LoadInt64(&log.active) == 0 {
		return
	}

	log.shutdownLock.RLock()
	defer log.shutdownLock.RUnlock()

	if log.msgChan != nil {
		//flush log by empty message with completion channel
		msg := allocMsg()
		msg.complete = make(chan bool, 1)
		log.msgChan <- msg
		<-msg.complete
		freeMsg(msg)
	}
}

func (log *Log) Shutdown() {
	active := atomic.CompareAndSwapInt64(&log.active, 1, 0)
	if !active {
		return
	}

	log.shutdownLock.Lock()
	defer log.shutdownLock.Unlock()

	close(log.msgChan)
	log.wg.Wait()
	log.msgChan = nil
}

func (log *Log) println(level int, s string) {
	if atomic.LoadInt64(&log.active) == 0 || level > log.level {
		return
	}

	msg := allocMsg()

	if log.systemd {
		msg.payload.WriteByte('<')
		msg.payload.WriteString(strconv.FormatUint((uint64)(level), 10))
		msg.payload.WriteByte('>')
	}

	if log.time {
		msg.timestamp = timestamp.GetTimestamp()
		msg.payload.WriteByte(' ')
		msg.payload.WriteString(timestamp.GetTimestampString(msg.timestamp))
	}

	if log.source {
		msg.payload.WriteByte(' ')
		msg.payload.WriteString(timestamp.GetSource(log.framesToIgnore))
	}

	if log.systemd || log.time || log.source {
		msg.payload.WriteByte(' ')
	}

	msg.payload.WriteString(strings.TrimRight(s, "\n"))

	log.shutdownLock.RLock()
	if log.msgChan != nil {
		log.msgChan <- msg
		log.shutdownLock.RUnlock()
	} else {
		log.shutdownLock.RUnlock()
		freeMsg(msg)
	}
}

func (log *Log) Pf(level int, format string, v ...interface{}) {
	log.println(level, fmt.Sprintf(format, v...))
}

func (log *Log) PfSync(level int, format string, v ...interface{}) {
	log.println(level, fmt.Sprintf(format, v...))
	log.Sync()
}
