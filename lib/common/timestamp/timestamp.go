package timestamp

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

func GetShortFileName(fileName string) string {
	short := fileName
	for i := len(fileName) - 1; i > 0; i-- {
		if fileName[i] == '/' {
			short = fileName[i+1:]
			break
		}
	}
	return short
}

func GetTimestamp() int64 {
	return time.Now().UnixNano()
}

func GetTimestampString(t int64) string {
	date := time.Unix(0, t).UTC().Format("+0000 UTC 2006-01-02 15:04:05")
	secs := t / 1000000000
	nsecs := t - 1000000000*secs
	usecs := nsecs / 1000

	return strings.Join([]string{date, fmt.Sprintf("%06d", usecs)}, ".")
}

func GetSource(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "???"
		line = 0
	}
	source := fmt.Sprintf("%s:%d:", GetShortFileName(file), line)
	return source
}

func GetTimestampAndSource(skip int) (string, string) {
	return GetTimestampString(GetTimestamp()), GetSource(skip)
}
