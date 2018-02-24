package logbackend

type LogBackend interface {
	Println(timestamp int64, message string) error
	Sync() error
	Shutdown()
}
