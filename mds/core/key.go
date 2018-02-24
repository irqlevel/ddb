package mds

type Key struct {
	id    string
	value string
	state string
}

func (key *Key) getId() string {
	return key.id
}

func (key *Key) getState() string {
	return key.state
}

func (key *Key) getValue() string {
	return key.value
}

func (mds *Mds) createKey() (*Key, error) {
	return nil, ErrNotImplemented
}

func (mds *Mds) lookupKey(id string) (*Key, error) {
	return nil, ErrNotImplemented
}

func (mds *Mds) deleteKey(id string) error {
	return ErrNotImplemented
}

func (mds *Mds) setKey(id string, value string) error {
	return ErrNotImplemented
}

func (mds *Mds) getKey(id string) (string, error) {
	return "", ErrNotImplemented
}
