package lsm

import (
	"bytes"

	"github.com/OneOfOne/xxhash"

	"encoding/binary"
	"fmt"
	"io"
)

var (
	ErrLsmNodeBadMagic    = fmt.Errorf("Lsm node bad magic")
	ErrLsmNodeBadCheckSum = fmt.Errorf("Lsm node bad checksum")
)

const (
	LsmNodeMagic = uint32(0x4CBDABDA)
)

type LsmNode struct {
	key     string
	value   string
	deleted bool
}

func newLsmNode(key string, value string) *LsmNode {
	node := new(LsmNode)
	node.key = key
	node.value = value
	node.deleted = false
	return node
}

func (node *LsmNode) WriteTo(f io.Writer) error {
	key := []byte(node.key)
	value := []byte(node.value)
	deleted := uint32(0)
	if node.deleted {
		deleted = 1
	}

	header := make([]byte, 16+8)
	binary.LittleEndian.PutUint32(header[0:], LsmNodeMagic)
	binary.LittleEndian.PutUint32(header[4:], deleted)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:], uint32(len(value)))

	h := xxhash.New64()
	h.Write(header[0:16])
	h.Write(key)
	h.Write(value)
	copy(header[16:16+8], h.Sum(nil))

	_, err := f.Write(header)
	if err != nil {
		return err
	}

	_, err = f.Write(key)
	if err != nil {
		return err
	}
	_, err = f.Write(value)
	return err
}

func (node *LsmNode) ReadFrom(f io.Reader) error {
	header := make([]byte, 16+8)
	_, err := f.Read(header)
	if err != nil {
		return err
	}

	if binary.LittleEndian.Uint32(header[0:]) != LsmNodeMagic {
		return ErrLsmNodeBadMagic
	}

	keyLength := binary.LittleEndian.Uint32(header[8:])
	valueLength := binary.LittleEndian.Uint32(header[12:])

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)
	_, err = f.Read(key)
	if err != nil {
		return err
	}
	_, err = f.Read(value)
	if err != nil {
		return err
	}

	h := xxhash.New64()
	h.Write(header[0:16])
	h.Write(key)
	h.Write(value)

	if !bytes.Equal(header[16:16+8], h.Sum(nil)) {
		return ErrLsmNodeBadCheckSum
	}

	node.key = string(key)
	node.value = string(value)
	node.deleted = false
	if binary.LittleEndian.Uint32(header[4:]) != 0 {
		node.deleted = true
	}

	return nil
}
