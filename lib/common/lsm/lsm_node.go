package lsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

var (
	ErrLsmNodeBadMagic    = fmt.Errorf("Lsm node bad magic")
	ErrLsmNodeBadCheckSum = fmt.Errorf("Lsm node bad checksum")
)

const (
	LsmNodeMagic = int32(0x4CBDABDA)
)

type LsmNodeOnDiskHeader struct {
	Magic       int32
	Deleted     int32
	KeyLength   int32
	ValueLength int32
	CheckSum    [sha256.Size]byte
}

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
	keyBytes := []byte(node.key)
	valueBytes := []byte(node.value)
	deleted := int32(0)
	if node.deleted {
		deleted = int32(1)
	}

	header := &LsmNodeOnDiskHeader{Magic: LsmNodeMagic, Deleted: deleted, KeyLength: int32(len(keyBytes)),
		ValueLength: int32(len(valueBytes))}
	h := sha256.New()

	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, header.Magic)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.Deleted)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.KeyLength)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.ValueLength)
	if err != nil {
		return err
	}

	_, err = h.Write(buf.Bytes())
	if err != nil {
		return err
	}

	_, err = h.Write(keyBytes)
	if err != nil {
		return err
	}

	_, err = h.Write(valueBytes)
	if err != nil {
		return err
	}

	copy(header.CheckSum[:], h.Sum(nil))

	err = binary.Write(buf, binary.LittleEndian, header.CheckSum)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, keyBytes)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, valueBytes)
	if err != nil {
		return err
	}

	_, err = f.Write(buf.Bytes())
	return err
}

func (node *LsmNode) ReadFrom(f io.Reader) error {
	header := &LsmNodeOnDiskHeader{}
	err := binary.Read(f, binary.LittleEndian, header)
	if err != nil {
		return err
	}

	if header.Magic != LsmNodeMagic {
		return ErrLsmNodeBadMagic
	}

	keyBytes := make([]byte, header.KeyLength)
	_, err = f.Read(keyBytes)
	if err != nil {
		return err
	}

	valueBytes := make([]byte, header.ValueLength)
	_, err = f.Read(valueBytes)
	if err != nil {
		return err
	}

	h := sha256.New()

	buf := new(bytes.Buffer)

	err = binary.Write(buf, binary.LittleEndian, header.Magic)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.Deleted)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.KeyLength)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, header.ValueLength)
	if err != nil {
		return err
	}

	_, err = h.Write(buf.Bytes())
	if err != nil {
		return err
	}

	_, err = h.Write(keyBytes)
	if err != nil {
		return err
	}

	_, err = h.Write(valueBytes)
	if err != nil {
		return err
	}

	if !bytes.Equal(header.CheckSum[:], h.Sum(nil)) {
		return ErrLsmNodeBadCheckSum
	}

	node.key = string(keyBytes)
	node.value = string(valueBytes)
	node.deleted = false
	if header.Deleted != 0 {
		node.deleted = true
	}

	return nil
}
