package lsm

import (
	"ddb/lib/common/filelog"
	"ddb/lib/common/log"
	"ddb/lib/common/random"
	"io/ioutil"
	"os"
	"testing"
)

func TestLsmNodeReadWrite(t *testing.T) {

	f, err := ioutil.TempFile("", "TestLsmNodeReadWrite_"+random.GenerateRandomHexString(5))
	if err != nil {
		t.Fatalf("Can't create temporary file")
		return
	}
	defer os.Remove(f.Name())
	defer f.Close()

	n := newLsmNode(random.GenerateRandomHexString(64), random.GenerateRandomHexString(128))
	err = n.WriteTo(f)
	if err != nil {
		t.Fatalf("Can't write node error %v", err)
		return
	}

	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatalf("Can't seek to begining of file error %v", err)
		return
	}

	rn := newLsmNode("", "")
	err = rn.ReadFrom(f)
	if err != nil {
		t.Fatalf("Can't read node error %v", err)
		return
	}

	if n.key != rn.key {
		t.Fatalf("Inconsistent key")
		return
	}

	if n.value != rn.value {
		t.Fatalf("Inconsistent value")
		return
	}
}

func TestLsmCreateOpen(t *testing.T) {
	rootPath, err := ioutil.TempDir("", "TestLsmCreateOpen_"+random.GenerateRandomHexString(5))
	if err != nil {
		t.Fatalf("Can't create tmp dir error %v", err)
		return
	}
	//defer os.RemoveAll(rootPath)

	log := log.NewLog(filelog.NewFileLogWithFile(os.Stdout))
	defer log.Sync()

	lsm, err := NewLsm(log, rootPath)
	if err != nil {
		t.Fatalf("Can't create lsm error %v", err)
		return
	}

	kv := make(map[string]string)
	for i := 0; i < 100000; i++ {
		kv[random.GenerateRandomHexString(16)] = random.GenerateRandomHexString(64)
	}

	i := 0
	for key, value := range kv {
		err = lsm.Set(key, value)
		if err != nil {
			t.Fatalf("Can't set lsm key error %v", err)
			lsm.Close()
			return
		}
		if i%1000 == 0 {
			log.Pf(0, "Set key %d", i)
		}
		i++
	}
	lsm.Close()

	lsm, err = OpenLsm(log, rootPath)
	if err != nil {
		t.Fatalf("Can't open lsm error %v", err)
		return
	}
	defer lsm.Close()

	keysToDelete := make(map[string]bool)
	i = 0
	for key := range kv {
		keysToDelete[key] = true
		i++
		if i > len(kv)/3 {
			break
		}
	}

	i = 0
	for key := range keysToDelete {
		err = lsm.Delete(key)
		if err != nil {
			t.Fatalf("Can't del lsm key %s error %v", key, err)
			return
		}
		if i%1000 == 0 {
			log.Pf(0, "Del key %d", i)
		}
		i++
	}

	i = 0
	for key, value := range kv {
		evalue, err := lsm.Get(key)
		if err != nil {
			if err == ErrNotFound {
				_, ok := keysToDelete[key]
				if ok {
					continue
				}
			}
			t.Fatalf("Can't get lsm key %s error %v", key, err)
			return
		}

		if i%1000 == 0 {
			log.Pf(0, "Get key %d", i)
		}
		i++

		_, ok := keysToDelete[key]
		if ok {
			t.Fatalf("Key %s already deleted", key)
			return
		}

		if evalue != value {
			t.Fatalf("Inconsistent value")
			return
		}
	}
}
