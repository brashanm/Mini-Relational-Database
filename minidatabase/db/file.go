package db

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

// Example “atomic save” to a file (not necessarily used by the B+tree code)
func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	if _, err = fp.Write(data); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = fp.Sync(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Example “append-only” log
func LogCreate(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0664)
}

func LogAppend(fp *os.File, line string) error {
	_, err := fp.WriteString(line + "\n")
	if err != nil {
		return err
	}
	return fp.Sync()
}

func randomInt() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63()
}
