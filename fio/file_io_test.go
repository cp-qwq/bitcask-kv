package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}
func TestNewFileIOManger(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "b.data")
	fio, err := NewFileIOManager(filepath.Join("/tmp", "b.data"))
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcast kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "c.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "d.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "e.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}