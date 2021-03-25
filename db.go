package raftwal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/ncabatoff/raft-wal/log"
)

var (
	msgPackHandle = &codec.MsgpackHandle{}
	crc32Table    = crc32.MakeTable(crc32.Castagnoli)
)

type LogConfig = log.UserLogConfig

func New(dir string) (*wal, error) {
	return NewWAL(nil, dir, LogConfig{})
}

type FileWALLog interface {
	GetSealedLogPath(index uint64) (string, int, error)
	Close() error
}

var _ FileWALLog = (*wal)(nil)
var _ raft.LogStore = (*wal)(nil)
var _ raft.StableStore = (*wal)(nil)

var errNotFound = errors.New("not found")

type wal struct {
	mu sync.RWMutex

	metaFile *os.File
	meta     *meta

	log    log.Log
	dir    string
	config LogConfig
	logger hclog.Logger
}

func NewWAL(logger hclog.Logger, dir string, c LogConfig) (*wal, error) {
	wal := &wal{
		dir:    dir,
		config: c,
		logger: logger,
	}

	err := wal.restoreMetaPage(filepath.Join(dir, "meta"))
	if err != nil {
		return nil, fmt.Errorf("failed to create meta: %v", err)
	}

	// note that log.NewLog will sync directory, including metapage

	wal.log, err = log.NewLog(logger, dir, log.LogConfig{
		KnownFirstIndex:           wal.meta.FirstIndex,
		FirstIndexUpdatedCallback: wal.setFirstIndex,
		UserLogConfig:             c,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %v", err)
	}

	return wal, nil
}

// SetFirstIndex is used when adding a node to a cluster.  The node will
// be asked to apply a snapshot, and then SetFirstIndex must be called to
// inform us what the next log index will be.
func (w *wal) SetFirstIndex(newIndex uint64) error {
	err := w.setFirstIndex(newIndex)
	if err != nil {
		return err
	}
	return w.log.SetFirstIndex(newIndex)
}

func (w *wal) SetLastIndex(newIndex uint64) {
	w.log.SetLastIndex(newIndex)
}

func (w *wal) setFirstIndex(newIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.setFirstIndexLocked(newIndex)
}

// FirstIndex returns the first index written. 0 for no entries.
func (w *wal) FirstIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.meta.FirstIndex, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *wal) LastIndex() (uint64, error) {
	return w.log.LastIndex(), nil
}

// GetLog gets a log entry at a given index.
func (w *wal) GetLog(index uint64, log *raft.Log) error {
	b, err := w.log.GetLog(index)
	if err != nil {
		return err
	}

	err = codec.NewDecoderBytes(b, msgPackHandle).Decode(log)
	return err
}

// StoreLog stores a log entry.
func (w *wal) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (w *wal) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	encoder := codec.NewEncoderBytes(nil, msgPackHandle)

	lastIndex := logs[0].Index - 1
	i := 0

	var berr error
	bytes := func() []byte {
		if i >= len(logs) {
			return nil
		}

		l := logs[i]
		i++

		if l.Index != lastIndex+1 {
			berr = fmt.Errorf("storing non-consecutive logs: %v != %v", l.Index, lastIndex+1)
			return nil
		}
		lastIndex = l.Index

		var b []byte
		encoder.ResetBytes(&b)
		berr = encoder.Encode(l)
		if berr != nil {
			return nil
		}

		return b
	}

	err := w.log.StoreLogs(logs[0].Index, bytes)
	if err != nil {
		return err
	}

	return berr
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (w *wal) DeleteRange(min, max uint64) error {
	firstIdx, _ := w.FirstIndex()
	lastIdx, _ := w.LastIndex()

	if min <= firstIdx && max > firstIdx && max <= lastIdx {
		return w.log.TruncateHead(max)
	} else if min > firstIdx && max == lastIdx {
		return w.log.TruncateTail(min)
	}

	return fmt.Errorf("deleting mid ranges not supported [%v, %v] is in [%v, %v]",
		min, max, firstIdx, lastIdx)
}

func (w *wal) Close() error {
	return w.log.Close()
}

func (w *wal) GetSealedLogPath(index uint64) (string, int, error) {
	return w.log.GetSealedLogPath(index)
}
