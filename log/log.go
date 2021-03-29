package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const defaultLogChunkSize = 4096

type LogCompression byte

const (
	LogCompressionNone LogCompression = iota
	LogCompressionZlib
	LogCompressionGZip
)

type Log interface {
	FirstIndex() uint64
	LastIndex() uint64

	GetLog(index uint64) ([]byte, error)
	GetSealedLogPath(index uint64) (string, int, error)
	StoreLogs(nextIndex uint64, next func() []byte) error

	TruncateTail(index uint64) error
	TruncateHead(index uint64) error

	Close() error
	SetFirstIndex(uint64) error
	SetLastIndex(uint64)
}

type UserLogConfig struct {
	SegmentChunkSize uint64

	NoSync bool

	Compression LogCompression

	TruncateOnFailure bool
}

type LogConfig struct {
	KnownFirstIndex uint64

	FirstIndexUpdatedCallback func(uint64) error

	UserLogConfig
}

// log manages a collection of segment files on disk.
type log struct {
	mu      sync.RWMutex
	dir     string
	dirFile *os.File
	config  LogConfig

	lf *lockFile

	// firstIndex is the first log index known to Raft
	firstIndex uint64
	// lastIndex is the last log index known; the next StoreLogs call
	// must be for an index of lastIndex+1.
	lastIndex uint64

	segmentBases  []uint64
	activeSegment *segment

	csMu          sync.RWMutex
	cachedSegment *segment

	firstIndexUpdatedCallback func(uint64) error

	logger hclog.Logger
}

func NewLog(logger hclog.Logger, dir string, c LogConfig) (*log, error) {

	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory: %v", err)
	}

	lf, err := newLockFile(filepath.Join(dir, "lock"), c.NoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %v", err)
	}

	bases, err := segmentsIn(dir)
	if err != nil {
		return nil, err
	}

	if c.SegmentChunkSize == 0 {
		c.SegmentChunkSize = defaultLogChunkSize
	}
	if len(bases) == 0 && c.KnownFirstIndex != 0 {
		return nil, fmt.Errorf("new WAL log requires 0 index")
	}

	var active *segment
	if len(bases) == 0 {
		bases = append(bases, 1)
		active, err = openSegment(logger, filepath.Join(dir, segmentName(1)), 1, true, c)
		if err != nil {
			return nil, err
		}

	}

	l := &log{
		dir:                       dir,
		dirFile:                   dirFile,
		lf:                        lf,
		firstIndex:                c.KnownFirstIndex,
		firstIndexUpdatedCallback: c.FirstIndexUpdatedCallback,
		segmentBases:              bases,
		activeSegment:             active,
		config:                    c,
		logger:                    logger,
	}

	// delete old files if they present from older run
	l.deleteOldLogFilesLocked()
	l.redoPendingTransaction()
	l.syncDir()

	return l, nil
}

func (l *log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.firstIndex
}

func (l *log) updateIndexs(firstWriteIndex uint64, count int) error {
	if l.firstIndex == 0 && count != 0 {
		l.firstIndex = firstWriteIndex
		l.lastIndex = uint64(count)
	} else {
		l.lastIndex += uint64(count)
	}

	return nil
}

func (l *log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIndex
}

func (l *log) segmentFor(index uint64) (*segment, error) {
	firstIdx, lastIdx := l.firstIndex, l.lastIndex

	if index < firstIdx {
		return nil, fmt.Errorf("index too small (%d < %d): %w", index, firstIdx, raft.ErrLogNotFound)
	}
	if index > lastIdx {
		return nil, fmt.Errorf("index too big (%d > %d): %w", index, lastIdx, raft.ErrLogNotFound)
	}

	// TODO remove first clause? we should instead create an activeSegment even when restoring based on another node's logs
	if l.activeSegment != nil && index >= l.activeSegment.baseIndex {
		return l.activeSegment, nil
	}

	sBase, err := searchSegmentIndex(l.segmentBases, index)
	if err != nil {
		return nil, err
	}

	l.csMu.Lock()
	defer l.csMu.Unlock()

	if l.cachedSegment != nil && l.cachedSegment.baseIndex == sBase {
		return l.cachedSegment, nil
	}

	seg, err := openSegment(l.logger, filepath.Join(l.dir, segmentName(sBase)), sBase, false, l.config)
	if err != nil {
		return nil, err
	}

	if l.cachedSegment != nil {
		l.cachedSegment.Close()
	}
	l.cachedSegment = seg
	return seg, nil
}

// GetSealedLogPath returns the path to a WAL segment file that contains index,
// as well as how many logs it contains,
// or an error if none is found.  An empty string with nil error is returned
// if the index is part of the active segment.
func (l *log) GetSealedLogPath(index uint64) (string, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.baseIndex <= index {
		return "", 0, nil
	}

	s, err := l.segmentFor(index)
	if err != nil {
		return "", 0, err
	}
	return s.f.Name(), len(s.offsets), nil
}

func (l *log) GetLog(index uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	s, err := l.segmentFor(index)
	if err != nil {
		return nil, raft.ErrLogNotFound
	}

	out := make([]byte, 1024*1024)
	n, err := s.GetLog(index, out)
	if err != nil {
		return nil, err
	}

	return out[:n], nil
}

func (l *log) StoreLogs(nextIndex uint64, next func() []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	//if nextIndex != l.lastIndex+1 {
	//	return fmt.Errorf("out of order insertion %v != %v", nextIndex, l.lastIndex+1)
	//}

	err := l.maybeStartNewSegment()
	if err != nil {
		return err
	}

	entries, err := l.activeSegment.StoreLogs(nextIndex, next)
	if err != nil {
		l.updateIndexs(nextIndex, entries)
		return err
	}

	return l.updateIndexs(nextIndex, entries)
}

func (l *log) maybeStartNewSegment() error {
	s := l.activeSegment

	if s.nextIndex()-s.baseIndex < l.config.SegmentChunkSize {
		return nil
	}

	err := s.Seal()
	if err != nil {
		return err
	}
	s.Close()

	return l.startNewSegment(s.nextIndex())
}

func (l *log) startNewSegment(nextBase uint64) error {
	active, err := openSegment(l.logger, filepath.Join(l.dir, segmentName(nextBase)), nextBase, true, l.config)
	if err != nil {
		return err
	}
	err = l.syncDir()
	if err != nil {
		return err
	}

	l.segmentBases = append(l.segmentBases, nextBase)
	l.activeSegment = active

	return nil

}

func (l *log) TruncateTail(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.lf.startTransaction(command{Type: cmdTruncatingTail, Index: index})
	if err != nil {
		return err
	}

	err = l.truncateTailImpl(index)
	if err != nil {
		return fmt.Errorf("failed to truncate tail: %v", err)
	}

	return l.lf.commit()
}

func (l *log) truncateTailImpl(index uint64) error {

	// make deletion idempotent, so deleting what's already deleted doesn't
	// fail
	if index > l.lastIndex {
		return nil
	}

	if index <= l.firstIndex {
		return fmt.Errorf("deletion would render log empty")
	}

	if index >= l.activeSegment.baseIndex {
		if err := l.activeSegment.truncateTail(index); err != nil {
			return err
		}
		l.lastIndex = index - 1

		return l.lf.commit()
	}

	l.activeSegment.Close()

	idx := segmentContainingIndex(l.segmentBases, index)
	if l.segmentBases[idx] == index {
		idx--
	}

	toKeep, toDelete := l.segmentBases[:idx+1], l.segmentBases[idx+1:]
	for _, sb := range toDelete {
		fp := filepath.Join(l.dir, segmentName(sb))
		if err := os.Remove(fp); err != nil {
			return err
		}
	}
	l.segmentBases = toKeep

	l.clearCachedSegment()

	err := l.startNewSegment(index)
	if err != nil {
		return err
	}
	err = l.syncDir()
	if err != nil {
		return err
	}

	l.lastIndex = index - 1
	return l.lf.commit()
}

// TruncateHead deletes all entries up to and including index. After returning
// a non-nil error, l.firstIndex will be index+1.

// TODO:
// - assumes index falls on a segment boundary
// - when index == l.lastIndex, we're deleting everything, which is fine, but
//   we get left with l.firstIndex = l.lastIndex = 0, which means that
//   subsequent StoreLogs calls will fail with "out of order insertion"
func (l *log) TruncateHead(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// make deletion idempotent, so deleting what's already deleted doesn't
	// fail
	if index < l.firstIndex {
		return nil
	}

	l.firstIndex = index + 1
	if index >= l.lastIndex {
		l.firstIndex = 1
		l.lastIndex = 1
	}

	l.deleteOldLogFilesLocked()
	if l.firstIndex == 1 {
		l.clearCachedSegment()
		err := l.startNewSegment(1)
		if err != nil {
			return err
		}
	}

	err := l.firstIndexUpdatedCallback(l.firstIndex)
	if err != nil {
		return err
	}

	return nil
}

func (l *log) SetFirstIndex(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.firstIndex, l.lastIndex = index, index

	for _, sb := range l.segmentBases {
		fp := filepath.Join(l.dir, segmentName(sb))
		os.Remove(fp)
	}

	l.segmentBases = l.segmentBases[:0]
	l.clearCachedSegment()
	return l.startNewSegment(l.firstIndex)
}

func (l *log) SetLastIndex(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lastIndex = index
}

func (l *log) deleteOldLogFilesLocked() {
	delIdx := segmentContainingIndex(l.segmentBases, l.firstIndex)

	toDelete, toKeep := l.segmentBases[:delIdx], l.segmentBases[delIdx:]

	for _, sb := range toDelete {
		fp := filepath.Join(l.dir, segmentName(sb))
		os.Remove(fp)
	}

	l.segmentBases = toKeep
}

func (l *log) redoPendingTransaction() error {
	c, err := l.lf.currentTransaction()
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}

	switch c.Type {
	case cmdTruncatingTail:
		err := l.truncateTailImpl(c.Index)
		if err != nil {
			return err
		}
		return l.lf.commit()
	default:
		return fmt.Errorf("unknown command: %v", c.Type)
	}
}

func (l *log) clearCachedSegment() error {
	l.csMu.Lock()
	defer l.csMu.Unlock()

	c := l.cachedSegment
	l.cachedSegment = nil

	if c != nil {
		return c.Close()
	}
	return nil
}

func (l *log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.activeSegment.Close()
	berr := l.clearCachedSegment()
	if err != nil {
		return err
	}
	return berr
}

func (l *log) syncDir() error {
	if l.config.NoSync {
		return nil
	}

	return l.dirFile.Sync()
}
