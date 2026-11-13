package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raft/logger"
	"github.com/jathurchan/raft/types"
)

const (
	defaultWALSegmentSize = 64 * 1024 * 1024 // 64 MiB
	defaultWALMaxBatch    = 128
	walRecordHeaderSize   = 4 + 4 + 1 + 2 // payloadLen(4) + crc32(4) + type(1) + groupIDLen(2)

	walRecState          byte = 1
	walRecAppend         byte = 2
	walRecTruncateSuffix byte = 3
	walRecTruncatePrefix byte = 4
	walRecResetLog       byte = 5
)

// SharedWALOptions configures a SharedWAL (MultiGroupStorage) instance.
type SharedWALOptions struct {
	// SyncOnAppend controls whether each group-commit batch calls fsync before returning.
	SyncOnAppend bool

	// SegmentSize is the maximum size in bytes of a single WAL segment before rotation.
	// Defaults to 64 MiB if <= 0.
	SegmentSize int64

	// MaxBatchSize is the maximum number of concurrent requests coalesced into a single
	// write() + fsync() batch. Defaults to 128 if <= 0.
	MaxBatchSize int

	// EnableMetrics enables collection of WAL and group-commit metrics.
	EnableMetrics bool
}

// DefaultSharedWALOptions returns optimal default options for SharedWAL.
func DefaultSharedWALOptions() SharedWALOptions {
	return SharedWALOptions{
		SyncOnAppend:  true,
		SegmentSize:   defaultWALSegmentSize,
		MaxBatchSize:  defaultWALMaxBatch,
		EnableMetrics: true,
	}
}

// MultiGroupStorage is an alias for SharedWAL.
type MultiGroupStorage = SharedWAL

// SharedWAL is a unified, segmented Write-Ahead Log with group-commit batched fsync
// shared across multiple Raft groups in a single process.
type SharedWAL struct {
	dir     string
	walDir  string
	snapDir string
	opts    SharedWALOptions
	logger  logger.Logger

	groupsMu sync.RWMutex
	groups   map[string]*walGroupState

	segMu      sync.RWMutex
	segFiles   map[uint64]*os.File
	activeSeg  uint64
	activeFile *os.File
	activeSize int64

	submitCh chan *walWriteReq
	closeCh  chan struct{}
	doneCh   chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once

	// Metrics
	appendOps       atomic.Uint64
	saveStateOps    atomic.Uint64
	batchWrites     atomic.Uint64
	fsyncOps        atomic.Uint64
	coalescedReqs   atomic.Uint64
	walBytesWritten atomic.Uint64
	snapshotSaveOps atomic.Uint64
	snapshotLoadOps atomic.Uint64
}

// NewSharedWAL creates or recovers a SharedWAL in cfg.Dir.
func NewSharedWAL(cfg StorageConfig, opts SharedWALOptions, lg logger.Logger) (*SharedWAL, error) {
	if cfg.Dir == "" {
		return nil, errors.New("storage directory must be specified")
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultWALSegmentSize
	}
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = defaultWALMaxBatch
	}
	if lg == nil {
		lg = logger.NewNoOpLogger()
	}

	walDir := filepath.Join(cfg.Dir, "wal")
	snapDir := filepath.Join(cfg.Dir, "snapshots")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create snapshots directory: %w", err)
	}

	w := &SharedWAL{
		dir:      cfg.Dir,
		walDir:   walDir,
		snapDir:  snapDir,
		opts:     opts,
		logger:   lg.WithComponent("shared-wal"),
		groups:   make(map[string]*walGroupState),
		segFiles: make(map[uint64]*os.File),
		submitCh: make(chan *walWriteReq, opts.MaxBatchSize*2),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	if err := w.recover(); err != nil {
		_ = w.closeFiles()
		return nil, fmt.Errorf("failed to recover shared WAL: %w", err)
	}

	go w.runWriter()
	return w, nil
}

// NewMultiGroupStorage creates a new MultiGroupStorage (alias for NewSharedWAL).
func NewMultiGroupStorage(cfg StorageConfig, opts SharedWALOptions, lg logger.Logger) (*MultiGroupStorage, error) {
	return NewSharedWAL(cfg, opts, lg)
}

// ForGroup returns a Storage adapter for the specified groupID backed by this SharedWAL.
func (w *SharedWAL) ForGroup(groupID string) Storage {
	g := w.getOrCreateGroup(groupID)
	return &groupStorage{
		wal:     w,
		groupID: groupID,
		state:   g,
	}
}

// Close flushes pending writes, closes all WAL segment files, and stops the writer goroutine.
func (w *SharedWAL) Close() error {
	if w.closed.Swap(true) {
		return nil
	}

	w.closeOnce.Do(func() {
		close(w.closeCh)
	})
	<-w.doneCh

	return w.closeFiles()
}

func (w *SharedWAL) closeFiles() error {
	w.segMu.Lock()
	defer w.segMu.Unlock()

	var firstErr error
	if w.activeFile != nil {
		if err := w.activeFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.activeFile = nil
	}
	for id, f := range w.segFiles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(w.segFiles, id)
	}
	return firstErr
}

// GetMetrics returns collected WAL and group-commit metrics.
func (w *SharedWAL) GetMetrics() map[string]uint64 {
	if !w.opts.EnableMetrics {
		return nil
	}
	return map[string]uint64{
		"append_ops":        w.appendOps.Load(),
		"save_state_ops":    w.saveStateOps.Load(),
		"batch_writes":      w.batchWrites.Load(),
		"fsync_ops":         w.fsyncOps.Load(),
		"coalesced_reqs":    w.coalescedReqs.Load(),
		"wal_bytes_written": w.walBytesWritten.Load(),
		"snapshot_save_ops": w.snapshotSaveOps.Load(),
		"snapshot_load_ops": w.snapshotLoadOps.Load(),
	}
}

// ResetMetrics resets all collected WAL metrics.
func (w *SharedWAL) ResetMetrics() {
	w.appendOps.Store(0)
	w.saveStateOps.Store(0)
	w.batchWrites.Store(0)
	w.fsyncOps.Store(0)
	w.coalescedReqs.Store(0)
	w.walBytesWritten.Store(0)
	w.snapshotSaveOps.Store(0)
	w.snapshotLoadOps.Store(0)
}

// GetMetricsSummary returns a human-readable summary of WAL metrics.
func (w *SharedWAL) GetMetricsSummary() string {
	if !w.opts.EnableMetrics {
		return "Storage metrics disabled"
	}
	return fmt.Sprintf(
		"SharedWAL metrics: append_ops=%d save_state_ops=%d batch_writes=%d fsync_ops=%d coalesced_reqs=%d wal_bytes=%d",
		w.appendOps.Load(),
		w.saveStateOps.Load(),
		w.batchWrites.Load(),
		w.fsyncOps.Load(),
		w.coalescedReqs.Load(),
		w.walBytesWritten.Load(),
	)
}

type walEntryMeta struct {
	Index     types.Index
	Term      types.Term
	SegmentID uint64
	CmdOffset int64
	CmdLen    uint32
}

type walGroupState struct {
	writeMu       sync.Mutex // Serializes mutations within this single group
	mu            sync.RWMutex
	hardState     types.PersistentState
	firstLogIndex types.Index
	lastLogIndex  types.Index
	entries       []walEntryMeta
	snapshotMu    sync.RWMutex
}

func (w *SharedWAL) getOrCreateGroup(groupID string) *walGroupState {
	w.groupsMu.RLock()
	g, ok := w.groups[groupID]
	w.groupsMu.RUnlock()
	if ok {
		return g
	}

	w.groupsMu.Lock()
	defer w.groupsMu.Unlock()
	if g, ok = w.groups[groupID]; ok {
		return g
	}
	g = &walGroupState{}
	w.groups[groupID] = g
	return g
}

type walWriteReq struct {
	records        [][]byte
	relCmdOffsets  []int64 // relative offset of command payload within each record, or -1
	cmdLens        []uint32
	segID          uint64
	absCmdOffsets  []int64
	err            error
	done           chan struct{}
}

func (w *SharedWAL) submit(ctx context.Context, req *walWriteReq) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.closed.Load() {
		return ErrStorageIO
	}

	select {
	case w.submitCh <- req:
	case <-w.closeCh:
		return ErrStorageIO
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-req.done:
		return req.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *SharedWAL) runWriter() {
	defer close(w.doneCh)

	batch := make([]*walWriteReq, 0, w.opts.MaxBatchSize)
	var buf bytes.Buffer

	for {
		batch = batch[:0]

		select {
		case first := <-w.submitCh:
			batch = append(batch, first)
		case <-w.closeCh:
			// Drain remaining queued requests before exiting.
			for len(batch) < w.opts.MaxBatchSize {
				select {
				case req := <-w.submitCh:
					batch = append(batch, req)
				default:
					if len(batch) > 0 {
						w.flushBatch(batch, &buf)
					}
					return
				}
			}
			if len(batch) > 0 {
				w.flushBatch(batch, &buf)
			}
			return
		}

		// Coalesce all immediately available requests into this batch.
		for len(batch) < w.opts.MaxBatchSize {
			select {
			case req := <-w.submitCh:
				batch = append(batch, req)
			default:
				goto flush
			}
		}

	flush:
		w.flushBatch(batch, &buf)
	}
}

func (w *SharedWAL) flushBatch(batch []*walWriteReq, buf *bytes.Buffer) {
	if w.activeSize >= w.opts.SegmentSize {
		if err := w.rotateSegment(); err != nil {
			for _, req := range batch {
				req.err = fmt.Errorf("%w: rotate segment: %v", ErrStorageIO, err)
				close(req.done)
			}
			return
		}
	}

	buf.Reset()
	baseOffset := w.activeSize
	segID := w.activeSeg

	for _, req := range batch {
		req.segID = segID
		req.absCmdOffsets = make([]int64, len(req.records))
		for i, rec := range req.records {
			recStart := baseOffset + int64(buf.Len())
			if req.relCmdOffsets[i] >= 0 {
				req.absCmdOffsets[i] = recStart + req.relCmdOffsets[i]
			} else {
				req.absCmdOffsets[i] = -1
			}
			buf.Write(rec)
		}
	}

	data := buf.Bytes()
	n, err := w.activeFile.Write(data)
	if err != nil {
		for _, req := range batch {
			req.err = fmt.Errorf("%w: wal write: %v", ErrStorageIO, err)
			close(req.done)
		}
		return
	}
	w.activeSize += int64(n)

	if w.opts.SyncOnAppend {
		if err := w.activeFile.Sync(); err != nil {
			for _, req := range batch {
				req.err = fmt.Errorf("%w: wal sync: %v", ErrStorageIO, err)
				close(req.done)
			}
			return
		}
		if w.opts.EnableMetrics {
			w.fsyncOps.Add(1)
		}
	}

	if w.opts.EnableMetrics {
		w.batchWrites.Add(1)
		w.coalescedReqs.Add(uint64(len(batch)))
		w.walBytesWritten.Add(uint64(n))
	}

	for _, req := range batch {
		close(req.done)
	}
}

func (w *SharedWAL) segmentPath(segID uint64) string {
	return filepath.Join(w.walDir, fmt.Sprintf("wal-%010d.seg", segID))
}

func (w *SharedWAL) rotateSegment() error {
	if w.activeFile != nil {
		if err := w.activeFile.Sync(); err != nil {
			return err
		}
		if err := w.activeFile.Close(); err != nil {
			return err
		}
	}

	nextSeg := w.activeSeg + 1
	path := w.segmentPath(nextSeg)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	rf, err := os.Open(path)
	if err != nil {
		_ = f.Close()
		return err
	}

	w.segMu.Lock()
	w.activeSeg = nextSeg
	w.activeFile = f
	w.activeSize = 0
	w.segFiles[nextSeg] = rf
	w.segMu.Unlock()

	return nil
}

func (w *SharedWAL) recover() error {
	entries, err := os.ReadDir(w.walDir)
	if err != nil {
		return err
	}

	var segIDs []uint64
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".seg")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue
		}
		segIDs = append(segIDs, id)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	if len(segIDs) == 0 {
		w.activeSeg = 0
		return w.rotateSegment()
	}

	for _, segID := range segIDs {
		path := w.segmentPath(segID)
		validEnd, err := w.replaySegment(segID, path)
		if err != nil {
			return err
		}
		if err := os.Truncate(path, validEnd); err != nil {
			return err
		}
		rf, err := os.Open(path)
		if err != nil {
			return err
		}
		w.segFiles[segID] = rf
	}

	lastSeg := segIDs[len(segIDs)-1]
	lastPath := w.segmentPath(lastSeg)
	info, err := os.Stat(lastPath)
	if err != nil {
		return err
	}

	af, err := os.OpenFile(lastPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.activeSeg = lastSeg
	w.activeFile = af
	w.activeSize = info.Size()
	return nil
}

func (w *SharedWAL) replaySegment(segID uint64, path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var offset int64
	var header [8]byte

	for {
		_, err := io.ReadFull(f, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}

		bodyLen := binary.BigEndian.Uint32(header[0:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:8])
		if bodyLen < 3 {
			// Corrupt/partial record at tail; stop replay cleanly.
			return offset, nil
		}

		body := make([]byte, bodyLen)
		_, err = io.ReadFull(f, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}

		if crc32.ChecksumIEEE(body) != expectedCRC {
			// Checksum mismatch at tail due to incomplete write; truncate here.
			return offset, nil
		}

		recType := body[0]
		gidLen := int(binary.BigEndian.Uint16(body[1:3]))
		if 3+gidLen > len(body) {
			return offset, nil
		}
		groupID := string(body[3 : 3+gidLen])
		payload := body[3+gidLen:]
		payloadStartInFile := offset + 8 + 3 + int64(gidLen)

		g := w.getOrCreateGroup(groupID)
		switch recType {
		case walRecState:
			if len(payload) < 10 {
				return offset, nil
			}
			term := types.Term(binary.BigEndian.Uint64(payload[0:8]))
			vLen := int(binary.BigEndian.Uint16(payload[8:10]))
			if 10+vLen != len(payload) {
				return offset, nil
			}
			g.hardState = types.PersistentState{
				CurrentTerm: term,
				VotedFor:    types.NodeID(payload[10 : 10+vLen]),
			}

		case walRecAppend:
			if len(payload) < 20 {
				return offset, nil
			}
			idx := types.Index(binary.BigEndian.Uint64(payload[0:8]))
			term := types.Term(binary.BigEndian.Uint64(payload[8:16]))
			cmdLen := binary.BigEndian.Uint32(payload[16:20])
			if 20+int(cmdLen) != len(payload) {
				return offset, nil
			}
			cmdOffset := payloadStartInFile + 20
			meta := walEntryMeta{
				Index:     idx,
				Term:      term,
				SegmentID: segID,
				CmdOffset: cmdOffset,
				CmdLen:    cmdLen,
			}
			if g.firstLogIndex == 0 {
				g.firstLogIndex = idx
			}
			g.entries = append(g.entries, meta)
			g.lastLogIndex = idx

		case walRecTruncateSuffix:
			if len(payload) != 8 {
				return offset, nil
			}
			idx := types.Index(binary.BigEndian.Uint64(payload[0:8]))
			if g.firstLogIndex != 0 && idx >= g.firstLogIndex && idx <= g.lastLogIndex {
				cut := int(idx - g.firstLogIndex)
				g.entries = g.entries[:cut]
				if len(g.entries) == 0 {
					g.firstLogIndex = 0
					g.lastLogIndex = 0
				} else {
					g.lastLogIndex = g.entries[len(g.entries)-1].Index
				}
			}

		case walRecTruncatePrefix:
			if len(payload) != 8 {
				return offset, nil
			}
			idx := types.Index(binary.BigEndian.Uint64(payload[0:8]))
			if g.firstLogIndex != 0 && idx > g.firstLogIndex {
				if idx > g.lastLogIndex {
					g.entries = nil
					g.firstLogIndex = 0
					g.lastLogIndex = 0
				} else {
					cut := int(idx - g.firstLogIndex)
					g.entries = append([]walEntryMeta(nil), g.entries[cut:]...)
					g.firstLogIndex = idx
				}
			}

		case walRecResetLog:
			if len(payload) < 20 {
				return offset, nil
			}
			idx := types.Index(binary.BigEndian.Uint64(payload[0:8]))
			term := types.Term(binary.BigEndian.Uint64(payload[8:16]))
			cmdLen := binary.BigEndian.Uint32(payload[16:20])
			if 20+int(cmdLen) != len(payload) {
				return offset, nil
			}
			cmdOffset := payloadStartInFile + 20
			g.entries = []walEntryMeta{{
				Index:     idx,
				Term:      term,
				SegmentID: segID,
				CmdOffset: cmdOffset,
				CmdLen:    cmdLen,
			}}
			g.firstLogIndex = idx
			g.lastLogIndex = idx
		}

		offset += 8 + int64(bodyLen)
	}
}

func (w *SharedWAL) readCommand(meta walEntryMeta) ([]byte, error) {
	if meta.CmdLen == 0 {
		return nil, nil
	}

	w.segMu.RLock()
	f, ok := w.segFiles[meta.SegmentID]
	w.segMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: segment %d not found", ErrStorageIO, meta.SegmentID)
	}

	cmd := make([]byte, meta.CmdLen)
	if _, err := f.ReadAt(cmd, meta.CmdOffset); err != nil {
		return nil, fmt.Errorf("%w: read command: %v", ErrStorageIO, err)
	}
	return cmd, nil
}

func encodeWALRecord(recType byte, groupID string, payload []byte) ([]byte, int64) {
	gidBytes := []byte(groupID)
	bodyLen := 1 + 2 + len(gidBytes) + len(payload)
	rec := make([]byte, 8+bodyLen)

	binary.BigEndian.PutUint32(rec[0:4], uint32(bodyLen))
	rec[8] = recType
	binary.BigEndian.PutUint16(rec[9:11], uint16(len(gidBytes)))
	copy(rec[11:11+len(gidBytes)], gidBytes)
	copy(rec[11+len(gidBytes):], payload)

	crc := crc32.ChecksumIEEE(rec[8:])
	binary.BigEndian.PutUint32(rec[4:8], crc)

	payloadStart := int64(11 + len(gidBytes))
	return rec, payloadStart
}

func sanitizeGroupID(groupID string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", "..", "_")
	return replacer.Replace(groupID)
}

// groupStorage implements storage.Storage for a single Raft group backed by SharedWAL.
type groupStorage struct {
	wal     *SharedWAL
	groupID string
	state   *walGroupState
}

func (gs *groupStorage) SaveState(ctx context.Context, st types.PersistentState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}

	gs.state.writeMu.Lock()
	defer gs.state.writeMu.Unlock()

	votedBytes := []byte(st.VotedFor)
	payload := make([]byte, 8+2+len(votedBytes))
	binary.BigEndian.PutUint64(payload[0:8], uint64(st.CurrentTerm))
	binary.BigEndian.PutUint16(payload[8:10], uint16(len(votedBytes)))
	copy(payload[10:], votedBytes)

	rec, _ := encodeWALRecord(walRecState, gs.groupID, payload)
	req := &walWriteReq{
		records:       [][]byte{rec},
		relCmdOffsets: []int64{-1},
		done:          make(chan struct{}),
	}

	if err := gs.wal.submit(ctx, req); err != nil {
		return err
	}

	gs.state.mu.Lock()
	gs.state.hardState = st
	gs.state.mu.Unlock()

	if gs.wal.opts.EnableMetrics {
		gs.wal.saveStateOps.Add(1)
	}
	return nil
}

func (gs *groupStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	if err := ctx.Err(); err != nil {
		return types.PersistentState{}, err
	}
	if gs.wal.closed.Load() {
		return types.PersistentState{}, ErrStorageIO
	}

	gs.state.mu.RLock()
	st := gs.state.hardState
	gs.state.mu.RUnlock()
	return st, nil
}

func (gs *groupStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}
	if len(entries) == 0 {
		return ErrEmptyEntries
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			if entries[i].Index <= entries[i-1].Index {
				return ErrOutOfOrderEntries
			}
			return ErrNonContiguousEntries
		}
	}

	gs.state.writeMu.Lock()
	defer gs.state.writeMu.Unlock()

	gs.state.mu.RLock()
	lastIdx := gs.state.lastLogIndex
	gs.state.mu.RUnlock()

	if lastIdx > 0 && entries[0].Index != lastIdx+1 {
		return ErrNonContiguousEntries
	}

	records := make([][]byte, len(entries))
	relOffsets := make([]int64, len(entries))
	cmdLens := make([]uint32, len(entries))

	for i, e := range entries {
		payload := make([]byte, 20+len(e.Command))
		binary.BigEndian.PutUint64(payload[0:8], uint64(e.Index))
		binary.BigEndian.PutUint64(payload[8:16], uint64(e.Term))
		binary.BigEndian.PutUint32(payload[16:20], uint32(len(e.Command)))
		copy(payload[20:], e.Command)

		rec, payloadStart := encodeWALRecord(walRecAppend, gs.groupID, payload)
		records[i] = rec
		relOffsets[i] = payloadStart + 20
		cmdLens[i] = uint32(len(e.Command))
	}

	req := &walWriteReq{
		records:       records,
		relCmdOffsets: relOffsets,
		cmdLens:       cmdLens,
		done:          make(chan struct{}),
	}

	if err := gs.wal.submit(ctx, req); err != nil {
		return err
	}

	gs.state.mu.Lock()
	if gs.state.firstLogIndex == 0 {
		gs.state.firstLogIndex = entries[0].Index
	}
	for i, e := range entries {
		gs.state.entries = append(gs.state.entries, walEntryMeta{
			Index:     e.Index,
			Term:      e.Term,
			SegmentID: req.segID,
			CmdOffset: req.absCmdOffsets[i],
			CmdLen:    cmdLens[i],
		})
	}
	gs.state.lastLogIndex = entries[len(entries)-1].Index
	gs.state.mu.Unlock()

	if gs.wal.opts.EnableMetrics {
		gs.wal.appendOps.Add(1)
	}
	return nil
}

func (gs *groupStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if gs.wal.closed.Load() {
		return nil, ErrStorageIO
	}
	if start >= end {
		return nil, ErrInvalidLogRange
	}

	gs.state.mu.RLock()
	if gs.state.firstLogIndex == 0 || start < gs.state.firstLogIndex || end-1 > gs.state.lastLogIndex {
		gs.state.mu.RUnlock()
		return nil, ErrIndexOutOfRange
	}
	from := int(start - gs.state.firstLogIndex)
	to := int(end - gs.state.firstLogIndex)
	metas := make([]walEntryMeta, to-from)
	copy(metas, gs.state.entries[from:to])
	gs.state.mu.RUnlock()

	result := make([]types.LogEntry, len(metas))
	for i, m := range metas {
		cmd, err := gs.wal.readCommand(m)
		if err != nil {
			return nil, err
		}
		result[i] = types.LogEntry{
			Index:   m.Index,
			Term:    m.Term,
			Command: cmd,
		}
	}
	return result, nil
}

func (gs *groupStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return types.LogEntry{}, err
	}
	if gs.wal.closed.Load() {
		return types.LogEntry{}, ErrStorageIO
	}

	gs.state.mu.RLock()
	if gs.state.firstLogIndex == 0 || index < gs.state.firstLogIndex || index > gs.state.lastLogIndex {
		gs.state.mu.RUnlock()
		return types.LogEntry{}, ErrEntryNotFound
	}
	meta := gs.state.entries[int(index-gs.state.firstLogIndex)]
	gs.state.mu.RUnlock()

	cmd, err := gs.wal.readCommand(meta)
	if err != nil {
		return types.LogEntry{}, err
	}
	return types.LogEntry{
		Index:   meta.Index,
		Term:    meta.Term,
		Command: cmd,
	}, nil
}

func (gs *groupStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}

	gs.state.writeMu.Lock()
	defer gs.state.writeMu.Unlock()

	gs.state.mu.RLock()
	first := gs.state.firstLogIndex
	last := gs.state.lastLogIndex
	gs.state.mu.RUnlock()

	if last == 0 || index > last || index < first {
		return ErrIndexOutOfRange
	}

	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(index))
	rec, _ := encodeWALRecord(walRecTruncateSuffix, gs.groupID, payload[:])

	req := &walWriteReq{
		records:       [][]byte{rec},
		relCmdOffsets: []int64{-1},
		done:          make(chan struct{}),
	}
	if err := gs.wal.submit(ctx, req); err != nil {
		return err
	}

	gs.state.mu.Lock()
	cut := int(index - gs.state.firstLogIndex)
	gs.state.entries = gs.state.entries[:cut]
	if len(gs.state.entries) == 0 {
		gs.state.firstLogIndex = 0
		gs.state.lastLogIndex = 0
	} else {
		gs.state.lastLogIndex = gs.state.entries[len(gs.state.entries)-1].Index
	}
	gs.state.mu.Unlock()
	return nil
}

func (gs *groupStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}

	gs.state.writeMu.Lock()
	defer gs.state.writeMu.Unlock()

	gs.state.mu.RLock()
	first := gs.state.firstLogIndex
	last := gs.state.lastLogIndex
	gs.state.mu.RUnlock()

	if first == 0 || index < first {
		return ErrIndexOutOfRange
	}
	if index == first {
		return nil
	}

	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(index))
	rec, _ := encodeWALRecord(walRecTruncatePrefix, gs.groupID, payload[:])

	req := &walWriteReq{
		records:       [][]byte{rec},
		relCmdOffsets: []int64{-1},
		done:          make(chan struct{}),
	}
	if err := gs.wal.submit(ctx, req); err != nil {
		return err
	}

	gs.state.mu.Lock()
	if index > last {
		gs.state.entries = nil
		gs.state.firstLogIndex = 0
		gs.state.lastLogIndex = 0
	} else {
		cut := int(index - gs.state.firstLogIndex)
		gs.state.entries = append([]walEntryMeta(nil), gs.state.entries[cut:]...)
		gs.state.firstLogIndex = index
	}
	gs.state.mu.Unlock()
	return nil
}

func (gs *groupStorage) ResetLog(ctx context.Context, entry types.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}

	gs.state.writeMu.Lock()
	defer gs.state.writeMu.Unlock()

	payload := make([]byte, 20+len(entry.Command))
	binary.BigEndian.PutUint64(payload[0:8], uint64(entry.Index))
	binary.BigEndian.PutUint64(payload[8:16], uint64(entry.Term))
	binary.BigEndian.PutUint32(payload[16:20], uint32(len(entry.Command)))
	copy(payload[20:], entry.Command)

	rec, payloadStart := encodeWALRecord(walRecResetLog, gs.groupID, payload)
	req := &walWriteReq{
		records:       [][]byte{rec},
		relCmdOffsets: []int64{payloadStart + 20},
		cmdLens:       []uint32{uint32(len(entry.Command))},
		done:          make(chan struct{}),
	}
	if err := gs.wal.submit(ctx, req); err != nil {
		return err
	}

	gs.state.mu.Lock()
	gs.state.entries = []walEntryMeta{{
		Index:     entry.Index,
		Term:      entry.Term,
		SegmentID: req.segID,
		CmdOffset: req.absCmdOffsets[0],
		CmdLen:    uint32(len(entry.Command)),
	}}
	gs.state.firstLogIndex = entry.Index
	gs.state.lastLogIndex = entry.Index
	gs.state.mu.Unlock()
	return nil
}

func (gs *groupStorage) groupSnapDir() string {
	return filepath.Join(gs.wal.snapDir, sanitizeGroupID(gs.groupID))
}

func (gs *groupStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gs.wal.closed.Load() {
		return ErrStorageIO
	}

	gs.state.snapshotMu.Lock()
	defer gs.state.snapshotMu.Unlock()

	dir := gs.groupSnapDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: mkdir snapshot dir: %v", ErrStorageIO, err)
	}

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("%w: marshal snapshot meta: %v", ErrStorageIO, err)
	}

	if err := writeFileAtomic(filepath.Join(dir, snapshotDataFilename), data); err != nil {
		return fmt.Errorf("%w: write snapshot data: %v", ErrStorageIO, err)
	}
	if err := writeFileAtomic(filepath.Join(dir, snapshotMetaFilename), metaBytes); err != nil {
		return fmt.Errorf("%w: write snapshot meta: %v", ErrStorageIO, err)
	}

	if gs.wal.opts.EnableMetrics {
		gs.wal.snapshotSaveOps.Add(1)
	}
	return nil
}

func (gs *groupStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	if err := ctx.Err(); err != nil {
		return types.SnapshotMetadata{}, nil, err
	}
	if gs.wal.closed.Load() {
		return types.SnapshotMetadata{}, nil, ErrStorageIO
	}

	gs.state.snapshotMu.RLock()
	defer gs.state.snapshotMu.RUnlock()

	dir := gs.groupSnapDir()
	metaBytes, err := os.ReadFile(filepath.Join(dir, snapshotMetaFilename))
	if err != nil {
		if os.IsNotExist(err) {
			return types.SnapshotMetadata{}, nil, ErrNoSnapshot
		}
		return types.SnapshotMetadata{}, nil, fmt.Errorf("%w: read snapshot meta: %v", ErrStorageIO, err)
	}

	var meta types.SnapshotMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return types.SnapshotMetadata{}, nil, fmt.Errorf("%w: %v", ErrCorruptedSnapshot, err)
	}

	data, err := os.ReadFile(filepath.Join(dir, snapshotDataFilename))
	if err != nil {
		if os.IsNotExist(err) {
			return types.SnapshotMetadata{}, nil, ErrNoSnapshot
		}
		return types.SnapshotMetadata{}, nil, fmt.Errorf("%w: read snapshot data: %v", ErrStorageIO, err)
	}

	if gs.wal.opts.EnableMetrics {
		gs.wal.snapshotLoadOps.Add(1)
	}
	return meta, data, nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (gs *groupStorage) LastLogIndex() types.Index {
	gs.state.mu.RLock()
	idx := gs.state.lastLogIndex
	gs.state.mu.RUnlock()
	return idx
}

func (gs *groupStorage) FirstLogIndex() types.Index {
	gs.state.mu.RLock()
	idx := gs.state.firstLogIndex
	gs.state.mu.RUnlock()
	return idx
}

// Close is a no-op on the per-group Storage adapter; the SharedWAL owns the underlying files.
func (gs *groupStorage) Close() error {
	return nil
}

func (gs *groupStorage) ResetMetrics() {
	gs.wal.ResetMetrics()
}

func (gs *groupStorage) GetMetrics() map[string]uint64 {
	return gs.wal.GetMetrics()
}

func (gs *groupStorage) GetMetricsSummary() string {
	return gs.wal.GetMetricsSummary()
}
