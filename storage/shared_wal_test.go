package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jathurchan/raft/logger"
	"github.com/jathurchan/raft/testutil"
	"github.com/jathurchan/raft/types"
)

func TestSharedWAL_GroupIsolationAndCRUD(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewSharedWAL(StorageConfig{Dir: dir}, DefaultSharedWALOptions(), logger.NewNoOpLogger())
	testutil.AssertNoError(t, err)
	defer func() { _ = wal.Close() }()

	ctx := context.Background()
	stA := wal.ForGroup("group-a")
	stB := wal.ForGroup("group-b")

	// Empty state & bounds
	sA, err := stA.LoadState(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.PersistentState{}, sA)
	testutil.AssertEqual(t, types.Index(0), stA.FirstLogIndex())
	testutil.AssertEqual(t, types.Index(0), stA.LastLogIndex())

	// SaveState isolation
	testutil.AssertNoError(t, stA.SaveState(ctx, types.PersistentState{CurrentTerm: 3, VotedFor: "node-1"}))
	testutil.AssertNoError(t, stB.SaveState(ctx, types.PersistentState{CurrentTerm: 7, VotedFor: "node-2"}))

	sA, err = stA.LoadState(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Term(3), sA.CurrentTerm)
	testutil.AssertEqual(t, types.NodeID("node-1"), sA.VotedFor)

	sB, err := stB.LoadState(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Term(7), sB.CurrentTerm)
	testutil.AssertEqual(t, types.NodeID("node-2"), sB.VotedFor)

	// Error validations
	testutil.AssertErrorIs(t, stA.AppendLogEntries(ctx, nil), ErrEmptyEntries)
	testutil.AssertErrorIs(t, stA.AppendLogEntries(ctx, []types.LogEntry{
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 1, Term: 1, Command: []byte("a")},
	}), ErrOutOfOrderEntries)

	// Append entries to both groups
	testutil.AssertNoError(t, stA.AppendLogEntries(ctx, []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("a1")},
		{Index: 2, Term: 1, Command: []byte("a2")},
		{Index: 3, Term: 2, Command: []byte("a3")},
	}))
	testutil.AssertNoError(t, stB.AppendLogEntries(ctx, []types.LogEntry{
		{Index: 1, Term: 5, Command: []byte("b1")},
		{Index: 2, Term: 5, Command: []byte("b2")},
	}))

	testutil.AssertErrorIs(t, stA.AppendLogEntries(ctx, []types.LogEntry{
		{Index: 5, Term: 2, Command: []byte("gap")},
	}), ErrNonContiguousEntries)

	// Read back
	eA2, err := stA.GetLogEntry(ctx, 2)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte("a2"), eA2.Command)

	rangeA, err := stA.GetLogEntries(ctx, 1, 4)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, 3, len(rangeA))
	testutil.AssertEqual(t, []byte("a3"), rangeA[2].Command)

	_, err = stA.GetLogEntry(ctx, 99)
	testutil.AssertErrorIs(t, err, ErrEntryNotFound)
	_, err = stA.GetLogEntries(ctx, 3, 3)
	testutil.AssertErrorIs(t, err, ErrInvalidLogRange)

	// Truncate suffix on A, B unaffected
	testutil.AssertNoError(t, stA.TruncateLogSuffix(ctx, 3))
	testutil.AssertEqual(t, types.Index(2), stA.LastLogIndex())
	testutil.AssertEqual(t, types.Index(2), stB.LastLogIndex())

	// Truncate prefix on A
	testutil.AssertNoError(t, stA.TruncateLogPrefix(ctx, 2))
	testutil.AssertEqual(t, types.Index(2), stA.FirstLogIndex())
	_, err = stA.GetLogEntry(ctx, 1)
	testutil.AssertErrorIs(t, err, ErrEntryNotFound)

	// ResetLog on B
	testutil.AssertNoError(t, stB.ResetLog(ctx, types.LogEntry{Index: 10, Term: 8, Command: []byte("snap-reset")}))
	testutil.AssertEqual(t, types.Index(10), stB.FirstLogIndex())
	testutil.AssertEqual(t, types.Index(10), stB.LastLogIndex())
	eB10, err := stB.GetLogEntry(ctx, 10)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte("snap-reset"), eB10.Command)

	// Per-group Close is a no-op
	testutil.AssertNoError(t, stA.Close())
	_, err = stB.GetLogEntry(ctx, 10)
	testutil.AssertNoError(t, err)
}

func TestSharedWAL_GroupCommitBatching(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultSharedWALOptions()
	opts.SyncOnAppend = true
	opts.MaxBatchSize = 64

	wal, err := NewSharedWAL(StorageConfig{Dir: dir}, opts, logger.NewNoOpLogger())
	testutil.AssertNoError(t, err)
	defer func() { _ = wal.Close() }()

	ctx := context.Background()
	const numGroups = 6
	const opsPerGroup = 10

	var wg sync.WaitGroup
	startGate := make(chan struct{})

	for g := 0; g < numGroups; g++ {
		st := wal.ForGroup(fmt.Sprintf("group-%d", g))
		wg.Add(1)
		go func(st Storage, gid int) {
			defer wg.Done()
			<-startGate
			for i := 1; i <= opsPerGroup; i++ {
				err := st.AppendLogEntries(ctx, []types.LogEntry{{
					Index:   types.Index(i),
					Term:    1,
					Command: []byte(fmt.Sprintf("g%d-cmd-%d", gid, i)),
				}})
				testutil.AssertNoError(t, err)
			}
		}(st, g)
	}

	close(startGate)
	wg.Wait()

	metrics := wal.GetMetrics()
	totalAppends := metrics["append_ops"]
	fsyncs := metrics["fsync_ops"]
	testutil.AssertEqual(t, uint64(numGroups*opsPerGroup), totalAppends)
	testutil.AssertTrue(t, fsyncs > 0 && fsyncs < totalAppends,
		"expected group commit to coalesce fsyncs (%d fsyncs for %d appends)", fsyncs, totalAppends)
}

func TestSharedWAL_CrashRecoveryAndSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultSharedWALOptions()
	opts.SegmentSize = 256 // Force frequent segment rotation
	opts.SyncOnAppend = true

	wal, err := NewSharedWAL(StorageConfig{Dir: dir}, opts, logger.NewNoOpLogger())
	testutil.AssertNoError(t, err)

	ctx := context.Background()
	st1 := wal.ForGroup("shard-1")
	st2 := wal.ForGroup("shard-2")
	st3 := wal.ForGroup("shard-3")

	testutil.AssertNoError(t, st1.SaveState(ctx, types.PersistentState{CurrentTerm: 2, VotedFor: "n1"}))
	testutil.AssertNoError(t, st2.SaveState(ctx, types.PersistentState{CurrentTerm: 4, VotedFor: "n2"}))

	for i := 1; i <= 15; i++ {
		testutil.AssertNoError(t, st1.AppendLogEntries(ctx, []types.LogEntry{
			{Index: types.Index(i), Term: 2, Command: []byte(fmt.Sprintf("s1-%d", i))},
		}))
		testutil.AssertNoError(t, st2.AppendLogEntries(ctx, []types.LogEntry{
			{Index: types.Index(i), Term: 4, Command: []byte(fmt.Sprintf("s2-%d", i))},
		}))
	}
	testutil.AssertNoError(t, st1.TruncateLogSuffix(ctx, 13))
	testutil.AssertNoError(t, st2.TruncateLogPrefix(ctx, 5))
	testutil.AssertNoError(t, st3.ResetLog(ctx, types.LogEntry{Index: 50, Term: 9, Command: []byte("s3-reset")}))

	activeSeg := wal.activeSeg
	testutil.AssertTrue(t, activeSeg > 1, "expected multiple WAL segments, got %d", activeSeg)
	testutil.AssertNoError(t, wal.Close())

	// Simulate crash mid-write by appending partial corrupt bytes to the tail of the latest segment.
	segPath := filepath.Join(dir, "wal", fmt.Sprintf("wal-%010d.seg", activeSeg))
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o644)
	testutil.AssertNoError(t, err)
	_, err = f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02})
	testutil.AssertNoError(t, err)
	testutil.AssertNoError(t, f.Close())

	// Reopen and verify full recovery.
	wal2, err := NewSharedWAL(StorageConfig{Dir: dir}, opts, logger.NewNoOpLogger())
	testutil.AssertNoError(t, err)
	defer func() { _ = wal2.Close() }()

	r1 := wal2.ForGroup("shard-1")
	r2 := wal2.ForGroup("shard-2")
	r3 := wal2.ForGroup("shard-3")

	s1, err := r1.LoadState(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Term(2), s1.CurrentTerm)
	testutil.AssertEqual(t, types.NodeID("n1"), s1.VotedFor)
	testutil.AssertEqual(t, types.Index(12), r1.LastLogIndex())

	e1, err := r1.GetLogEntry(ctx, 12)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte("s1-12"), e1.Command)

	s2, err := r2.LoadState(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Term(4), s2.CurrentTerm)
	testutil.AssertEqual(t, types.Index(5), r2.FirstLogIndex())
	testutil.AssertEqual(t, types.Index(15), r2.LastLogIndex())

	testutil.AssertEqual(t, types.Index(50), r3.FirstLogIndex())
	e3, err := r3.GetLogEntry(ctx, 50)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte("s3-reset"), e3.Command)

	// Verify we can continue appending cleanly after recovery.
	testutil.AssertNoError(t, r1.AppendLogEntries(ctx, []types.LogEntry{
		{Index: 13, Term: 3, Command: []byte("s1-13-recovered")},
	}))
	e13, err := r1.GetLogEntry(ctx, 13)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte("s1-13-recovered"), e13.Command)
}

func TestSharedWAL_SnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewSharedWAL(StorageConfig{Dir: dir}, DefaultSharedWALOptions(), logger.NewNoOpLogger())
	testutil.AssertNoError(t, err)
	defer func() { _ = wal.Close() }()

	ctx := context.Background()
	stA := wal.ForGroup("alpha")
	stB := wal.ForGroup("beta")

	_, _, err = stA.LoadSnapshot(ctx)
	testutil.AssertErrorIs(t, err, ErrNoSnapshot)

	testutil.AssertNoError(t, stA.SaveSnapshot(ctx, types.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 2}, []byte("snap-alpha")))
	testutil.AssertNoError(t, stB.SaveSnapshot(ctx, types.SnapshotMetadata{LastIncludedIndex: 25, LastIncludedTerm: 4}, []byte("snap-beta")))

	metaA, dataA, err := stA.LoadSnapshot(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Index(10), metaA.LastIncludedIndex)
	testutil.AssertEqual(t, []byte("snap-alpha"), dataA)

	metaB, dataB, err := stB.LoadSnapshot(ctx)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, types.Index(25), metaB.LastIncludedIndex)
	testutil.AssertEqual(t, []byte("snap-beta"), dataB)

	// Verify per-group snapshot directories exist.
	_, err = os.Stat(filepath.Join(dir, "snapshots", "alpha", snapshotMetaFilename))
	testutil.AssertNoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "snapshots", "beta", snapshotDataFilename))
	testutil.AssertNoError(t, err)
}
