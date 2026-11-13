package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raft/logger"
	"github.com/jathurchan/raft/raft/proto"
	"github.com/jathurchan/raft/storage"
	"github.com/jathurchan/raft/testutil"
	"github.com/jathurchan/raft/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func getFreePortAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.AssertNoError(t, err)
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type recordingRPCHandler struct {
	mu           sync.Mutex
	voteCalls    int
	appendCalls  int
	snapCalls    int
	timeoutCalls int
	lastLeader   types.NodeID
}

func (r *recordingRPCHandler) RequestVote(_ context.Context, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	r.mu.Lock()
	r.voteCalls++
	r.mu.Unlock()
	return &types.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (r *recordingRPCHandler) AppendEntries(_ context.Context, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	r.mu.Lock()
	r.appendCalls++
	r.lastLeader = args.LeaderID
	r.mu.Unlock()
	return &types.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (r *recordingRPCHandler) InstallSnapshot(_ context.Context, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	r.mu.Lock()
	r.snapCalls++
	r.mu.Unlock()
	return &types.InstallSnapshotReply{Term: args.Term}, nil
}

func (r *recordingRPCHandler) TimeoutNow(_ context.Context, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	r.mu.Lock()
	r.timeoutCalls++
	r.mu.Unlock()
	return &TimeoutNowReply{Term: args.Term, Success: true}, nil
}

func TestMultiGroupNetworkManager_RoutingAndUnknownGroupErrors(t *testing.T) {
	addr1 := getFreePortAddr(t)
	addr2 := getFreePortAddr(t)
	peers := map[types.NodeID]PeerConfig{
		"node-1": {ID: "node-1", Address: addr1},
		"node-2": {ID: "node-2", Address: addr2},
	}

	var shut1, shut2 atomic.Bool
	nm1, err := NewMultiGroupGRPCNetworkManager("node-1", addr1, peers, &shut1, logger.NewNoOpLogger(), NewNoOpMetrics(), NewStandardClock(), DefaultGRPCNetworkManagerOptions())
	testutil.AssertNoError(t, err)
	nm2, err := NewMultiGroupGRPCNetworkManager("node-2", addr2, peers, &shut2, logger.NewNoOpLogger(), NewNoOpMetrics(), NewStandardClock(), DefaultGRPCNetworkManagerOptions())
	testutil.AssertNoError(t, err)

	hA := &recordingRPCHandler{}
	hB := &recordingRPCHandler{}
	testutil.AssertNoError(t, nm2.RegisterGroup("group-a", hA))
	testutil.AssertNoError(t, nm2.RegisterGroup("group-b", hB))
	testutil.AssertError(t, nm2.RegisterGroup("group-a", hA)) // duplicate

	testutil.AssertNoError(t, nm1.Start())
	defer func() { _ = nm1.Stop() }()
	testutil.AssertNoError(t, nm2.Start())
	defer func() { _ = nm2.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Route via ForGroup adapters on nm1 -> nm2
	gA := nm1.ForGroup("group-a")
	gB := nm1.ForGroup("group-b")

	// Start/Stop on per-group adapter must be no-op
	testutil.AssertNoError(t, gA.Start())
	testutil.AssertNoError(t, gA.Stop())

	_, err = gA.SendAppendEntries(ctx, "node-2", &types.AppendEntriesArgs{Term: 1, LeaderID: "node-1"})
	testutil.AssertNoError(t, err)
	_, err = gA.SendRequestVote(ctx, "node-2", &types.RequestVoteArgs{Term: 2, CandidateID: "node-1"})
	testutil.AssertNoError(t, err)
	_, err = gB.SendInstallSnapshot(ctx, "node-2", &types.InstallSnapshotArgs{Term: 3, LeaderID: "node-1", Data: []byte("snap")})
	testutil.AssertNoError(t, err)

	hA.mu.Lock()
	testutil.AssertEqual(t, 1, hA.appendCalls)
	testutil.AssertEqual(t, 1, hA.voteCalls)
	testutil.AssertEqual(t, 0, hA.snapCalls)
	hA.mu.Unlock()

	hB.mu.Lock()
	testutil.AssertEqual(t, 0, hB.appendCalls)
	testutil.AssertEqual(t, 0, hB.voteCalls)
	testutil.AssertEqual(t, 1, hB.snapCalls)
	hB.mu.Unlock()

	// Unknown group and empty group_id must return explicit gRPC status errors
	conn, err := grpc.NewClient(addr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.AssertNoError(t, err)
	defer func() { _ = conn.Close() }()
	client := proto.NewRaftClient(conn)

	_, err = client.AppendEntries(ctx, &proto.AppendEntriesRequest{GroupId: "unknown-group", Term: 1, LeaderId: "node-1"})
	testutil.AssertError(t, err)
	testutil.AssertEqual(t, codes.NotFound, status.Code(err))

	_, err = client.RequestVote(ctx, &proto.RequestVoteRequest{GroupId: "", Term: 1, CandidateId: "node-1"})
	testutil.AssertError(t, err)
	testutil.AssertEqual(t, codes.InvalidArgument, status.Code(err))

	// Unregister group-b and verify NotFound
	nm2.UnregisterGroup("group-b")
	_, err = client.InstallSnapshot(ctx, &proto.InstallSnapshotRequest{GroupId: "group-b", Term: 3, LeaderId: "node-1"})
	testutil.AssertError(t, err)
	testutil.AssertEqual(t, codes.NotFound, status.Code(err))
}

type noopApplier struct{}

func (a *noopApplier) Apply(_ context.Context, _ types.Index, _ []byte) (any, error) {
	return nil, nil
}

func (a *noopApplier) Snapshot(_ context.Context) (types.Index, []byte, error) {
	return 0, nil, nil
}

func (a *noopApplier) RestoreSnapshot(_ context.Context, _ types.Index, _ types.Term, _ []byte) error {
	return nil
}

type multiRaftTestNode struct {
	id     types.NodeID
	addr   string
	wal    *storage.SharedWAL
	nm     *MultiGroupNetworkManager
	engine *MultiRaftEngine
	groups map[string]Raft
}

func (n *multiRaftTestNode) stop(ctx context.Context) {
	_ = n.engine.Stop(ctx)
	for _, r := range n.groups {
		_ = r.Stop(ctx)
	}
	_ = n.nm.Stop()
	_ = n.wal.Close()
}

func TestMultiRaft_ThreeNodeThreeGroupClusterAndFailover(t *testing.T) {
	nodeIDs := []types.NodeID{"node-1", "node-2", "node-3"}
	groupIDs := []string{"group-1", "group-2", "group-3"}

	peers := make(map[types.NodeID]PeerConfig, len(nodeIDs))
	for _, id := range nodeIDs {
		peers[id] = PeerConfig{ID: id, Address: getFreePortAddr(t)}
	}

	// Deterministic initial leadership placement:
	// group-1 -> node-1 (delay 1), group-2 -> node-2 (delay 1), group-3 -> node-3 (delay 1).
	preferredOwner := map[string]types.NodeID{
		"group-1": "node-1",
		"group-2": "node-2",
		"group-3": "node-3",
	}

	nodes := make(map[types.NodeID]*multiRaftTestNode, len(nodeIDs))
	for _, id := range nodeIDs {
		wal, err := storage.NewSharedWAL(
			storage.StorageConfig{Dir: t.TempDir()},
			storage.DefaultSharedWALOptions(),
			logger.NewNoOpLogger(),
		)
		testutil.AssertNoError(t, err)

		var shut atomic.Bool
		nm, err := NewMultiGroupGRPCNetworkManager(
			id,
			peers[id].Address,
			peers,
			&shut,
			logger.NewNoOpLogger(),
			NewNoOpMetrics(),
			NewStandardClock(),
			DefaultGRPCNetworkManagerOptions(),
		)
		testutil.AssertNoError(t, err)

		engine := NewMultiRaftEngine(MultiRaftEngineOptions{
			TickInterval: 25 * time.Millisecond, // Fast ticks for test speed
			Logger:       logger.NewNoOpLogger(),
		})

		tn := &multiRaftTestNode{
			id:     id,
			addr:   peers[id].Address,
			wal:    wal,
			nm:     nm,
			engine: engine,
			groups: make(map[string]Raft, len(groupIDs)),
		}

		for _, gid := range groupIDs {
			delay := 25
			if preferredOwner[gid] == id {
				delay = 1
			}
			cfg := Config{
				ID:    id,
				Peers: peers,
				Options: Options{
					ElectionTickCount:           8,
					HeartbeatTickCount:          2,
					ElectionRandomizationFactor: 1.5,
					MaxLogEntriesPerRequest:     64,
					SnapshotThreshold:           10000,
					LogCompactionMinEntries:     100,
					ApplyEntryTimeout:           time.Second,
					FetchEntriesTimeout:         time.Second,
					InitialElectionDelayTicks:   delay,
				},
			}
			r, err := NewRaftBuilder().
				WithConfig(cfg).
				WithStorage(wal.ForGroup(gid)).
				WithApplier(&noopApplier{}).
				WithLogger(logger.NewNoOpLogger()).
				Build()
			testutil.AssertNoError(t, err)

			testutil.AssertNoError(t, nm.RegisterGroup(gid, r))
			r.SetNetworkManager(nm.ForGroup(gid))
			testutil.AssertNoError(t, engine.RegisterGroup(gid, r))
			tn.groups[gid] = r
		}

		nodes[id] = tn
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Start network managers, Raft groups, and shared ticker engines.
	for _, id := range nodeIDs {
		testutil.AssertNoError(t, nodes[id].nm.Start())
	}
	for _, id := range nodeIDs {
		for _, gid := range groupIDs {
			testutil.AssertNoError(t, nodes[id].groups[gid].Start())
		}
		testutil.AssertNoError(t, nodes[id].engine.Start())
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		for _, n := range nodes {
			if n != nil {
				n.stop(stopCtx)
			}
		}
	}()

	// 1. Verify each group elects its deterministic initial leader (1 leader per node).
	deadline := time.Now().Add(6 * time.Second)
	for _, gid := range groupIDs {
		expectedLeader := preferredOwner[gid]
		for time.Now().Before(deadline) {
			_, isLeader := nodes[expectedLeader].groups[gid].GetState()
			if isLeader {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		_, isLeader := nodes[expectedLeader].groups[gid].GetState()
		if !isLeader {
			for _, nid := range nodeIDs {
				st := nodes[nid].groups[gid].Status()
				t.Logf("DEBUG %s on %s: role=%v term=%d leader=%s", gid, nid, st.Role, st.Term, st.LeaderID)
			}
		}
		testutil.AssertTrue(t, isLeader, "expected %s to be initial leader for %s", expectedLeader, gid)
	}

	// 2. Propose commands on all 3 groups and verify commit index advances.
	for _, gid := range groupIDs {
		leaderID := preferredOwner[gid]
		idx, _, isLeader, err := nodes[leaderID].groups[gid].Propose(ctx, []byte(fmt.Sprintf("cmd-%s", gid)))
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, isLeader, "expected leader")

		waitDeadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(waitDeadline) {
			if nodes[leaderID].groups[gid].GetCommitIndex() >= idx {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		testutil.AssertTrue(t, nodes[leaderID].groups[gid].GetCommitIndex() >= idx, "expected commit on %s", gid)
	}

	// Record terms of group-1 and group-2 before crashing node-3.
	termG1Before, _ := nodes["node-1"].groups["group-1"].GetState()
	termG2Before, _ := nodes["node-2"].groups["group-2"].GetState()

	// 3. Simulate single-node crash: stop node-3 (which leads group-3).
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	nodes["node-3"].stop(stopCtx)
	stopCancel()
	nodes["node-3"] = nil

	// Wait for group-3 to elect a new leader among node-1 and node-2.
	var newG3Leader types.NodeID
	failoverDeadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(failoverDeadline) {
		for _, candidate := range []types.NodeID{"node-1", "node-2"} {
			if _, ok := nodes[candidate].groups["group-3"].GetState(); ok {
				newG3Leader = candidate
				break
			}
		}
		if newG3Leader != "" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	testutil.AssertTrue(t, newG3Leader == "node-1" || newG3Leader == "node-2",
		"expected group-3 to elect a new leader after node-3 crash, got %q", newG3Leader)

	// Verify group-1 and group-2 remained completely unaffected (same leaders, same terms, still writable).
	termG1After, isL1 := nodes["node-1"].groups["group-1"].GetState()
	termG2After, isL2 := nodes["node-2"].groups["group-2"].GetState()
	testutil.AssertTrue(t, isL1, "node-1 should remain leader of group-1")
	testutil.AssertTrue(t, isL2, "node-2 should remain leader of group-2")
	testutil.AssertEqual(t, termG1Before, termG1After)
	testutil.AssertEqual(t, termG2Before, termG2After)

	// Verify group-1 can still commit proposals after node-3 crash.
	idx1, _, _, err := nodes["node-1"].groups["group-1"].Propose(ctx, []byte("after-crash-g1"))
	testutil.AssertNoError(t, err)
	commitDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(commitDeadline) {
		if nodes["node-1"].groups["group-1"].GetCommitIndex() >= idx1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	testutil.AssertTrue(t, nodes["node-1"].groups["group-1"].GetCommitIndex() >= idx1, "expected group-1 to commit after node-3 crash")

	// 4. Verify TransferLeadership on group-1 from node-1 to node-2.
	transferCtx, transferCancel := context.WithTimeout(ctx, 5*time.Second)
	defer transferCancel()
	err = nodes["node-1"].groups["group-1"].TransferLeadership(transferCtx, "node-2")
	testutil.AssertNoError(t, err)

	xferDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(xferDeadline) {
		if _, isL2Now := nodes["node-2"].groups["group-1"].GetState(); isL2Now {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, isL2Now := nodes["node-2"].groups["group-1"].GetState()
	testutil.AssertTrue(t, isL2Now, "expected node-2 to become leader of group-1 after TransferLeadership")
}
