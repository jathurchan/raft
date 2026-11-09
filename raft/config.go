package raft

import (
	"time"

	"github.com/jathurchan/raft/types"
)

// PeerConfig holds the configuration for a single peer in the Raft cluster.
type PeerConfig struct {
	// ID is the unique identifier of the peer node.
	ID types.NodeID

	// Address is the network address (e.g., "host:port") for communication with this peer.
	Address string
}

// Config holds the complete configuration for initializing a Raft node.
// It aggregates node identity, peer info, Raft timing options, feature flags,
// tuning parameters, and external dependencies.
type Config struct {
	// ID is the unique identifier of this node.
	ID types.NodeID

	// Peers maps peer NodeIDs to their PeerConfig.
	Peers map[types.NodeID]PeerConfig

	// Options define Raft timing and behavior parameters.
	Options Options

	// FeatureFlags control optional Raft features and optimizations.
	FeatureFlags FeatureFlags

	// TuningParams configure advanced performance settings.
	TuningParams TuningParams
}

// Options define core Raft timing and behavior settings.
// Values are typically expressed in logical "ticks".
type Options struct {
	// ElectionTickCount is the number of ticks a follower waits before starting an election.
	ElectionTickCount int

	// HeartbeatTickCount is the number of ticks between leader heartbeats to followers.
	HeartbeatTickCount int

	// ElectionRandomizationFactor adds jitter to election timeouts to prevent split votes.
	// The timeout is randomized between ElectionTickCount and
	// ElectionTickCount * (1 + 2*ElectionRandomizationFactor), skewed toward lower values,
	// plus a small per-node offset. Values below 1.0 are replaced with the default.
	ElectionRandomizationFactor float64

	// MaxLogEntriesPerRequest limits the number of entries per AppendEntries RPC.
	MaxLogEntriesPerRequest int

	// SnapshotThreshold controls when to trigger snapshotting based on the log size.
	SnapshotThreshold int

	// Deprecated: StorageSyncDelay has no effect. Durability is controlled by the storage
	// layer, for example FileStorageOptions.SyncOnAppend.
	StorageSyncDelay int

	// LogCompactionMinEntries is the minimum number of entries needed beyond a snapshot index to allow compaction.
	LogCompactionMinEntries int

	// Deprecated: ApplyTickCount has no effect. Committed entries are applied as soon as
	// they commit.
	ApplyTickCount int

	// ApplyEntryTimeout sets a maximum duration for applying a single entry to the state machine.
	ApplyEntryTimeout time.Duration

	// FetchEntriesTimeout sets a timeout for fetching log entries from a leader or peer.
	FetchEntriesTimeout time.Duration
}

// FeatureFlags enables optional Raft features and optimizations.
// Flags are used exactly as set, so a flag left unset is off.
type FeatureFlags struct {
	// Deprecated: EnableReadIndex has no effect. ReadIndex is always available.
	EnableReadIndex bool

	// EnableLeaderLease enables leader leases for low-latency reads.
	EnableLeaderLease bool

	// EnablePreVote makes a node confirm it could win an election before raising its
	// term, so a node that lost contact cannot disrupt a healthy leader. It adds one
	// round trip to each election. Enable only when every node in the cluster supports
	// it, because older nodes treat a pre-vote as a real vote.
	EnablePreVote bool
}

// TuningParams configures advanced performance-related parameters.
type TuningParams struct {
	// MaxApplyBatchSize limits the number of entries applied in a single batch.
	MaxApplyBatchSize int
	// MaxSnapshotChunkSize sets the maximum snapshot chunk size (bytes) during InstallSnapshot RPCs.
	// 0 disables chunking, so the whole snapshot must fit in one gRPC message.
	// Enable only when every node in the cluster supports chunked transfer.
	MaxSnapshotChunkSize int
}

// Deprecated: WithExplicitFlags returns f unchanged. Flags are always used as set.
func (f FeatureFlags) WithExplicitFlags() FeatureFlags {
	return f
}
