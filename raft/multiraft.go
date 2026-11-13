package raft

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raft/logger"
)

// MultiRaftEngineOptions configures a MultiRaftEngine (TickManager).
type MultiRaftEngineOptions struct {
	// TickInterval is the interval between ticks across all registered Raft groups.
	// Defaults to NominalTickInterval (100ms) if <= 0.
	TickInterval time.Duration

	// WorkerCount is the size of the bounded worker pool used to tick groups in parallel.
	// Defaults to runtime.GOMAXPROCS(0) if <= 0.
	WorkerCount int

	// Logger is used for engine lifecycle and error logging.
	// Defaults to a no-op logger if nil.
	Logger logger.Logger
}

// DefaultMultiRaftEngineOptions returns the default options for MultiRaftEngine.
func DefaultMultiRaftEngineOptions() MultiRaftEngineOptions {
	return MultiRaftEngineOptions{
		TickInterval: NominalTickInterval,
		WorkerCount:  runtime.GOMAXPROCS(0),
		Logger:       logger.NewNoOpLogger(),
	}
}

// MultiRaftEngine drives multiple Raft groups from a single ticker goroutine,
// fanning out Tick() calls across a bounded worker pool sized to runtime.GOMAXPROCS(0).
type MultiRaftEngine struct {
	opts MultiRaftEngineOptions

	mu     sync.RWMutex
	groups map[string]Raft

	started atomic.Bool
	stopped atomic.Bool

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// TickManager is an alias for MultiRaftEngine.
type TickManager = MultiRaftEngine

// NewMultiRaftEngine creates a new MultiRaftEngine with the given options.
func NewMultiRaftEngine(opts MultiRaftEngineOptions) *MultiRaftEngine {
	if opts.TickInterval <= 0 {
		opts.TickInterval = NominalTickInterval
	}
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = runtime.GOMAXPROCS(0)
		if opts.WorkerCount <= 0 {
			opts.WorkerCount = 1
		}
	}
	if opts.Logger == nil {
		opts.Logger = logger.NewNoOpLogger()
	}

	return &MultiRaftEngine{
		opts:   opts,
		groups: make(map[string]Raft),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// NewTickManager creates a new TickManager (alias for NewMultiRaftEngine).
func NewTickManager(opts MultiRaftEngineOptions) *TickManager {
	return NewMultiRaftEngine(opts)
}

// RegisterGroup registers a Raft instance under the given groupID.
func (e *MultiRaftEngine) RegisterGroup(groupID string, node Raft) error {
	if groupID == "" {
		return errors.New("group ID cannot be empty")
	}
	if node == nil {
		return errors.New("raft node cannot be nil")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.groups[groupID]; exists {
		return fmt.Errorf("group %q is already registered", groupID)
	}
	e.groups[groupID] = node
	return nil
}

// UnregisterGroup removes a Raft instance from the engine.
func (e *MultiRaftEngine) UnregisterGroup(groupID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.groups, groupID)
}

// GetGroup returns the Raft instance registered for groupID, if any.
func (e *MultiRaftEngine) GetGroup(groupID string) (Raft, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	node, ok := e.groups[groupID]
	return node, ok
}

// GroupIDs returns a sorted slice of all currently registered group IDs.
func (e *MultiRaftEngine) GroupIDs() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ids := make([]string, 0, len(e.groups))
	for id := range e.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Start launches the single shared ticker goroutine and its bounded worker pool.
func (e *MultiRaftEngine) Start() error {
	if e.stopped.Load() {
		return ErrShuttingDown
	}
	if !e.started.CompareAndSwap(false, true) {
		return errors.New("multi-raft engine already started")
	}

	workerCount := e.opts.WorkerCount
	workCh := make(chan []Raft, workerCount)

	for i := 0; i < workerCount; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for batch := range workCh {
				for _, node := range batch {
					node.Tick(context.Background())
				}
			}
		}()
	}

	e.wg.Add(1)
	go e.runTicker(workCh, workerCount)

	e.opts.Logger.Infow("MultiRaftEngine started",
		"tick_interval", e.opts.TickInterval,
		"worker_count", workerCount,
	)
	return nil
}

func (e *MultiRaftEngine) runTicker(workCh chan<- []Raft, workerCount int) {
	defer func() {
		close(workCh)
		close(e.doneCh)
		e.wg.Done()
	}()

	ticker := time.NewTicker(e.opts.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			nodes := e.snapshotNodes()
			if len(nodes) == 0 {
				continue
			}

			// Partition nodes across up to workerCount shards.
			shards := min(workerCount, len(nodes))
			buckets := make([][]Raft, shards)
			for i, n := range nodes {
				b := i % shards
				buckets[b] = append(buckets[b], n)
			}

			for _, bucket := range buckets {
				select {
				case workCh <- bucket:
				case <-e.stopCh:
					return
				}
			}
		}
	}
}

// TickOnce synchronously ticks all registered groups once. Useful for deterministic testing.
func (e *MultiRaftEngine) TickOnce(ctx context.Context) {
	nodes := e.snapshotNodes()
	for _, n := range nodes {
		n.Tick(ctx)
	}
}

func (e *MultiRaftEngine) snapshotNodes() []Raft {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.groups) == 0 {
		return nil
	}
	nodes := make([]Raft, 0, len(e.groups))
	for _, n := range e.groups {
		nodes = append(nodes, n)
	}
	return nodes
}

// Stop gracefully shuts down the ticker goroutine and waits for workers to finish.
func (e *MultiRaftEngine) Stop(ctx context.Context) error {
	e.stopped.Store(true)
	if !e.started.Load() {
		return nil
	}

	e.stopOnce.Do(func() {
		close(e.stopCh)
	})

	waitCh := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
