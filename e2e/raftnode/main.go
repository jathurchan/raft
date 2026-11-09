//go:build e2e

// Command raftnode is the program under test in the kind end-to-end suite.
//
// "node" runs one Raft replica backed by real file storage and gRPC. "client"
// runs the load generator and safety checker, which sits outside the network
// partitions. "ctl" sends a request to the local client so the test can reach it
// through kubectl exec.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jathurchan/raft/logger"
	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/storage"
	"github.com/jathurchan/raft/types"
)

const (
	clusterSize = 5
	raftPort    = 9001
	nodePort    = 8080
	clientPort  = 8090
)

func dnsName(i int) string {
	return fmt.Sprintf("node-%d.raft-headless.default.svc.cluster.local", i)
}

type entry struct {
	Ver int
	Val []byte
}

// store is the replicated key-value state machine.
type store struct {
	mu       sync.RWMutex
	data     map[string]entry
	applied  types.Index
	cmdAt    map[types.Index]string
	restores atomic.Int64
}

func (s *store) Apply(_ context.Context, idx types.Index, cmd []byte) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx <= s.applied {
		return nil, nil
	}
	s.applied = idx
	s.cmdAt[idx] = string(cmd)

	parts := strings.Split(string(cmd), "|")
	ver, _ := strconv.Atoi(parts[1])
	size, _ := strconv.Atoi(parts[2])
	if ver > s.data[parts[0]].Ver {
		s.data[parts[0]] = entry{Ver: ver, Val: bytes.Repeat([]byte{byte(ver)}, size)}
	}
	return nil, nil
}

func (s *store) Snapshot(context.Context) (types.Index, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.data); err != nil {
		return 0, nil, err
	}
	return s.applied, buf.Bytes(), nil
}

func (s *store) RestoreSnapshot(
	_ context.Context,
	idx types.Index,
	_ types.Term,
	data []byte,
) error {
	restored := map[string]entry{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&restored); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.applied = restored, idx
	s.restores.Add(1)
	return nil
}

func (s *store) hash() (string, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%d:%d;", k, s.data[k].Ver, len(s.data[k].Val))
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16], len(keys)
}

func runNode() {
	id := types.NodeID(os.Getenv("POD_NAME"))
	peers := map[types.NodeID]raft.PeerConfig{}
	for i := range clusterSize {
		peer := types.NodeID(fmt.Sprintf("node-%d", i))
		peers[peer] = raft.PeerConfig{ID: peer, Address: fmt.Sprintf("%s:%d", dnsName(i), raftPort)}
	}

	lg := logger.NewStdLogger("error")
	st, err := storage.NewFileStorage(storage.StorageConfig{Dir: "/data"}, lg)
	if err != nil {
		log.Fatal(err)
	}

	kv := &store{data: map[string]entry{}, cmdAt: map[types.Index]string{}}
	cfg := raft.Config{
		ID:    id,
		Peers: peers,
		Options: raft.Options{
			ElectionTickCount:           50,
			HeartbeatTickCount:          5,
			ElectionRandomizationFactor: 2,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           1000,
			LogCompactionMinEntries:     200,
		},
		FeatureFlags: raft.FeatureFlags{
			EnableLeaderLease: os.Getenv("RAFT_LEASE") == "true",
			EnablePreVote:     os.Getenv("RAFT_PREVOTE") == "true",
		},
		TuningParams: raft.TuningParams{MaxApplyBatchSize: 64, MaxSnapshotChunkSize: 4 << 20},
	}

	node, err := raft.NewRaftBuilder().
		WithConfig(cfg).
		WithStorage(st).
		WithApplier(kv).
		WithLogger(lg).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	nm, err := raft.NewGRPCNetworkManager(
		id,
		fmt.Sprintf("0.0.0.0:%d", raftPort),
		peers,
		node,
		&atomic.Bool{},
		lg,
		raft.NewNoOpMetrics(),
		raft.NewStandardClock(),
		raft.DefaultGRPCNetworkManagerOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}
	node.SetNetworkManager(nm)
	if err := node.Start(); err != nil {
		log.Fatal(err)
	}

	go func() {
		for range time.Tick(raft.NominalTickInterval) {
			node.Tick(context.Background())
		}
	}()
	go func() {
		for range node.ApplyChannel() {
		}
	}()

	boot := strconv.FormatInt(time.Now().UnixNano(), 36)
	serveNode(node, kv, boot)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = node.Stop(ctx)
}

func serveNode(node raft.Raft, kv *store, boot string) {
	waitApplied := func(ctx context.Context, idx types.Index) bool {
		for node.Status().LastApplied < idx {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Millisecond):
			}
		}
		return true
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/livez", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if node.GetLeaderID() == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		s := node.Status()
		_ = json.NewEncoder(w).Encode(nodeStatus{
			OK: true, ID: string(s.ID), Role: s.Role.String(), Term: uint64(s.Term),
			Leader: string(s.LeaderID), Last: uint64(s.LastLogIndex),
			Commit: uint64(s.CommitIndex), Applied: uint64(s.LastApplied),
			Snap: uint64(s.SnapshotIndex), Restores: kv.restores.Load(), Boot: boot,
		})
	})
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		cmd := q.Get("key") + "|" + q.Get("ver") + "|" + q.Get("size")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		idx, _, _, err := node.Propose(ctx, []byte(cmd))
		if errors.Is(err, raft.ErrNotLeader) {
			w.WriteHeader(http.StatusMisdirectedRequest)
			return
		}
		if err != nil || !waitApplied(ctx, idx) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		kv.mu.RLock()
		got := kv.cmdAt[idx]
		kv.mu.RUnlock()
		if got != cmd {
			w.WriteHeader(http.StatusConflict)
		}
	})
	mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 400*time.Millisecond)
		defer cancel()

		idx, err := node.ReadIndex(ctx)
		if errors.Is(err, raft.ErrNotLeader) {
			w.WriteHeader(http.StatusMisdirectedRequest)
			return
		}
		if err != nil || !waitApplied(ctx, idx) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		kv.mu.RLock()
		e, ok := kv.data[r.URL.Query().Get("key")]
		kv.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, e.Ver)
	})
	mux.HandleFunc("/hash", func(w http.ResponseWriter, _ *http.Request) {
		h, keys := kv.hash()
		fmt.Fprintf(w, "%s/%d", h, keys)
	})
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		peer, _ := strconv.Atoi(r.URL.Query().Get("peer"))
		conn, err := net.DialTimeout(
			"tcp",
			fmt.Sprintf("%s:%d", dnsName(peer), raftPort),
			700*time.Millisecond,
		)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = conn.Close()
	})
	go func() { log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", nodePort), mux)) }()
}

type nodeStatus struct {
	OK       bool   `json:"ok"`
	ID       string `json:"id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Leader   string `json:"leader"`
	Last     uint64 `json:"last"`
	Commit   uint64 `json:"commit"`
	Applied  uint64 `json:"applied"`
	Snap     uint64 `json:"snap"`
	Restores int64  `json:"restores"`
	Boot     string `json:"boot"`
}

func nodeURL(i int, path string) string {
	return fmt.Sprintf("http://%s:%d%s", dnsName(i), nodePort, path)
}

// client generates load and checks the safety properties from outside the cluster.
type client struct {
	http *http.Client

	statusMu sync.RWMutex
	statuses [clusterSize]nodeStatus
	seenAt   [clusterSize]time.Time

	violationMu sync.Mutex
	leaderTerms map[uint64]string
	lastTerm    [clusterSize]uint64
	lastCommit  [clusterSize]map[string]uint64
	violations  []string

	keyMu sync.Mutex
	acked map[string]int
	next  map[string]int

	running                  atomic.Bool
	workers                  sync.WaitGroup
	writesOK, writesErr      atomic.Int64
	readsOK, readsErr, stale atomic.Int64
	lastWrite, lastRead      atomic.Int64
	maxWriteGap, maxReadGap  atomic.Int64
	staleSamples             []string
}

func (c *client) violation(format string, args ...any) {
	c.violations = append(c.violations, fmt.Sprintf(format, args...))
}

func (c *client) pollNode(i int) {
	poll := http.Client{Timeout: 400 * time.Millisecond}
	for {
		var s nodeStatus
		resp, err := poll.Get(nodeURL(i, "/status"))
		if err == nil {
			err = json.NewDecoder(resp.Body).Decode(&s)
			_ = resp.Body.Close()
		}
		if err == nil {
			c.observe(i, s)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *client) observe(i int, s nodeStatus) {
	c.statusMu.Lock()
	c.statuses[i], c.seenAt[i] = s, time.Now()
	c.statusMu.Unlock()

	c.violationMu.Lock()
	defer c.violationMu.Unlock()

	if s.Term < c.lastTerm[i] {
		c.violation("node-%d term moved backwards from %d to %d", i, c.lastTerm[i], s.Term)
	}
	c.lastTerm[i] = max(c.lastTerm[i], s.Term)

	if c.lastCommit[i] == nil {
		c.lastCommit[i] = map[string]uint64{}
	}
	if s.Commit < c.lastCommit[i][s.Boot] {
		c.violation("node-%d commit index moved backwards from %d to %d", i, c.lastCommit[i][s.Boot], s.Commit)
	}
	c.lastCommit[i][s.Boot] = max(c.lastCommit[i][s.Boot], s.Commit)

	if s.Role == "Leader" {
		if other, seen := c.leaderTerms[s.Term]; seen && other != s.ID {
			c.violation("two leaders in term %d: %s and %s", s.Term, other, s.ID)
		}
		c.leaderTerms[s.Term] = s.ID
	}
}

func (c *client) cluster() [clusterSize]nodeStatus {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()

	out := c.statuses
	for i := range out {
		out[i].OK = time.Since(c.seenAt[i]) < time.Second
	}
	return out
}

func (c *client) leader() int {
	best, bestTerm := -1, uint64(0)
	for i, s := range c.cluster() {
		if s.OK && s.Role == "Leader" && (best < 0 || s.Term > bestTerm) {
			best, bestTerm = i, s.Term
		}
	}
	return best
}

func recordGap(last, largest *atomic.Int64) {
	now := time.Now().UnixMilli()
	if prev := last.Swap(now); prev != 0 && now-prev > largest.Load() {
		largest.Store(now - prev)
	}
}

func (c *client) read(i int, key string) (int, int, error) {
	resp, err := c.http.Get(nodeURL(i, "/read?key="+url.QueryEscape(key)))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	ver, _ := strconv.Atoi(string(body))
	return resp.StatusCode, ver, nil
}

func (c *client) write(rng *rand.Rand, keys, size int) {
	l := c.leader()
	if l < 0 {
		c.writesErr.Add(1)
		return
	}

	key := fmt.Sprintf("k%d", rng.Intn(keys))
	c.keyMu.Lock()
	c.next[key]++
	ver := c.next[key]
	c.keyMu.Unlock()

	resp, err := c.http.Get(nodeURL(l, fmt.Sprintf("/put?key=%s&ver=%d&size=%d", key, ver, size)))
	if err != nil {
		c.writesErr.Add(1)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.writesErr.Add(1)
		return
	}

	c.writesOK.Add(1)
	recordGap(&c.lastWrite, &c.maxWriteGap)
	c.keyMu.Lock()
	if ver > c.acked[key] {
		c.acked[key] = ver
	}
	c.keyMu.Unlock()
}

func (c *client) readChecked(rng *rand.Rand, keys int) {
	l := c.leader()
	if l < 0 {
		c.readsErr.Add(1)
		return
	}

	key := fmt.Sprintf("k%d", rng.Intn(keys))
	c.keyMu.Lock()
	want := c.acked[key]
	c.keyMu.Unlock()

	code, ver, err := c.read(l, key)
	if err != nil || (code != http.StatusOK && code != http.StatusNotFound) {
		c.readsErr.Add(1)
		return
	}
	if (code == http.StatusNotFound && want > 0) || ver < want {
		c.stale.Add(1)
		c.keyMu.Lock()
		if len(c.staleSamples) < 10 {
			c.staleSamples = append(c.staleSamples, fmt.Sprintf("%s on node-%d: got %d, want at least %d", key, l, ver, want))
		}
		c.keyMu.Unlock()
	}
	c.readsOK.Add(1)
	recordGap(&c.lastRead, &c.maxReadGap)
}

func queryInt(r *http.Request, name string, fallback int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil {
		return v
	}
	return fallback
}

func (c *client) startLoad(writers, readers, keys, size int) {
	c.running.Store(true)
	for w := range writers {
		c.workers.Add(1)
		go func(seed int64) {
			defer c.workers.Done()
			rng := rand.New(rand.NewSource(seed))
			for c.running.Load() {
				time.Sleep(25 * time.Millisecond)
				c.write(rng, keys, size)
			}
		}(int64(w + 1))
	}
	for r := range readers {
		c.workers.Add(1)
		go func(seed int64) {
			defer c.workers.Done()
			rng := rand.New(rand.NewSource(seed))
			for c.running.Load() {
				time.Sleep(10 * time.Millisecond)
				c.readChecked(rng, keys)
			}
		}(int64(r + 1000))
	}
}

type loadStats struct {
	Running       bool     `json:"running"`
	WritesOK      int64    `json:"writesOK"`
	WritesErr     int64    `json:"writesErr"`
	ReadsOK       int64    `json:"readsOK"`
	ReadsErr      int64    `json:"readsErr"`
	Stale         int64    `json:"stale"`
	StaleSamples  []string `json:"staleSamples"`
	MaxWriteGapMs int64    `json:"maxWriteGapMs"`
	MaxReadGapMs  int64    `json:"maxReadGapMs"`
	Keys          int      `json:"keys"`
}

func (c *client) stats() loadStats {
	c.keyMu.Lock()
	defer c.keyMu.Unlock()
	return loadStats{
		Running: c.running.Load(), WritesOK: c.writesOK.Load(), WritesErr: c.writesErr.Load(),
		ReadsOK: c.readsOK.Load(), ReadsErr: c.readsErr.Load(), Stale: c.stale.Load(),
		StaleSamples:  append([]string(nil), c.staleSamples...),
		MaxWriteGapMs: c.maxWriteGap.Load(), MaxReadGapMs: c.maxReadGap.Load(), Keys: len(c.acked),
	}
}

type verification struct {
	Lost       []string `json:"lost"`
	LostCount  int      `json:"lostCount"`
	Present    []string `json:"present"`
	Violations []string `json:"violations"`
	Terms      int      `json:"terms"`
	Converged  bool     `json:"converged"`
	Hashes     []string `json:"hashes"`
	Error      string   `json:"error"`
}

// verify stops the load and checks durability, convergence, and election safety.
func (c *client) verify(absent []string) verification {
	c.running.Store(false)
	c.workers.Wait()

	var out verification

	lead, lastCode := -1, 0
	var lastErr error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if lead = c.leader(); lead >= 0 {
			code, _, err := c.read(lead, "__probe__")
			if err == nil && (code == http.StatusOK || code == http.StatusNotFound) {
				break
			}
			lastCode, lastErr = code, err
			lead = -1
		}
	}
	if lead < 0 {
		out.Error = fmt.Sprintf("no leader could serve reads within 90s (last code=%d err=%v, cluster=%+v)", lastCode, lastErr, c.cluster())
		return out
	}

	c.keyMu.Lock()
	acked := make(map[string]int, len(c.acked))
	for k, v := range c.acked {
		acked[k] = v
	}
	c.keyMu.Unlock()

	readRetry := func(key string) (int, int, error) {
		var code, ver int
		var err error
		for range 50 {
			code, ver, err = c.read(c.leaderOr(lead), key)
			if err == nil && (code == http.StatusOK || code == http.StatusNotFound) {
				return code, ver, nil
			}
			time.Sleep(100 * time.Millisecond)
		}
		return code, ver, err
	}

	for key, want := range acked {
		code, ver, err := readRetry(key)
		if err != nil || code != http.StatusOK || ver < want {
			out.LostCount++
			if len(out.Lost) < 10 {
				out.Lost = append(out.Lost, fmt.Sprintf("%s: code=%d got=%d want>=%d err=%v", key, code, ver, want, err))
			}
		}
	}
	for _, key := range absent {
		if code, _, err := readRetry(key); err != nil || code != http.StatusNotFound {
			out.Present = append(out.Present, fmt.Sprintf("%s: code=%d err=%v", key, code, err))
		}
	}

	for deadline := time.Now().Add(120 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		out.Hashes = out.Hashes[:0]
		for i := range clusterSize {
			out.Hashes = append(out.Hashes, c.fetchHash(i))
		}
		same := !strings.Contains(strings.Join(out.Hashes, ","), "unreachable")
		for _, h := range out.Hashes {
			same = same && h == out.Hashes[0]
		}
		if same {
			out.Converged = true
			break
		}
	}

	c.violationMu.Lock()
	defer c.violationMu.Unlock()
	out.Violations = append([]string(nil), c.violations...)
	out.Terms = len(c.leaderTerms)
	return out
}

func (c *client) leaderOr(fallback int) int {
	if l := c.leader(); l >= 0 {
		return l
	}
	return fallback
}

func (c *client) fetchHash(i int) string {
	resp, err := c.http.Get(nodeURL(i, "/hash"))
	if err != nil {
		return "unreachable"
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func runClient() {
	c := &client{
		http:        &http.Client{Timeout: 3 * time.Second},
		leaderTerms: map[uint64]string{},
		acked:       map[string]int{},
		next:        map[string]int{},
	}
	for i := range clusterSize {
		go c.pollNode(i)
	}

	reply := func(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }

	mux := http.NewServeMux()
	mux.HandleFunc("/cluster", func(w http.ResponseWriter, _ *http.Request) { reply(w, c.cluster()) })
	mux.HandleFunc("/load/start", func(w http.ResponseWriter, r *http.Request) {
		c.startLoad(queryInt(r, "writers", 4), queryInt(r, "readers", 4), queryInt(r, "keys", 1000), queryInt(r, "size", 1000))
	})
	mux.HandleFunc("/load/stats", func(w http.ResponseWriter, _ *http.Request) { reply(w, c.stats()) })
	mux.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		var absent []string
		if a := r.URL.Query().Get("absent"); a != "" {
			absent = strings.Split(a, ",")
		}
		reply(w, c.verify(absent))
	})
	mux.HandleFunc("/proxy", func(w http.ResponseWriter, r *http.Request) {
		result := map[string]any{"code": 0, "body": ""}
		resp, err := c.http.Get(nodeURL(queryInt(r, "node", 0), r.URL.Query().Get("path")))
		if err != nil {
			result["body"] = err.Error()
			reply(w, result)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		result["code"], result["body"] = resp.StatusCode, string(body)
		reply(w, result)
	})
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", clientPort), mux))
}

func runCtl(path string) {
	c := http.Client{Timeout: 5 * time.Minute}
	resp, err := c.Get(fmt.Sprintf("http://127.0.0.1:%d%s", clientPort, path))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: raftnode node|client|ctl <path>")
	}
	switch os.Args[1] {
	case "node":
		runNode()
	case "client":
		runClient()
	case "ctl":
		runCtl(os.Args[2])
	default:
		log.Fatalf("unknown mode %q", os.Args[1])
	}
}
