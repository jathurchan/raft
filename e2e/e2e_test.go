//go:build e2e

// Package e2e runs a real 5-node Raft cluster on kind and injects failures:
// pod deletion, process crashes, machine freezes, and network partitions.
// Run it with ./e2e/run.sh. Results are written to e2e/RESULTS.md.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	clusterName  = "raft-e2e"
	imageName    = "raft-e2e:latest"
	nodes        = 5
	quorum       = 3
	leaderWait   = 60 * time.Second
	recoverWait  = 120 * time.Second
	partitionTag = "RAFTE2E"
)

var (
	kubeconfig string
	workers    []string
	report     = &resultsReport{}
)

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
}

type loadStats struct {
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

// profile selects the optional Raft features for a scenario.
type profile struct {
	prevote, lease bool
}

func (p profile) String() string {
	switch {
	case p.prevote && p.lease:
		return "PreVote + lease"
	case p.prevote:
		return "PreVote"
	case p.lease:
		return "lease"
	}
	return "default"
}

// harness gives one scenario access to the cluster and collects its measurements.
type harness struct {
	t        *testing.T
	metrics  []string
	absent   []string
	failures []string
	verified bool
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if err != nil {
		out.WriteString(errOut.String())
	}
	return strings.TrimSpace(out.String()), err
}

func mustRun(timeout time.Duration, name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := run(ctx, name, args...)
	if err != nil {
		panic(fmt.Sprintf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out))
	}
	return out
}

func kubectl(args ...string) string { return mustRun(4*time.Minute, "kubectl", args...) }

func TestMain(m *testing.M) {
	wd, _ := os.Getwd()
	binDir := filepath.Join(wd, ".bin")
	_ = os.MkdirAll(binDir, 0o755)
	kubeconfig = filepath.Join(binDir, "kubeconfig")

	keep := os.Getenv("E2E_KEEP_CLUSTER") == "1"
	existing, _ := run(context.Background(), "kind", "get", "clusters")
	if !slices.Contains(strings.Fields(existing), clusterName) {
		mustRun(8*time.Minute, "kind", "create", "cluster", "--name", clusterName,
			"--kubeconfig", kubeconfig, "--config", "kind.yaml", "--wait", "240s")
	} else {
		mustRun(30*time.Second, "kind", "export", "kubeconfig", "--name", clusterName, "--kubeconfig", kubeconfig)
	}

	buildImage(binDir)
	for _, node := range strings.Fields(kubectl("get", "nodes", "-o", "name")) {
		name := strings.TrimPrefix(node, "node/")
		if !strings.Contains(name, "control-plane") {
			workers = append(workers, name)
		}
	}
	installPartitionChains()

	report.start()
	code := m.Run()
	report.write()

	if !keep {
		mustRun(3*time.Minute, "kind", "delete", "cluster", "--name", clusterName)
	}
	os.Exit(code)
}

func buildImage(binDir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	context := filepath.Join(binDir, "image")
	_ = os.MkdirAll(context, 0o755)

	build := exec.CommandContext(ctx, "go", "build", "-tags", "e2e",
		"-o", filepath.Join(context, "raftnode"), "./raftnode")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if out, err := build.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("go build: %v\n%s", err, out))
	}

	dockerfile, _ := os.ReadFile("Dockerfile")
	_ = os.WriteFile(filepath.Join(context, "Dockerfile"), dockerfile, 0o644)
	mustRun(3*time.Minute, "docker", "build", "-q", "-t", imageName, context)
	mustRun(4*time.Minute, "kind", "load", "docker-image", imageName, "--name", clusterName)
}

// installPartitionChains adds an empty packet filter chain on every worker. Rules
// live in the raw table so they also apply to established connections.
func installPartitionChains() {
	for _, w := range workers {
		mustRun(30*time.Second, "docker", "exec", w, "sh", "-c", fmt.Sprintf(
			"iptables -t raw -N %[1]s 2>/dev/null; iptables -t raw -F %[1]s; "+
				"iptables -t raw -C PREROUTING -j %[1]s 2>/dev/null || iptables -t raw -I PREROUTING 1 -j %[1]s",
			partitionTag))
	}
}

func healPartitions() {
	for _, w := range workers {
		mustRun(2*time.Minute, "docker", "exec", w, "iptables", "-t", "raw", "-F", partitionTag)
	}
}

func unpauseWorkers() {
	for _, w := range workers {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_, _ = run(ctx, "docker", "unpause", w)
		cancel()
	}
}

func (h *harness) m(name string, value any) {
	if d, ok := value.(time.Duration); ok {
		value = d.Round(100 * time.Millisecond)
	}
	h.metrics = append(h.metrics, fmt.Sprintf("%s: %v", name, value))
}

func (h *harness) failf(format string, args ...any) {
	h.t.Helper()
	msg := fmt.Sprintf(format, args...)
	h.failures = append(h.failures, msg)
	h.t.Error(msg)
}

func (h *harness) fatalf(format string, args ...any) {
	h.t.Helper()
	msg := fmt.Sprintf(format, args...)
	h.failures = append(h.failures, msg)
	h.t.Fatal(msg)
}

func (h *harness) ctl(path string, out any) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// The Kubernetes API can briefly stall on a busy machine; that says nothing about Raft.
	var body string
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		body, err = run(ctx, "kubectl", "exec", "e2e-client", "--", "/raftnode", "ctl", path)
		if err == nil || !isAPIUnavailable(body) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		h.fatalf("client request %s failed: %v: %s", path, err, body)
	}
	if out != nil {
		if err := json.Unmarshal([]byte(body), out); err != nil {
			h.fatalf("client request %s returned invalid JSON: %v: %s", path, err, body)
		}
	}
}

func isAPIUnavailable(output string) bool {
	return strings.Contains(output, "Unable to connect to the server") ||
		strings.Contains(output, "TLS handshake timeout") ||
		strings.Contains(output, "i/o timeout") ||
		strings.Contains(output, "connection refused")
}

func (h *harness) deploy(p profile) {
	h.t.Helper()
	unpauseWorkers()
	healPartitions()

	_, _ = run(context.Background(), "kubectl", "delete", "pod", "e2e-client", "--force", "--grace-period=0", "--ignore-not-found")
	_, _ = run(context.Background(), "kubectl", "delete", "statefulset", "node", "--ignore-not-found", "--wait=false")
	_, _ = run(context.Background(), "kubectl", "delete", "pod", "-l", "app=raft", "--force", "--grace-period=0", "--ignore-not-found")
	kubectl("delete", "pvc", "--all", "--wait=true", "--timeout=120s")

	manifest, err := os.ReadFile("deploy.yaml")
	if err != nil {
		h.fatalf("read deploy.yaml: %v", err)
	}
	rendered := strings.NewReplacer(
		"{{PREVOTE}}", strconv.FormatBool(p.prevote),
		"{{LEASE}}", strconv.FormatBool(p.lease),
	).Replace(string(manifest))

	path := filepath.Join(h.t.TempDir(), "deploy.yaml")
	if err := os.WriteFile(path, []byte(rendered), 0o644); err != nil {
		h.fatalf("write manifest: %v", err)
	}
	kubectl("apply", "-f", path)
	kubectl("rollout", "status", "statefulset/node", "--timeout=240s")
	kubectl("wait", "--for=condition=Ready", "pod/e2e-client", "--timeout=120s")

	h.t.Cleanup(func() {
		unpauseWorkers()
		healPartitions()
	})
	h.m("profile", p.String())
}

func (h *harness) cluster() []nodeStatus {
	h.t.Helper()
	var out []nodeStatus
	h.ctl("/cluster", &out)
	return out
}

// currentLeader returns the reachable leader with the highest term, or -1.
func currentLeader(statuses []nodeStatus) (int, uint64) {
	best, term := -1, uint64(0)
	for i, s := range statuses {
		if s.OK && s.Role == "Leader" && (best < 0 || s.Term > term) {
			best, term = i, s.Term
		}
	}
	return best, term
}

func (h *harness) waitFor(what string, limit time.Duration, check func([]nodeStatus) bool) time.Duration {
	h.t.Helper()
	start := time.Now()
	for time.Since(start) < limit {
		if check(h.cluster()) {
			return time.Since(start)
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.fatalf("timed out after %v waiting for %s: %+v", limit, what, h.cluster())
	return 0
}

func (h *harness) waitLeader() (int, uint64) {
	h.t.Helper()
	var leader int
	var term uint64
	h.waitFor("a leader", leaderWait, func(s []nodeStatus) bool {
		leader, term = currentLeader(s)
		return leader >= 0
	})
	return leader, term
}

// waitNewLeader waits for a leader in a higher term among the allowed nodes.
func (h *harness) waitNewLeader(afterTerm uint64, allowed []int) (int, uint64, time.Duration) {
	h.t.Helper()
	leader, term := -1, uint64(0)
	took := h.waitFor("a new leader", leaderWait, func(statuses []nodeStatus) bool {
		for _, i := range allowed {
			s := statuses[i]
			if s.OK && s.Role == "Leader" && s.Term > afterTerm {
				leader, term = i, s.Term
				return true
			}
		}
		return false
	})
	return leader, term, took
}

func (h *harness) waitCaughtUp(node int) time.Duration {
	h.t.Helper()
	return h.waitFor(fmt.Sprintf("node-%d to catch up", node), recoverWait, func(statuses []nodeStatus) bool {
		leader, _ := currentLeader(statuses)
		if leader < 0 || !statuses[node].OK {
			return false
		}
		if leader == node {
			return true
		}
		s, l := statuses[node], statuses[leader]
		return s.Role == "Follower" && s.Leader == l.ID && l.Applied <= s.Applied+500
	})
}

func (h *harness) startLoad(query string) {
	h.t.Helper()
	h.ctl("/load/start?"+query, nil)
}

func (h *harness) stats() loadStats {
	h.t.Helper()
	var out loadStats
	h.ctl("/load/stats", &out)
	return out
}

// waitServing waits until both reads and writes have succeeded again.
func (h *harness) waitServing() time.Duration {
	h.t.Helper()
	start := time.Now()
	before := h.stats()
	for time.Since(start) < recoverWait {
		now := h.stats()
		if now.WritesOK > before.WritesOK && now.ReadsOK > before.ReadsOK {
			return time.Since(start)
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.fatalf("reads and writes did not resume within %v: %+v", recoverWait, h.stats())
	return 0
}

// verify stops the load and applies the safety checks every scenario must pass.
func (h *harness) verify() {
	h.t.Helper()
	h.verified = true

	query := "/verify"
	if len(h.absent) > 0 {
		query += "?absent=" + url.QueryEscape(strings.Join(h.absent, ","))
	}

	var v verification
	h.ctl(query, &v)
	s := h.stats()

	h.m("writes ok / failed", fmt.Sprintf("%d / %d", s.WritesOK, s.WritesErr))
	h.m("reads ok / failed", fmt.Sprintf("%d / %d", s.ReadsOK, s.ReadsErr))
	h.m("longest read gap", time.Duration(s.MaxReadGapMs)*time.Millisecond)
	h.m("longest write gap", time.Duration(s.MaxWriteGapMs)*time.Millisecond)
	h.m("leader terms seen", v.Terms)
	h.m("lost writes", v.LostCount)
	h.m("stale reads", s.Stale)

	if v.Error != "" {
		h.failf("verification could not finish: %s", v.Error)
	}
	if v.LostCount > 0 {
		h.failf("%d acknowledged writes were lost or unreadable: %v", v.LostCount, v.Lost)
	}
	if s.Stale > 0 {
		h.failf("%d stale reads: %v", s.Stale, s.StaleSamples)
	}
	if len(v.Present) > 0 {
		h.failf("writes that were never acknowledged survived: %v", v.Present)
	}
	if len(v.Violations) > 0 {
		h.failf("Raft safety violations: %v", v.Violations)
	}
	if !v.Converged {
		h.failf("replicas did not converge: %v", v.Hashes)
	}
}

type podInfo struct {
	name, ip, host string
}

func (h *harness) pods() []podInfo {
	h.t.Helper()
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				NodeName string `json:"nodeName"`
			} `json:"spec"`
			Status struct {
				PodIP string `json:"podIP"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(kubectl("get", "pods", "-l", "app=raft", "-o", "json")), &list); err != nil {
		h.fatalf("parse pods: %v", err)
	}

	out := make([]podInfo, nodes)
	for _, item := range list.Items {
		i, err := strconv.Atoi(strings.TrimPrefix(item.Metadata.Name, "node-"))
		if err == nil && i < nodes {
			out[i] = podInfo{item.Metadata.Name, item.Status.PodIP, item.Spec.NodeName}
		}
	}
	return out
}

// dropTraffic drops packets from each source node to each destination node.
func (h *harness) dropTraffic(from, to []int) {
	h.t.Helper()
	pods := h.pods()

	var rules strings.Builder
	for _, a := range from {
		for _, b := range to {
			fmt.Fprintf(&rules, "iptables -t raw -A %s -s %s -d %s -j DROP; ", partitionTag, pods[a].ip, pods[b].ip)
		}
	}
	for _, w := range workers {
		mustRun(40*time.Second, "docker", "exec", w, "sh", "-c", rules.String())
	}
}

// partition isolates each group from every other group, in both directions.
func (h *harness) partition(groups ...[]int) {
	h.t.Helper()
	for i, a := range groups {
		for j, b := range groups {
			if i != j {
				h.dropTraffic(a, b)
			}
		}
	}
}

func (h *harness) heal() {
	h.t.Helper()
	healPartitions()
}

type probeResult struct {
	Code int    `json:"code"`
	Body string `json:"body"`
}

func (h *harness) probe(node int, path string) probeResult {
	h.t.Helper()
	var out probeResult
	h.ctl(fmt.Sprintf("/proxy?node=%d&path=%s", node, url.QueryEscape(path)), &out)
	return out
}

func (h *harness) assertCannotReach(from, to int) {
	h.t.Helper()
	if r := h.probe(from, fmt.Sprintf("/ping?peer=%d", to)); r.Code == 200 {
		h.fatalf("the partition is not working: node-%d can still reach node-%d", from, to)
	}
}

// assertPutRejected checks that a node does not acknowledge a write, and that
// the write later disappears.
func (h *harness) assertPutRejected(node int, key string) {
	h.t.Helper()
	h.absent = append(h.absent, key)
	if r := h.probe(node, fmt.Sprintf("/put?key=%s&ver=1&size=8", key)); r.Code == 200 {
		h.failf("SPLIT BRAIN: node-%d acknowledged write %s without a quorum", node, key)
	}
}

func (h *harness) killPod(node int) {
	h.t.Helper()
	kubectl("delete", "pod", fmt.Sprintf("node-%d", node), "--force", "--grace-period=0")
}

func without(items []int, drop ...int) []int {
	out := make([]int, 0, len(items))
	for _, v := range items {
		if !slices.Contains(drop, v) {
			out = append(out, v)
		}
	}
	return out
}

func allNodes() []int {
	out := make([]int, nodes)
	for i := range out {
		out[i] = i
	}
	return out
}

func sleep(d time.Duration) { time.Sleep(d) }

const defaultLoad = "writers=4&readers=4&keys=1000&size=1000"

// loaded deploys a cluster, starts the load, and returns the leader.
func (h *harness) loaded(p profile) (int, uint64) {
	h.t.Helper()
	h.deploy(p)
	h.waitLeader()
	h.startLoad(defaultLoad)
	sleep(8 * time.Second)
	return h.waitLeader()
}

// saveDiagnostics keeps the cluster state and node logs of a failed scenario.
func (h *harness) saveDiagnostics(name string) {
	dir := filepath.Join(".bin", "diagnostics", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	save := func(file string, args ...string) {
		out, _ := run(ctx, "kubectl", args...)
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("cluster.json", "exec", "e2e-client", "--", "/raftnode", "ctl", "/cluster")
	save("load.json", "exec", "e2e-client", "--", "/raftnode", "ctl", "/load/stats")
	save("pods.txt", "get", "pods", "-o", "wide")
	for i := range nodes {
		pod := fmt.Sprintf("node-%d", i)
		save(pod+".log", "logs", pod, "--tail=4000")
		save(pod+".previous.log", "logs", pod, "--previous", "--tail=4000")
		save(pod+".goroutines.json", "exec", "e2e-client", "--", "/raftnode", "ctl",
			fmt.Sprintf("/proxy?node=%d&path=%s", i, url.QueryEscape("/debug/pprof/goroutine?debug=1")))
	}
	h.failures = append(h.failures, "diagnostics saved in e2e/.bin/diagnostics/"+name)
}

type scenario struct {
	group, name, description string
	run                      func(*harness)
}

func TestE2E(t *testing.T) {
	for _, s := range scenarios() {
		t.Run(s.group+"/"+s.name, func(t *testing.T) {
			h := &harness{t: t}
			start := time.Now()
			defer func() {
				failed := t.Failed()
				if r := recover(); r != nil {
					failed = true
					h.failures = append(h.failures, fmt.Sprint(r))
					t.Errorf("%v", r)
				}
				if failed {
					h.saveDiagnostics(s.name)
				}
				report.add(s, h, time.Since(start), failed, t.Skipped())
			}()
			s.run(h)
			if !h.verified {
				h.fatalf("scenario finished without running the safety checks")
			}
		})
	}
}

func scenarios() []scenario {
	return []scenario{
		{"formation", "cluster_forms", "5 pods start on 5 machines and elect exactly one leader", testClusterForms},
		{"formation", "replicates_to_all", "Acknowledged writes reach all 5 replicas", testReplicatesToAll},
		{"formation", "followers_reject_clients", "Followers refuse client reads and writes", testFollowersReject},
		{"formation", "leader_stable_under_load", "No spurious elections during 40s of load", testLeaderStable},

		{"failover", "kill_leader", "Leader pod force-deleted under load", testKillLeader},
		{"failover", "kill_leader_repeatedly", "Leader pod force-deleted 6 times in a row", testKillLeaderRepeatedly},
		{"failover", "kill_follower", "Follower pod deleted; the service keeps running", testKillFollower},
		{"failover", "kill_two_followers", "2 followers deleted at once; quorum remains", testKillTwoFollowers},
		{"failover", "kill_leader_and_follower", "Leader and a follower deleted at once", testKillLeaderAndFollower},
		{"failover", "crash_leader_process", "Leader process killed with SIGKILL", testCrashLeaderProcess},
		{"failover", "freeze_leader_machine", "Leader's machine frozen, then resumed", testFreezeLeaderMachine},
		{"failover", "full_cluster_restart", "All 5 pods deleted; state recovers from disk", testFullRestart},
		{"failover", "rolling_restart", "Graceful restart of each pod in turn", testRollingRestart},
		{"failover", "snapshot_catch_up", "A lagging follower catches up through a large chunked snapshot", testSnapshotCatchUp},

		{"split_brain", "minority_leader", "Leader stranded with 1 follower (2|3)", func(h *harness) { testMinorityLeader(h, profile{}) }},
		{"split_brain", "minority_leader_prevote_lease", "Same, with PreVote and leader leases on", func(h *harness) { testMinorityLeader(h, profile{prevote: true, lease: true}) }},
		{"split_brain", "isolated_leader", "Leader fully isolated (1|4)", testIsolatedLeader},
		{"split_brain", "leader_in_majority", "Leader keeps a quorum (3|2)", testLeaderInMajority},
		{"split_brain", "isolated_follower", "One follower isolated, then healed", func(h *harness) { testIsolatedFollower(h, profile{}) }},
		{"split_brain", "isolated_follower_prevote", "Same with PreVote; the leader must not be disturbed", func(h *harness) { testIsolatedFollower(h, profile{prevote: true}) }},
		{"split_brain", "no_quorum_anywhere", "Three-way split (2|2|1); no write may succeed", testNoQuorum},
		{"split_brain", "one_way_partition", "Followers can hear the leader, but it cannot hear them", testOneWayPartition},
		{"split_brain", "bridge_partition", "Two sides connected only through one node", testBridgePartition},
		{"split_brain", "flapping_partitions", "8 rounds of random partitions under load", testFlapping},

		{"chaos", "mixed_faults", "12 random kills, crashes, freezes, and partitions", testChaos},
	}
}

func testClusterForms(h *harness) {
	start := time.Now()
	h.deploy(profile{})
	leader, term := h.waitLeader()
	h.m("start to leader", time.Since(start))

	pods := h.pods()
	hosts := map[string]bool{}
	for _, p := range pods {
		hosts[p.host] = true
	}
	h.m("distinct machines", len(hosts))
	if len(hosts) != nodes {
		h.failf("expected %d distinct machines, got %d: %+v", nodes, len(hosts), pods)
	}

	id := fmt.Sprintf("node-%d", leader)
	h.waitFor("all nodes to agree on the leader", leaderWait, func(s []nodeStatus) bool {
		for _, n := range s {
			if !n.OK || n.Leader != id || n.Term != term {
				return false
			}
		}
		return true
	})

	leaders := 0
	for _, n := range h.cluster() {
		if n.Role == "Leader" {
			leaders++
		}
	}
	h.m("leaders", leaders)
	if leaders != 1 {
		h.failf("expected exactly 1 leader, got %d", leaders)
	}
	h.verify()
}

func testReplicatesToAll(h *harness) {
	h.loaded(profile{})
	sleep(15 * time.Second)
	h.verify()
}

func testFollowersReject(h *harness) {
	leader, _ := h.loaded(profile{})
	for _, f := range without(allNodes(), leader) {
		key := fmt.Sprintf("follower-probe-%d", f)
		h.absent = append(h.absent, key)
		if r := h.probe(f, fmt.Sprintf("/put?key=%s&ver=1&size=8", key)); r.Code != 421 {
			h.failf("node-%d accepted a client write as a follower: %+v", f, r)
		}
		if r := h.probe(f, "/read?key=k1"); r.Code != 421 {
			h.failf("node-%d served a client read as a follower: %+v", f, r)
		}
	}
	if r := h.probe(leader, "/read?key=__absent__"); r.Code != 404 {
		h.failf("leader did not serve a read: %+v", r)
	}
	h.verify()
}

func testLeaderStable(h *harness) {
	leader, term := h.loaded(profile{})
	sleep(40 * time.Second)
	now, nowTerm := h.waitLeader()
	h.m("terms", fmt.Sprintf("%d -> %d", term, nowTerm))
	if now != leader || nowTerm != term {
		h.failf("spurious election: node-%d term %d became node-%d term %d", leader, term, now, nowTerm)
	}
	h.verify()
}

func testKillLeader(h *harness) {
	leader, term := h.loaded(profile{})
	h.killPod(leader)
	_, _, took := h.waitNewLeader(term, allNodes())
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())
	h.m("pod rejoined after", h.waitCaughtUp(leader))
	sleep(5 * time.Second)
	h.verify()
}

func testKillLeaderRepeatedly(h *harness) {
	leader, term := h.loaded(profile{})
	var times []string
	for range 6 {
		h.killPod(leader)
		next, nextTerm, took := h.waitNewLeader(term, allNodes())
		times = append(times, took.Round(100*time.Millisecond).String())
		h.waitServing()
		h.waitCaughtUp(leader)
		sleep(5 * time.Second)
		leader, term = next, nextTerm
		leader, term = h.waitLeader()
	}
	h.m("time to new leader per kill", strings.Join(times, ", "))
	h.verify()
}

func testKillFollower(h *harness) {
	leader, term := h.loaded(profile{})
	follower := without(allNodes(), leader)[0]
	h.killPod(follower)
	h.m("pod rejoined after", h.waitCaughtUp(follower))
	sleep(5 * time.Second)
	now, nowTerm := h.waitLeader()
	h.m("terms", fmt.Sprintf("%d -> %d", term, nowTerm))
	if now != leader {
		h.m("note", fmt.Sprintf("leadership moved from node-%d to node-%d", leader, now))
	}
	h.verify()
}

func testKillTwoFollowers(h *harness) {
	leader, _ := h.loaded(profile{})
	followers := without(allNodes(), leader)[:2]
	h.killPod(followers[0])
	h.killPod(followers[1])
	h.m("time to resume serving", h.waitServing())
	h.waitCaughtUp(followers[0])
	h.waitCaughtUp(followers[1])
	sleep(5 * time.Second)
	h.verify()
}

func testKillLeaderAndFollower(h *harness) {
	leader, term := h.loaded(profile{})
	follower := without(allNodes(), leader)[0]
	h.killPod(leader)
	h.killPod(follower)
	_, _, took := h.waitNewLeader(term, allNodes())
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())
	h.waitCaughtUp(leader)
	h.waitCaughtUp(follower)
	sleep(5 * time.Second)
	h.verify()
}

func testCrashLeaderProcess(h *harness) {
	leader, term := h.loaded(profile{})
	host := h.pods()[leader].host
	mustRun(30*time.Second, "docker", "exec", host, "pkill", "-9", "-x", "raftnode")
	_, _, took := h.waitNewLeader(term, allNodes())
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())
	h.m("process rejoined after", h.waitCaughtUp(leader))
	sleep(5 * time.Second)
	h.verify()
}

func testFreezeLeaderMachine(h *harness) {
	leader, term := h.loaded(profile{})
	host := h.pods()[leader].host

	mustRun(30*time.Second, "docker", "pause", host)
	_, newTerm, took := h.waitNewLeader(term, without(allNodes(), leader))
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())
	sleep(8 * time.Second)

	mustRun(30*time.Second, "docker", "unpause", host)
	h.m("frozen leader rejoined after", h.waitCaughtUp(leader))
	sleep(5 * time.Second)

	_, finalTerm := h.waitLeader()
	if finalTerm < newTerm {
		h.failf("term moved backwards after the freeze: %d -> %d", newTerm, finalTerm)
	}
	h.verify()
}

func testFullRestart(h *harness) {
	_, term := h.loaded(profile{})
	sleep(10 * time.Second)
	for i := range nodes {
		h.killPod(i)
	}
	_, _, took := h.waitNewLeader(term, allNodes())
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())
	for i := range nodes {
		h.waitCaughtUp(i)
	}
	sleep(5 * time.Second)
	h.verify()
}

func testRollingRestart(h *harness) {
	h.loaded(profile{})
	for i := range nodes {
		kubectl("delete", "pod", fmt.Sprintf("node-%d", i), "--grace-period=10")
		sleep(2 * time.Second)
		h.waitCaughtUp(i)
		h.waitServing()
		sleep(3 * time.Second)
	}
	h.verify()
}

func testSnapshotCatchUp(h *harness) {
	h.deploy(profile{})
	h.waitLeader()

	// 800 keys of 40 KB make a snapshot of about 32 MiB, above the 16 MiB gRPC limit.
	h.startLoad("writers=4&readers=2&keys=800&size=40000")
	sleep(35 * time.Second)

	leader, _ := h.waitLeader()
	follower := without(allNodes(), leader)[0]
	before := h.cluster()[follower]

	h.partition([]int{follower}, without(allNodes(), follower))
	h.assertCannotReach(follower, leader)
	h.waitFor("the leader to compact past the follower", recoverWait, func(s []nodeStatus) bool {
		l, _ := currentLeader(s)
		return l >= 0 && s[l].Snap > before.Applied+200
	})
	h.heal()

	h.m("follower caught up after", h.waitCaughtUp(follower))
	h.waitFor("the follower to install a snapshot", recoverWait, func(s []nodeStatus) bool {
		return s[follower].Restores > before.Restores
	})
	s := h.cluster()[follower]
	h.m("snapshots installed", s.Restores-before.Restores)
	h.m("follower snapshot index", fmt.Sprintf("%d -> %d", before.Snap, s.Snap))
	h.verify()
}

func testMinorityLeader(h *harness, p profile) {
	leader, term := h.loaded(p)
	follower := without(allNodes(), leader)[0]
	minority := []int{leader, follower}
	majority := without(allNodes(), minority...)

	h.partition(minority, majority)
	h.assertCannotReach(leader, majority[0])

	newLeader, _, took := h.waitNewLeader(term, majority)
	h.m("time to new leader in majority", took)
	h.m("time to resume serving", h.waitServing())

	// Leases are valid for at most half the election timeout, so wait them out.
	sleep(4 * time.Second)
	h.assertPutRejected(leader, "split-probe-1")
	h.assertPutRejected(follower, "split-probe-2")
	if r := h.probe(leader, "/read?key=k1"); r.Code == 200 {
		h.failf("SPLIT BRAIN: stranded leader node-%d served a read: %+v", leader, r)
	}
	if r := h.probe(newLeader, "/put?key=majority-probe&ver=1&size=8"); r.Code != 200 {
		h.failf("the majority leader rejected a write: %+v", r)
	}

	stranded := h.cluster()[leader]
	h.m("stranded node role during partition", stranded.Role)
	sleep(5 * time.Second)

	h.heal()
	h.m("stranded leader rejoined after", h.waitCaughtUp(leader))
	h.waitCaughtUp(follower)
	sleep(5 * time.Second)
	h.verify()
}

func testIsolatedLeader(h *harness) {
	leader, term := h.loaded(profile{})
	rest := without(allNodes(), leader)

	h.partition([]int{leader}, rest)
	h.assertCannotReach(leader, rest[0])

	_, _, took := h.waitNewLeader(term, rest)
	h.m("time to new leader", took)
	h.m("time to resume serving", h.waitServing())

	h.assertPutRejected(leader, "isolated-probe")
	if r := h.probe(leader, "/read?key=k1"); r.Code == 200 {
		h.failf("SPLIT BRAIN: isolated leader node-%d served a read: %+v", leader, r)
	}

	h.heal()
	h.m("isolated leader rejoined after", h.waitCaughtUp(leader))
	sleep(5 * time.Second)
	h.verify()
}

func testLeaderInMajority(h *harness) {
	leader, term := h.loaded(profile{})
	followers := without(allNodes(), leader)
	majority := []int{leader, followers[0], followers[1]}
	minority := []int{followers[2], followers[3]}

	h.partition(majority, minority)
	h.assertCannotReach(minority[0], leader)
	h.waitServing()

	sleep(15 * time.Second)
	during := h.cluster()[leader]
	h.m("leader during partition", fmt.Sprintf("%s in term %d (was %d)", during.Role, during.Term, term))
	if during.Role != "Leader" || during.Term != term {
		h.failf("the majority leader lost leadership during the partition: %+v", during)
	}
	h.assertPutRejected(minority[0], "minority-probe")

	h.heal()
	h.m("minority rejoined after", h.waitCaughtUp(minority[0]))
	h.waitCaughtUp(minority[1])
	sleep(5 * time.Second)
	h.verify()
}

func testIsolatedFollower(h *harness, p profile) {
	leader, term := h.loaded(p)
	follower := without(allNodes(), leader)[0]

	h.partition([]int{follower}, without(allNodes(), follower))
	h.assertCannotReach(follower, leader)

	sleep(40 * time.Second)
	isolated := h.cluster()[follower]
	h.m("isolated follower term", fmt.Sprintf("%d -> %d", term, isolated.Term))

	h.heal()
	h.m("follower rejoined after", h.waitCaughtUp(follower))
	sleep(8 * time.Second)

	now, nowTerm := h.waitLeader()
	h.m("cluster term", fmt.Sprintf("%d -> %d", term, nowTerm))
	if p.prevote {
		if isolated.Term != term {
			h.failf("with PreVote, an isolated follower must not raise its term: %d -> %d", term, isolated.Term)
		}
		if now != leader || nowTerm != term {
			h.failf("with PreVote, the rejoin must not disturb the leader: node-%d term %d -> node-%d term %d", leader, term, now, nowTerm)
		}
	}
	h.verify()
}

func testNoQuorum(h *harness) {
	leader, _ := h.loaded(profile{})
	rest := without(allNodes(), leader)
	groups := [][]int{{leader, rest[0]}, {rest[1], rest[2]}, {rest[3]}}

	h.partition(groups...)
	h.assertCannotReach(leader, rest[1])

	// Let requests in progress finish before measuring.
	sleep(5 * time.Second)
	before := h.stats()
	sleep(20 * time.Second)
	after := h.stats()
	h.m("writes acknowledged without quorum", after.WritesOK-before.WritesOK)
	if after.WritesOK != before.WritesOK {
		h.failf("SPLIT BRAIN: %d writes were acknowledged with no quorum anywhere", after.WritesOK-before.WritesOK)
	}

	// These writes may legally survive, because no leader replaced them in any log.
	for _, n := range allNodes() {
		key := fmt.Sprintf("noquorum-probe-%d", n)
		if r := h.probe(n, fmt.Sprintf("/put?key=%s&ver=1&size=8", key)); r.Code == 200 {
			h.failf("SPLIT BRAIN: node-%d acknowledged write %s with no quorum anywhere", n, key)
		}
	}

	h.heal()
	h.m("time to resume serving", h.waitServing())
	for _, n := range allNodes() {
		h.waitCaughtUp(n)
	}
	sleep(5 * time.Second)
	h.verify()
}

func testOneWayPartition(h *harness) {
	leader, term := h.loaded(profile{})
	h.dropTraffic(without(allNodes(), leader), []int{leader})
	sleep(30 * time.Second)

	now, nowTerm := h.waitLeader()
	h.m("leader during partition", fmt.Sprintf("node-%d term %d (was node-%d term %d)", now, nowTerm, leader, term))

	h.heal()
	h.m("time to resume serving", h.waitServing())
	for _, n := range allNodes() {
		h.waitCaughtUp(n)
	}
	sleep(5 * time.Second)
	h.verify()
}

func testBridgePartition(h *harness) {
	leader, _ := h.loaded(profile{})
	rest := without(allNodes(), leader)

	// node rest[0] can reach both sides, and the two sides cannot reach each other.
	left, right := []int{leader, rest[1]}, []int{rest[2], rest[3]}
	h.partition(left, right)
	h.assertCannotReach(left[0], right[0])
	sleep(30 * time.Second)

	h.heal()
	h.m("time to resume serving", h.waitServing())
	for _, n := range allNodes() {
		h.waitCaughtUp(n)
	}
	sleep(5 * time.Second)
	h.verify()
}

func testFlapping(h *harness) {
	h.loaded(profile{})
	rng := rand.New(rand.NewSource(20261109))

	for range 8 {
		order := rng.Perm(nodes)
		h.partition(order[:2], order[2:])
		sleep(10 * time.Second)
		h.heal()
		sleep(8 * time.Second)
	}
	h.m("time to resume serving", h.waitServing())
	for _, n := range allNodes() {
		h.waitCaughtUp(n)
	}
	sleep(5 * time.Second)
	h.verify()
}

func testChaos(h *harness) {
	h.loaded(profile{})
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	h.m("seed", seed)

	var faults []string
	for range 12 {
		leader, _ := currentLeader(h.cluster())
		target := rng.Intn(nodes)
		if leader >= 0 && rng.Intn(2) == 0 {
			target = leader
		}

		switch rng.Intn(4) {
		case 0:
			faults = append(faults, fmt.Sprintf("kill node-%d", target))
			h.killPod(target)
		case 1:
			faults = append(faults, fmt.Sprintf("crash node-%d", target))
			mustRun(30*time.Second, "docker", "exec", h.pods()[target].host, "pkill", "-9", "-x", "raftnode")
		case 2:
			faults = append(faults, fmt.Sprintf("freeze node-%d", target))
			host := h.pods()[target].host
			mustRun(30*time.Second, "docker", "pause", host)
			sleep(10 * time.Second)
			mustRun(30*time.Second, "docker", "unpause", host)
		default:
			order := rng.Perm(nodes)
			faults = append(faults, fmt.Sprintf("partition %v|%v", order[:2], order[2:]))
			h.partition(order[:2], order[2:])
			sleep(10 * time.Second)
			h.heal()
		}

		h.waitServing()
		for _, n := range allNodes() {
			h.waitCaughtUp(n)
		}
		sleep(3 * time.Second)
	}
	h.m("faults", strings.Join(faults, "; "))
	h.verify()
}

type result struct {
	scenario scenario
	metrics  []string
	failures []string
	duration time.Duration
	status   string
}

type resultsReport struct {
	started time.Time
	results []result
}

func (r *resultsReport) start() { r.started = time.Now() }

func (r *resultsReport) add(s scenario, h *harness, d time.Duration, failed, skipped bool) {
	status := "PASS"
	switch {
	case failed:
		status = "FAIL"
	case skipped:
		status = "SKIP"
	}
	r.results = append(r.results, result{s, h.metrics, h.failures, d, status})
	r.write()
}

func commandOutput(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := run(ctx, name, args...)
	if err != nil {
		return "unknown"
	}
	return out
}

var whitespace = regexp.MustCompile(`\s+`)

func cell(s string) string {
	return strings.ReplaceAll(whitespace.ReplaceAllString(s, " "), "|", "\\|")
}

// write replaces RESULTS.md, so a partial run still leaves a report.
func (r *resultsReport) write() {
	passed, failed := 0, 0
	for _, res := range r.results {
		switch res.status {
		case "PASS":
			passed++
		case "FAIL":
			failed++
		}
	}

	commit := commandOutput("git", "rev-parse", "--short", "HEAD")
	if commandOutput("git", "status", "--porcelain", "--", "../raft", "../storage", "../types", "../logger") != "" {
		commit += " with uncommitted library changes"
	}

	var b strings.Builder
	b.WriteString("# Raft end-to-end results\n\n")
	b.WriteString("Generated by `./e2e/run.sh`. Do not edit by hand.\n\n")
	fmt.Fprintf(&b, "**%d passed, %d failed, of %d scenarios run.**\n\n", passed, failed, len(r.results))
	fmt.Fprintf(&b, "- Run started: %s\n", r.started.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "- Total time: %s\n", time.Since(r.started).Round(time.Second))
	fmt.Fprintf(&b, "- Library commit: `%s`\n", commit)
	fmt.Fprintf(&b, "- Tools: %s; Kubernetes %s; %s\n",
		commandOutput("kind", "version"),
		commandOutput("kubectl", "get", "nodes", "-o", "jsonpath={.items[0].status.nodeInfo.kubeletVersion}"),
		commandOutput("go", "version"))
	b.WriteString("- Cluster: 5 Raft pods on 5 `kind` workers, with real gRPC, file storage, and fsync on\n")
	b.WriteString("- Raft: base election timeout 50 ticks of 100 ms, randomization factor 2.0, snapshots every 1000 entries\n")
	b.WriteString("- Load: 4 writers and 4 readers\n\n")
	b.WriteString("Timings come from one laptop and include polling delays. Treat them as health checks, not benchmarks.\n\n")

	b.WriteString("## Safety checks in every scenario\n\n")
	b.WriteString("- No acknowledged write is lost.\n")
	b.WriteString("- No read returns a value older than one already acknowledged.\n")
	b.WriteString("- Never two leaders in the same term.\n")
	b.WriteString("- A node's term never moves backwards, including across restarts.\n")
	b.WriteString("- A node's commit index never moves backwards while its process runs.\n")
	b.WriteString("- All 5 replicas end with identical state.\n\n")

	b.WriteString("## Summary\n\n| Group | Scenario | What it does | Result | Time |\n| :--- | :--- | :--- | :--- | :--- |\n")
	for _, res := range r.results {
		fmt.Fprintf(&b, "| %s | `%s` | %s | **%s** | %s |\n", res.scenario.group, res.scenario.name,
			cell(res.scenario.description), res.status, res.duration.Round(time.Second))
	}

	if failed > 0 {
		b.WriteString("\n## Failures\n")
		for _, res := range r.results {
			if res.status != "FAIL" {
				continue
			}
			fmt.Fprintf(&b, "\n### `%s/%s`\n\n", res.scenario.group, res.scenario.name)
			for _, f := range res.failures {
				fmt.Fprintf(&b, "- %s\n", cell(f))
			}
		}
	}

	b.WriteString("\n## Measurements\n")
	for _, res := range r.results {
		fmt.Fprintf(&b, "\n### `%s/%s`\n\n", res.scenario.group, res.scenario.name)
		for _, m := range res.metrics {
			fmt.Fprintf(&b, "- %s\n", cell(m))
		}
	}

	if err := os.WriteFile("RESULTS.md", []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write RESULTS.md:", err)
	}
}
