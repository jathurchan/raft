# End-to-end tests

These tests run a real 5-node Raft cluster on [kind](https://kind.sigs.k8s.io/) and inject failures while clients read and write. Each node uses real gRPC, file storage, and fsync.

## Running

You need Go, `kubectl`, and a running Docker daemon. The runner installs `kind` into `e2e/.bin` if it is missing.

```bash
./e2e/run.sh                                 # every scenario
./e2e/run.sh 'TestE2E/split_brain'           # one group
./e2e/run.sh 'TestE2E/failover/kill_leader'  # one scenario
```

A full run takes about 30 minutes and needs about 6 GB of memory for Docker. It creates a `kind` cluster named `raft-e2e` with its own kubeconfig, so your Kubernetes settings are not touched. The cluster is deleted afterwards.

| Variable | Effect |
| :--- | :--- |
| `E2E_KEEP_CLUSTER=1` | Keep the cluster after the run and reuse it next time |
| `E2E_TIMEOUT=170m` | Overall time limit |

## Results

Each run rewrites [`RESULTS.md`](RESULTS.md), including after every scenario, so an interrupted run still leaves a report. When a scenario fails, its cluster state and node logs are saved in `e2e/.bin/diagnostics/<scenario>/`.

## What is tested

| Group | Coverage |
| :--- | :--- |
| `formation` | Cluster start, leader election, replication, follower rejection, leader stability |
| `failover` | Killed leaders and followers, process crashes, frozen machines, full and rolling restarts, snapshot catch-up |
| `split_brain` | Minority, isolated, majority, three-way, one-way, bridge, and flapping network partitions |
| `chaos` | Random mixes of the faults above |

Every scenario must also pass these checks:

- No acknowledged write is lost.
- No read returns a value older than one already acknowledged.
- There are never two leaders in the same term.
- A node's term never moves backwards, even across restarts.
- A node's commit index never moves backwards while its process runs.
- All replicas end with identical state.

## How faults are injected

- **Pod failure:** `kubectl delete pod --force --grace-period=0`.
- **Process crash:** `SIGKILL` from the host machine.
- **Machine freeze:** `docker pause` on the `kind` worker.
- **Network partition:** `iptables` drop rules on the workers. They use the `raw` table so that established connections are cut too.

The load generator runs on the control-plane node, outside the partitions, so it can talk to both sides of a split.

The code is behind the `e2e` build tag, so it is not part of `go build ./...`, `go test ./...`, or coverage.
