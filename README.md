# Raft

[![Unit Tests](https://github.com/jathurchan/raft/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/jathurchan/raft/actions/workflows/unit-tests.yml)
[![Integration Tests](https://github.com/jathurchan/raft/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/jathurchan/raft/actions/workflows/integration-tests.yml)
[![codecov](https://codecov.io/gh/jathurchan/raft/graph/badge.svg?token=RW0H2MKNMV)](https://codecov.io/gh/jathurchan/raft)
[![Lint](https://github.com/jathurchan/raft/actions/workflows/lint.yml/badge.svg)](https://github.com/jathurchan/raft/actions/workflows/lint.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/jathurchan/raft)](https://goreportcard.com/report/github.com/jathurchan/raft)

`raft` is a fault-tolerant distributed consensus library for coordinating distributed applications. It is written in Go and is designed to be a reliable and highly available solution for managing consensus across clusters of machines.

## What the project does

In distributed systems, coordinating actions between different services or nodes can be a complex challenge. `raft` provides a simple and robust solution for this by offering a distributed consensus mechanism. This ensures that nodes in a cluster agree on state transitions and execution logs in an identical sequence, preventing data corruption and ensuring consistency across your distributed application.

The project is built on top of the **Raft consensus algorithm**, which guarantees fault tolerance. This means that `raft` can withstand failures of some of its nodes without losing availability or data.

## Why the project is useful

`raft` is useful for a variety of use cases in distributed systems, including:

* **Leader Election:** Electing a single leader from a group of nodes to perform a specific task.
* **Distributed Cron Jobs:** Ensuring that a scheduled task is executed by only one node in a cluster.
* **Resource Locking (`raftlock`):** Providing the consensus backbone for distributed lock managers and fencing tokens to prevent multiple processes from concurrently modifying shared resources.
* **Replicated Key-Value Stores (`raftkv`):** Enabling linearizable, high-availability key-value storage with automatic failover and crash recovery.
* **Distributed Semaphores:** Limiting the number of concurrent processes that can access a pool of resources.

By providing a reliable and easy-to-use distributed consensus library, `raft` simplifies the development of robust and scalable distributed applications.

## Architecture Overview

`raft` is composed of several key packages, each handling a specific aspect of the distributed consensus service:

* **`raft/`**: Contains a custom-built implementation of the Raft consensus algorithm. This is the core engine for maintaining strong consistency and fault tolerance across the cluster, implementing leader election, log replication, snapshotting, and peer-to-peer gRPC networking. Dive deeper into its design in the [Raft Consensus Module README](raft/README.md).
* **`raft/proto/`**: Defines the Protocol Buffer messages and generated gRPC service for Raft peer-to-peer communication, ensuring type-safe and efficient communication between cluster nodes.
* **`storage/`**: Offers a durable and crash-resilient persistence layer for Raft data, including persistent state, log entries, and snapshots. Details can be found in the [Storage Package README](storage/README.md).
* **`types/`**: Defines the foundational data types and structures used across Raft, including terms, log indices, node roles, log entries, and RPC arguments and replies.
* **`logger/`**: Provides structured, context-aware logging with support for persistent fields like node IDs and terms.
* **`testutil/`**: Contains shared testing utilities and assertion helpers for unit and integration testing.

## How users can get started with the project

You can include `raft` as a dependency in your Go project or build and run tests from the source code.

### Installation

To add `raft` to your Go module:

```bash
go get github.com/jathurchan/raft
```

### Building from Source

If you prefer to build and test directly from the source code:

**Prerequisites:**
- **Go**: 1.23 or higher
- **Protocol Buffers**: `protoc` compiler and Go plugins (only if modifying `.proto` files)

1. **Clone the repository:**

    ```bash
    git clone https://github.com/jathurchan/raft.git
    cd raft
    ```

2. **Install Protobuf compiler and Go plugins:**
    - Install `protoc`:
      - **macOS**: `brew install protobuf`
      - **Linux (Ubuntu/Debian)**: `sudo apt-get install -y protobuf-compiler`
    - Install the Go protobuf plugins:
      ```bash
      go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
      go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
      ```
      Make sure `$(go env GOPATH)/bin` is in your system's `PATH`.

3. **Generate Protobuf files (optional):**

    ```bash
    protoc --proto_path=. \
      --go_out=. --go_opt=paths=source_relative \
      --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      raft/proto/raft.proto
    ```

4. **Run the test suite:**

    ```bash
    go test -v ./...
    ```

5. **Run multi-node cluster integration tests:**

    ```bash
    go test -v -timeout=5m -tags=integration ./raft -run=TestRaftIntegration
    ```

6. **Run the end-to-end failure tests on a 5-node `kind` cluster** (needs Docker and `kubectl`, see [`e2e/README.md`](e2e/README.md)):

    ```bash
    ./e2e/run.sh
    ```

## Where users can get help with your project

If you have any questions or encounter any issues while using `raft`, please feel free to open an issue on the [**GitHub repository**](https://github.com/jathurchan/raft/issues). We will do our best to help you as soon as possible.

## Contributing

We welcome contributions from the community\! If you are interested in contributing to `raft`, please follow these steps:

1. **Fork the repository** on GitHub.
2. **Create a new branch** for your feature or bug fix: `git checkout -b my-new-feature`.
3. **Make your changes** and commit them with clear, descriptive messages.
4. **Run the tests** to ensure everything is working: `go test ./...`.
5. **Push your branch** to your fork: `git push origin my-new-feature`.
6. **Submit a pull request** to the `main` branch of the `jathurchan/raft` repository.

## Who maintains and contributes to the project

`raft` is maintained by a team of dedicated developers:

* **Jathurchan Selvakumar**
* **Patrice Zhou**
* **Mathusan Selvakumar**

We welcome contributions from the community\! If you are interested in contributing to `raft`, please fork the repository and submit a pull request.
