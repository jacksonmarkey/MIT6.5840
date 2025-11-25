# MIT 6.5840 — Distributed Systems (Labs)

This repository contains my solutions for the MIT 6.5840 *Distributed Systems* course (2025) [http://nil.csail.mit.edu/6.5840/2025/index.html]. The labs progressively build core distributed systems components—including a replicated state machine, a fault-tolerant key/value store, and snapshotting/log-compaction mechanisms—using the Raft consensus protocol.

## Labs Included

### Lab 1 — MapReduce
Implements a simplified MapReduce framework with a master/worker architecture. Focuses on task scheduling, worker crash recovery, and managing concurrent job execution.

### Lab 2 — Key/Value Server
Implements a linearizable key/value server on a single machine with at-most-once semantics.

### Lab 3 — Raft (Leader Election, Log Replication, Persistence, & Log Compaction)
A full implementation of the Raft consensus algorithm:
- Leader election  
- Log replication  
- Snapshotting for persisting after crashes
- Log compaction for faster persistence

### Lab 4 — Fault-Tolerant Key/Value Store
Builds a linearizable KV store on top of Raft:
- Client request deduplication  
- Exactly-once semantics
- Persistence after crashes via snapshots

## Testing

The repository includes the official 6.5840 test suite (`go test ./...`).  
Each lab passes the course’s required tests unless otherwise noted.

Run tests with:

```sh
go test -run 2A
go test -run 3B -race
go test ./...
