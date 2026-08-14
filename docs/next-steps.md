# What We Do Now

The next move is to stop expanding design docs and start building a small, testable vertical slice. We should implement the project in PR-sized increments, always keeping the educational goal in mind.

## Immediate objective

Build the smallest runnable version of `space-node` that proves this path:

```text
manual event input
  -> changelog message
  -> in-memory executor graph
  -> aggregate materialized view
  -> query current view state
```

Storage, SQL, connectors, and distributed execution come after this vertical slice works.

## PR 1: Core types and in-memory materialized view — implemented

### Focus

Create the core Go module and types that every later component will use.

### Files to add

```text
go.mod
internal/types/value.go
internal/types/row.go
internal/types/schema.go
internal/types/change.go
internal/types/barrier.go
internal/storage/memory.go
internal/engine/engine.go
internal/engine/materialized_view.go
internal/engine/engine_test.go
```

### Build

Implement:

- `Value`, `Row`, `Schema`, and `Column`;
- `Change` with `Insert`, `Delete`, and `Update` operations;
- `Barrier` with an epoch number;
- an in-memory view store;
- a tiny engine that accepts events and updates one aggregate view.

### Done when

A test can:

1. create an `orders` stream definition;
2. create a `user_spend` aggregate view manually;
3. insert two events for the same user;
4. read `user_spend[user_id]` and see the sum update incrementally.

## PR 2: Executor pipeline — implemented

### Focus

Replace hard-coded view logic with a simple executor chain.

### Build

Implement:

- `Executor` interface;
- `SourceExecutor`;
- `FilterExecutor`;
- `ProjectExecutor`;
- `HashAggExecutor`;
- `MaterializeExecutor`.

### Done when

The same aggregate test is expressed as:

```text
Source -> Filter -> HashAggregate -> Materialize
```

## PR 3: Epochs and barriers — next

### Focus

Introduce the barrier/checkpoint concept before adding disk storage.

### Build

Implement:

- `Message` as either `Change` or `Barrier`;
- epoch assignment;
- barrier propagation through executors;
- an in-memory checkpoint marker.

### Done when

A test can insert events in epoch 1, send a barrier, insert events in epoch 2, and verify the materialized view state after each epoch.

## PR 4: Storage interface

### Focus

Add the interface that the future Hummock-inspired storage engine will implement.

### Build

Implement:

```go
type Store interface {
    NewBatch(epoch uint64) Batch
    Get(readEpoch uint64, key []byte) ([]byte, error)
    Scan(readEpoch uint64, start, end []byte) Iterator
    Seal(epoch uint64) error
    Checkpoint(epoch uint64) error
}
```

Start with an in-memory MVCC implementation.

### Done when

Tests cover:

- put/get at latest epoch;
- historical reads;
- deletes/tombstones;
- scans at a read epoch.

## PR 5: Local `streamstore` prototype

### Focus

Only after the execution path works, add the educational storage engine.

### Build

Implement:

- WAL append;
- mutable memtable;
- immutable memtable;
- simple SSTable writer/reader;
- manifest file;
- checkpoint flush;
- reopen from disk.

### Done when

A test can write state, checkpoint, close the store, reopen it, and read the same materialized view state.

## PR 6: Compaction

### Focus

Add safe-epoch-aware cleanup.

### Build

Implement a simple compactor that:

- keeps all versions newer than the safe epoch;
- keeps the newest version at or before the safe epoch;
- drops older versions;
- preserves tombstones only while they are needed.

### Done when

Tests prove compaction does not lose future versions and does remove old unreachable versions.

## PR 7: Minimal HTTP API

### Focus

Make the vertical slice runnable outside tests.

### Build

Implement:

```text
POST /streams/{name}
GET  /views/{name}
POST /checkpoint
GET  /debug/catalog
GET  /debug/storage
```

### Done when

A developer can run the server, post JSON order events, and query the current materialized view.

## PR 8: Minimal SQL

### Focus

Add SQL only after the manual API and executor graph are already working.

### Build

Support:

```sql
CREATE STREAM
CREATE MATERIALIZED VIEW
SELECT ... FROM ... WHERE ... GROUP BY
COUNT
SUM
```

### Done when

A developer can create the `orders` stream and `user_spend` materialized view through `/sql`.

## Decision for the very next change

Start with **PR 1: Core types and in-memory materialized view**.

Do not implement disk storage yet. The storage engine will be much easier to design correctly after the engine has a real state access pattern from materialized views.
