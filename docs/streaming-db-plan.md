# Educational Streaming Database Plan

This document describes a staged plan for building an educational streaming database in Go inspired by RisingWave concepts, including a simple storage engine inspired by Hummock.

The goal is not to clone RisingWave. The goal is to preserve the important ideas in a codebase that is small enough to read, modify, and teach from.

## Related exploration

After inspecting the RisingWave repository, see [`risingwave-focus-map.md`](risingwave-focus-map.md) for the focused set of concepts we should preserve in `space-node` and the production details we should intentionally defer.

## 1. Product goal

Build a single-node streaming database that can:

1. ingest append-only events;
2. maintain materialized views incrementally;
3. expose current view state through a simple API;
4. persist streaming state through an educational storage engine;
5. later evolve toward SQL, windows, joins, connectors, and distributed execution.

A minimal demo should look like this:

```sql
CREATE STREAM orders (
  user_id INT,
  amount DOUBLE,
  created_at TIMESTAMP
);

CREATE MATERIALIZED VIEW user_spend AS
SELECT user_id, SUM(amount)
FROM orders
GROUP BY user_id;
```

Then an inserted order should update `user_spend` without recomputing the whole view.

## 2. Core concepts to keep

### Streams and tables

Model streams as ordered changelog records and tables/materialized views as keyed state derived from those records.

```go
type Op int

const (
    Insert Op = iota
    Delete
    Update
)

type Change struct {
    Epoch  uint64
    Op     Op
    Source string
    Key    []byte
    Value  []byte
}
```

### Incremental materialized views

Each input record should flow through an executor graph and update only affected keys.

Example aggregation state:

```text
orders event:       { user_id: 7, amount: 20 }
previous view row:  user_spend[7] = 100
new view row:       user_spend[7] = 120
```

### Epochs and checkpoints

Use epochs as logical barriers between batches of streaming changes. Operators write state updates under an epoch. A checkpoint makes an epoch durable and eligible for compaction after it is no longer needed by readers.

### Storage optimized for streaming state

The storage engine should serve stateful operators, not general OLTP workloads. It should support:

- point gets by key at a read epoch;
- batch writes for one epoch;
- flush at checkpoint barriers;
- compaction after a safe epoch;
- recovery from manifest and immutable table files.

## 3. Proposed repository layout

```text
cmd/space-node/
  main.go

internal/catalog/
  catalog.go
  schema.go

internal/types/
  change.go
  row.go
  value.go

internal/engine/
  engine.go
  pipeline.go

internal/executor/
  executor.go
  source.go
  filter.go
  project.go
  aggregate.go
  materialize.go
  window.go
  join.go

internal/storage/
  engine.go
  memtable.go
  sstable.go
  manifest.go
  checkpoint.go
  compactor.go

internal/sql/
  ast.go
  parser.go
  planner.go

internal/server/
  http.go
```

## 4. Execution engine plan

### Phase 1: Manual pipelines before SQL

Start with a Go API so storage and execution can be tested without a parser.

```go
engine.CreateStream("orders", schema)
engine.CreateMaterializedView(
    "user_spend",
    pipeline.Source("orders").GroupBy("user_id").Sum("amount"),
)
```

### Phase 2: Basic operators

Implement operators in this order:

1. source;
2. filter;
3. project;
4. hash aggregate;
5. materialize;
6. tumbling window aggregate;
7. stream-table join.

Each operator should accept `Change` records and emit zero or more `Change` records.

```go
type Executor interface {
    Process(ctx context.Context, change Change) ([]Change, error)
}
```

### Phase 3: Materialized view serving

Expose simple HTTP endpoints first:

```text
POST /streams/{stream_name}
GET  /views/{view_name}
GET  /catalog
POST /checkpoint
```

Avoid PostgreSQL wire compatibility until the educational engine is stable.

## 5. Storage engine plan: `streamstore`

The storage engine should be planned as a separate package, for example `internal/storage/streamstore`. It should be inspired by Hummock concepts while staying intentionally small.

### 5.1 Storage API

Start with a minimal interface:

```go
type Engine interface {
    NewBatch(epoch uint64) Batch
    Get(readEpoch uint64, key []byte) ([]byte, error)
    Scan(readEpoch uint64, start, end []byte) Iterator
    Checkpoint(epoch uint64) error
    Compact(safeEpoch uint64) error
    Close() error
}

type Batch interface {
    Put(key, value []byte)
    Delete(key []byte)
    Commit() error
}
```

The execution engine should only depend on this interface, not on file formats.

### 5.2 Data model

Every stored value is versioned by epoch:

```text
user:7 @ epoch 10 -> {sum: 120}
user:7 @ epoch 11 -> {sum: 150}
user:7 @ epoch 12 -> tombstone
```

Reads use snapshot semantics:

```text
Get(readEpoch=11, key=user:7) returns epoch 11 value
Get(readEpoch=12, key=user:7) returns not found
```

### 5.3 Components

```text
Memtable
  mutable map of key+epoch -> value/tombstone

Immutable memtable
  frozen memtable waiting to flush

SSTable
  immutable sorted file of versioned key/value records

Manifest
  metadata for SSTables, epochs, checkpoints, and compaction state

Compactor
  rewrites old versions and tombstones after safe epoch advances
```

### 5.4 File layout

Use a simple debuggable layout first:

```text
data/
  manifest.json
  wal/
    000001.log
  sst/
    000001.sst
    000002.sst
  checkpoints/
    epoch-000010.json
```

JSON is acceptable for the first prototype. After behavior is clear, switch SSTables to a binary block format.

### 5.5 Write path

```text
1. Streaming executor produces state updates for epoch N.
2. Updates are appended to WAL.
3. Updates are inserted into memtable.
4. Barrier/checkpoint arrives for epoch N.
5. Memtable is frozen and flushed into an immutable SSTable.
6. Manifest records the new SSTable and committed epoch.
```

### 5.6 Read path

```text
1. Reader asks for key K at read epoch R.
2. Check mutable memtable.
3. Check immutable memtables.
4. Check newest SSTables first.
5. Return the newest version whose epoch <= R.
6. If that version is a tombstone, return not found.
```

### 5.7 Compaction plan

Compaction should be safe-epoch aware.

```text
safe epoch = oldest epoch still needed by any reader/checkpoint/recovery task
```

For each key:

- keep all versions newer than the safe epoch;
- keep the newest version at or before the safe epoch;
- drop older versions;
- keep tombstones only while needed to hide older values.

This rule prevents compaction from deleting future versions while still allowing old state to be reclaimed.

### 5.8 Recovery plan

On restart:

1. read `manifest.json`;
2. load SSTable metadata;
3. find latest committed checkpoint;
4. replay WAL records after that checkpoint;
5. rebuild mutable state;
6. resume from the next epoch.

## 6. Milestones

### Milestone A: In-memory database

- Define `Change`, `Row`, `Schema`.
- Create streams programmatically.
- Maintain one aggregate materialized view in memory.
- Query view state over HTTP.

### Milestone B: Storage interface

- Add `internal/storage` interfaces.
- Keep an in-memory implementation.
- Make materialized views use the storage interface.

### Milestone C: Local streaming storage prototype

- Add memtable.
- Add JSON SSTable files.
- Add manifest.
- Add checkpoint flush.
- Add snapshot reads by epoch.
- Add tombstones.

### Milestone D: Compaction

- Add safe epoch tracking.
- Add compaction tests for old versions, tombstones, and future versions.
- Add metrics for SSTable count and compacted bytes.

### Milestone E: SQL subset

Support only:

```sql
CREATE STREAM
CREATE MATERIALIZED VIEW
SELECT ... FROM ... WHERE ... GROUP BY
COUNT
SUM
```

### Milestone F: Streaming features

- Tumbling windows.
- Stream-table joins.
- File source.
- HTTP source.
- Optional Kafka source.

### Milestone G: Distributed learning mode

Only after single-node storage and execution are clear, add a toy distributed mode:

```text
coordinator -> assigns fragments and epochs
worker      -> executes operators and owns keyed state
frontend    -> accepts SQL and serves views
```

## 7. Testing strategy

### Storage tests

- put/get at latest epoch;
- historical reads;
- delete tombstones;
- checkpoint and reopen;
- compaction keeps newest safe version;
- compaction preserves versions newer than safe epoch;
- manifest corruption returns clear errors;
- WAL replay after crash.

### Execution tests

- single insert updates aggregate view;
- multiple inserts update same group;
- deletes decrement aggregate state;
- filter prevents updates;
- checkpoint and recovery preserve materialized views.

### Integration tests

- start server;
- create stream;
- create materialized view;
- post events;
- query view;
- checkpoint;
- restart;
- query same view.

## 8. What not to build first

Do not start with:

- full SQL compatibility;
- distributed scheduling;
- PostgreSQL wire protocol;
- Kafka or CDC connectors;
- binary SSTable optimizations;
- object storage integration;
- cost-based optimizer.

Those can come later. The first objective is to make incremental view maintenance and streaming state storage understandable.

## 9. Recommended first pull requests

1. Add core types and in-memory stream engine.
2. Add manual materialized-view pipeline API.
3. Add HTTP ingestion and view querying.
4. Add storage interfaces with in-memory implementation.
5. Add local `streamstore` prototype with memtable, manifest, and JSON SSTables.
6. Add checkpoint/recovery.
7. Add compaction.
8. Add minimal SQL parser and planner.

## 10. Definition of done for the first educational version

The first version is complete when a developer can:

1. create an `orders` stream;
2. define a `user_spend` materialized view;
3. insert order events;
4. observe incremental updates;
5. checkpoint state;
6. restart the process;
7. query the recovered materialized view;
8. read the storage files and understand what happened.
