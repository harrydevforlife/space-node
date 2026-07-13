# RisingWave Exploration Focus Map

This note summarizes what to focus on after exploring the RisingWave repository structure. It is a learning map for building `space-node`, not a requirement to reproduce RisingWave's full production architecture.

## What RisingWave is organized around

RisingWave is split into major crates/components. The most relevant ones for our educational Go version are:

| RisingWave area | Observed location | What to learn from it | What to build in `space-node` |
| --- | --- | --- | --- |
| SQL frontend and planner | `src/frontend` | SQL handlers, expressions, optimizer plans, stream fragment generation | A tiny SQL subset and logical/physical plan builder |
| Stream engine | `src/stream` | Executors, barriers, actors, materialized-view maintenance | Single-node executor graph with barriers and stateful operators |
| Storage engine | `src/storage` | `StateStore`, local state, Hummock storage, SSTables, compaction | Educational `streamstore` with MVCC epochs, memtables, SSTables, checkpoints |
| Metadata/control plane | `src/meta` | Catalog, stream actors, barrier scheduling, compaction tasks | Simple catalog and checkpoint coordinator |
| Connectors | `src/connector` and integration tests | Source/sink ecosystem and data formats | HTTP/file first, Kafka later |
| Batch/query serving | `src/batch` | Reading materialized state for ad hoc queries | Simple view scans over state store |
| Protocol layer | `src/utils/pgwire` | PostgreSQL-compatible client access | Defer; use HTTP/CLI first |
| Protobuf/API contracts | `proto` and `src/prost` | Internal RPC/message contracts | Defer; use in-process interfaces first |

## Highest-value concepts to preserve

### 1. Frontend creates stream plans, not just SQL strings

The important lesson from the frontend is that SQL should become a durable plan. For `space-node`, focus on a small planning pipeline:

```text
SQL string
  -> AST
  -> logical plan
  -> physical stream plan
  -> executor graph
```

Do not start with a complete optimizer. Start with deterministic rules for:

- `CREATE STREAM`;
- `CREATE MATERIALIZED VIEW`;
- `SELECT ... FROM ... WHERE ... GROUP BY`;
- `COUNT`, `SUM`, and simple projections.

### 2. Streaming execution is message-driven

RisingWave's stream engine is organized around executors that process stream messages, including data chunks and barriers. For `space-node`, model the same idea with simpler records:

```go
type Message struct {
    Change  *Change
    Barrier *Barrier
}

type Barrier struct {
    Epoch uint64
}
```

The first executor graph should support:

```text
Source -> Filter -> Project -> HashAggregate -> Materialize
```

Then add:

```text
Source -> WindowAggregate -> Materialize
Source + Table -> StreamTableJoin -> Materialize
```

### 3. Barriers connect execution and storage

The key architectural bridge is the barrier/checkpoint flow:

```text
coordinator emits barrier(epoch N)
  -> all executors finish epoch N work
  -> state store seals/flushed epoch N
  -> manifest records durable checkpoint
  -> coordinator marks epoch N committed
```

This should be a first-class feature in `space-node`, even if the first version is single-process.

### 4. Materialized views are stateful operators

A materialized view is not just a stored query. It is an operator that consumes changelog messages and updates keyed state. Focus on:

- primary key encoding;
- insert/delete/update handling;
- idempotent state writes per epoch;
- recovery from storage after restart;
- serving current view state.

### 5. Storage is built for versioned streaming state

The storage engine should focus on the path used by stateful streaming operators:

```text
local writes for current epoch
snapshot reads at a committed epoch
checkpoint flush
safe-epoch compaction
recovery
```

This is more important than generic database features like transactions, secondary indexes, or SQL isolation levels.

## Storage-engine focus based on Hummock

For our educational implementation, build `streamstore` in stages.

### Stage 1: Interface and in-memory implementation

Define storage interfaces before writing files:

```go
type Store interface {
    NewBatch(epoch uint64) Batch
    Get(readEpoch uint64, key []byte) ([]byte, error)
    Scan(readEpoch uint64, start, end []byte) Iterator
    Seal(epoch uint64) error
    Checkpoint(epoch uint64) error
}
```

This lets the execution engine depend on storage behavior instead of storage internals.

### Stage 2: Local write path

Implement:

```text
Batch -> WAL append -> mutable memtable -> immutable memtable -> SSTable
```

Important details:

- each write carries an epoch;
- deletes are tombstones;
- writes are grouped by barrier epoch;
- checkpoint flushes immutable state;
- manifest update is atomic from the engine's perspective.

### Stage 3: Snapshot read path

Reads must return the newest version whose epoch is less than or equal to the read epoch.

Lookup order:

```text
mutable memtable
immutable memtables
newest SSTables
older SSTables
```

For scans, merge iterators across all layers and collapse versions by user key.

### Stage 4: SSTable format

Start with a simple format:

```text
block 1: sorted versioned key/value records
block 2: sorted versioned key/value records
...
footer: block offsets, min/max keys, min/max epochs
```

Use JSON only for a debugging prototype. Move to binary blocks once tests prove the behavior.

### Stage 5: Compaction

Compaction should be safe-epoch aware:

```text
for each user key:
  keep all versions > safe_epoch
  keep newest version <= safe_epoch
  drop older versions
  keep tombstone only if needed to hide an older value
```

This is the first compaction rule to implement. More advanced level picking can wait.

### Stage 6: Recovery

Recovery should prove that storage is usable for a streaming database:

```text
read manifest
load SSTable metadata
replay WAL after last checkpoint
reconstruct memtable
resume next epoch
```

Only after this works should we add performance optimizations.

## Stream-engine focus

### Minimal executor contract

```go
type Executor interface {
    Process(ctx context.Context, msg Message) ([]Message, error)
}
```

### Operators to build first

1. `SourceExecutor`: converts ingested rows into changelog messages.
2. `FilterExecutor`: drops messages that do not match a predicate.
3. `ProjectExecutor`: rewrites row shape.
4. `HashAggExecutor`: keeps per-group aggregate state in `streamstore`.
5. `MaterializeExecutor`: writes final rows into a materialized-view table.
6. `BarrierExecutor` or coordinator logic: propagates barriers and triggers storage checkpoint.

### Operators to defer

- top-N;
- over-window functions;
- dynamic filters;
- temporal joins;
- distributed exchange;
- vector search;
- sink connectors.

## Metadata/control-plane focus

RisingWave has a substantial meta service. For `space-node`, keep it small:

```go
type Catalog struct {
    Streams map[string]StreamDef
    Tables  map[string]TableDef
    Views   map[string]MaterializedViewDef
}

type Coordinator struct {
    Catalog *Catalog
    Epoch   uint64
    Store   storage.Store
}
```

Responsibilities:

- allocate epochs;
- register streams and materialized views;
- build executor graphs;
- inject barriers;
- record committed checkpoints;
- expose catalog state.

## Connector focus

RisingWave has a large connector surface. For our project, connectors should come after the engine works.

Build in this order:

1. in-memory/manual source for tests;
2. HTTP JSON source;
3. file JSONL source;
4. generated data source;
5. Kafka source;
6. Postgres CDC source;
7. sinks.

Do not let connector complexity delay storage and materialized-view correctness.

## Serving focus

Use HTTP first:

```text
POST /sql
POST /streams/{name}
GET  /views/{name}
POST /checkpoint
GET  /debug/catalog
GET  /debug/storage
```

PostgreSQL wire compatibility is useful, but it should be a later compatibility layer over a working engine.

## Implementation roadmap after this exploration

### PR 1: Core model

- `internal/types`: row, value, schema, changelog, barrier.
- Unit tests for encoding/decoding rows and changelog records.

### PR 2: In-memory stream engine

- Manual stream registration.
- Manual materialized-view pipeline.
- In-memory hash aggregation.
- Query current view state.

### PR 3: Storage interface

- `internal/storage` interface.
- In-memory MVCC store.
- Snapshot reads by epoch.
- Tests for put/delete/get/scan by epoch.

### PR 4: Barrier and checkpoint loop

- Coordinator allocates epochs.
- Barriers flow through executors.
- Store seals/checkpoints epochs.
- Tests for recovery boundary semantics, even if recovery is still in memory.

### PR 5: Local `streamstore`

- WAL.
- Memtable.
- Immutable memtable.
- SSTable writer/reader.
- Manifest.
- Reopen from manifest.

### PR 6: Compaction

- Safe epoch tracking.
- Compaction picker can be trivial.
- Version/tombstone cleanup tests.

### PR 7: SQL subset

- Parse `CREATE STREAM`.
- Parse `CREATE MATERIALIZED VIEW`.
- Plan filter/project/group-by aggregate.

### PR 8: HTTP API

- Create stream/view through `/sql`.
- Ingest JSON records.
- Query materialized views.
- Trigger checkpoints.

## What to ignore from RisingWave for now

Do not copy these early:

- distributed actors and fragment scheduling;
- complex meta-service state machines;
- object-store integration;
- advanced compaction groups;
- vector indexes;
- PostgreSQL wire protocol;
- full SQL optimizer;
- dozens of connectors;
- backup/restore;
- dashboards and metrics depth.

These are valuable later, but the educational database should first make one thing excellent: a readable path from event ingestion to incrementally maintained durable materialized view.

## The main focus for `space-node`

The project should focus on this vertical slice:

```text
HTTP/file event source
  -> changelog message
  -> executor graph
  -> stateful aggregate
  -> materialized view
  -> streamstore epoch write
  -> barrier checkpoint
  -> recovery
  -> query current view
```

If this slice is clear and well tested, the project will successfully teach the core ideas behind systems like RisingWave without inheriting their production complexity.
