# PR 2 Executor Pipeline Diagrams

This document explains the PR 2 executor pipeline visually. PR 2 replaces the earlier hard-coded materialized-view update path with a small RisingWave-inspired executor chain.

## 1. Big picture

```mermaid
flowchart LR
    Client[Caller inserts row] --> Engine[Engine.Insert]
    Engine --> Change[types.Change]
    Change --> Pipeline[executor.Pipeline]
    Pipeline --> Store[MemoryViewStore]
    Store --> Query[Engine.GetViewRow / ViewRows]
```

The engine still exposes a simple API, but the actual view maintenance now happens inside an executor pipeline.

## 2. PR 2 pipeline shape

```mermaid
flowchart LR
    Source[SourceExecutor\nkeeps matching stream] --> Filter[FilterExecutor\noptional predicate]
    Filter --> Project[ProjectExecutor\nkeeps group + value columns]
    Project --> HashAgg[HashAggExecutor\nupdates grouped SUM]
    HashAgg --> Materialize[MaterializeExecutor\nwrites final view row]
    Materialize --> ViewStore[(MemoryViewStore)]
```

For the first SUM materialized view, the pipeline is:

```text
Source -> Filter -> Project -> HashAggregate -> Materialize
```

## 3. Concrete `orders -> user_spend` flow

```mermaid
sequenceDiagram
    participant Test as Test / Caller
    participant Engine as Engine
    participant Source as SourceExecutor
    participant Filter as FilterExecutor
    participant Project as ProjectExecutor
    participant Agg as HashAggExecutor
    participant Mat as MaterializeExecutor
    participant Store as MemoryViewStore

    Test->>Engine: Insert(epoch=1, stream="orders", row={user_id:7, amount:20})
    Engine->>Source: Change{Source:"orders", Op:Insert}
    Source-->>Filter: keep matching stream
    Filter-->>Project: predicate passes
    Project-->>Agg: row={user_id:7, amount:20}
    Agg-->>Mat: row={user_id:7, total_amount:20}
    Mat->>Store: Put("7", aggregate row)

    Test->>Engine: Insert(epoch=2, stream="orders", row={user_id:7, amount:5.5})
    Engine->>Source: Change{Source:"orders", Op:Insert}
    Source-->>Filter: keep matching stream
    Filter-->>Project: predicate passes
    Project-->>Agg: row={user_id:7, amount:5.5}
    Agg-->>Mat: row={user_id:7, total_amount:25.5}
    Mat->>Store: Put("7", updated aggregate row)

    Test->>Engine: GetViewRow("user_spend", "7")
    Engine->>Store: Get("7")
    Store-->>Engine: {user_id:7, total_amount:25.5}
    Engine-->>Test: copied row
```

## 4. What each executor does

```mermaid
flowchart TB
    subgraph SourceExecutor
        S1[Input Change]
        S2{Source == stream?}
        S3[Forward]
        S4[Drop]
        S1 --> S2
        S2 -- yes --> S3
        S2 -- no --> S4
    end

    subgraph FilterExecutor
        F1[Input Change]
        F2{Predicate nil or true?}
        F3[Forward]
        F4[Drop]
        F1 --> F2
        F2 -- yes --> F3
        F2 -- no --> F4
    end

    subgraph ProjectExecutor
        P1[Input Row]
        P2[Copy selected columns]
        P3[Forward projected row]
        P1 --> P2 --> P3
    end

    subgraph HashAggExecutor
        A1[Input projected row]
        A2[Read group key]
        A3[Add amount to group sum]
        A4[Emit aggregate row]
        A1 --> A2 --> A3 --> A4
    end

    subgraph MaterializeExecutor
        M1[Input aggregate row]
        M2[Derive materialized key]
        M3[Put row in MemoryViewStore]
        M1 --> M2 --> M3
    end
```

## 5. Data shape at each stage

Given this input:

```go
types.Row{
    "user_id": 7,
    "amount": 20.0,
    "ignored": "debug-only",
}
```

The row changes like this:

```mermaid
flowchart LR
    In["Input row\nuser_id=7\namount=20\nignored=debug-only"]
    Source["After Source\nsame row"]
    Filter["After Filter\nsame row if predicate passes"]
    Project["After Project\nuser_id=7\namount=20"]
    Agg["After HashAgg\nuser_id=7\ntotal_amount=20"]
    Store["Stored row\nkey=7\nuser_id=7\ntotal_amount=20"]

    In --> Source --> Filter --> Project --> Agg --> Store
```

On the next row for `user_id=7` with `amount=5.5`, `HashAggExecutor` emits:

```text
user_id=7, total_amount=25.5
```

`MaterializeExecutor` overwrites the stored row for key `7` with the latest aggregate value.

## 6. How filtering works

```mermaid
flowchart LR
    Row1[amount=20] --> Pred1{amount > 0?}
    Pred1 -- yes --> Agg1[contributes to SUM]

    Row2[amount=-7] --> Pred2{amount > 0?}
    Pred2 -- no --> Drop[does not reach aggregate]
```

This is covered by `TestSumViewFilterUsesExecutorPipeline`.

## 7. How source gating works

```mermaid
flowchart LR
    Orders[Change Source=orders] --> Gate{SourceExecutor stream=orders}
    Gate -- match --> Keep[forward]

    Other[Change Source=other] --> Gate2{SourceExecutor stream=orders}
    Gate2 -- mismatch --> Drop[drop]
```

This lets future engines route many streams through many view pipelines safely.

## 8. Current limitations

PR 2 intentionally keeps the executor model small:

- only insert changes are supported by `HashAggExecutor`;
- there is no `Message` wrapper yet;
- barriers are not propagated yet;
- there is no checkpointing yet;
- aggregation state is in-memory only;
- the pipeline is built manually by `CreateSumView`, not by SQL planning.

These limitations line up with PR 3, where the next step is to introduce messages, barriers, and checkpoint-oriented epoch flow.
