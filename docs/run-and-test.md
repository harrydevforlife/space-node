# Run and Test Guide

This guide explains how to work with the current PR 1 implementation. The project is a Go library right now; there is no CLI, HTTP server, SQL parser, or persistent storage process to run yet.

## Requirements

- Go 1.22 or newer.
- A shell from the repository root.

Check your Go version:

```bash
go version
```

## Install or refresh dependencies

The current module has no third-party dependencies. You can still ask Go to verify module metadata:

```bash
go mod tidy
```

If `go mod tidy` changes `go.mod` or creates `go.sum`, review those changes before committing them.

## Run all tests

From the repository root:

```bash
go test ./...
```

This runs every package test. At the current stage, the main coverage is in `internal/engine`.

## Run only the engine tests

```bash
go test ./internal/engine
```

Use verbose output when you want to see individual test names:

```bash
go test -v ./internal/engine
```

## Run a single test

```bash
go test -v ./internal/engine -run TestInMemorySumViewUpdatesIncrementally
```

The test creates an `orders` stream, creates a manual `user_spend` SUM materialized view, inserts two rows for the same user, and verifies that the aggregate updates incrementally.

## Format code

Before committing code changes, run:

```bash
gofmt -w internal
```

If later work adds Go files outside `internal`, format those paths too.

## Quick local verification checklist

Before opening a PR, run:

```bash
gofmt -w internal
go test ./...
git status --short
```

Expected result:

- `gofmt` produces no unwanted diffs;
- `go test ./...` passes;
- `git status --short` only shows intentional changes.

## What can be exercised today

The current implementation supports only the PR 1 library flow:

1. create an in-memory engine;
2. register a stream schema;
3. register a manual SUM materialized view;
4. insert rows into the stream;
5. read materialized view rows.

A minimal example looks like this:

```go
db := engine.New()

_ = db.CreateStream("orders", types.Schema{Columns: []types.Column{
    {Name: "user_id", Type: types.IntType},
    {Name: "amount", Type: types.FloatType},
}})

_ = db.CreateSumView(engine.SumViewSpec{
    Name:     "user_spend",
    Source:   "orders",
    GroupKey: "user_id",
    SumField: "amount",
    SumAlias: "total_amount",
})

_ = db.Insert(1, "orders", types.Row{"user_id": 7, "amount": 20.0})
_ = db.Insert(2, "orders", types.Row{"user_id": 7, "amount": 5.5})

row, ok := db.GetViewRow("user_spend", "7")
// ok == true
// row["total_amount"] == 25.5
```

## What is not runnable yet

These features are intentionally not available yet:

- command-line binary;
- HTTP API;
- SQL parser;
- executor pipeline abstraction;
- barrier propagation;
- checkpointing;
- disk-backed `streamstore`;
- connectors.

The next implementation step is PR 2 from `docs/next-steps.md`: replace the hard-coded materialized view path with an executor pipeline.
