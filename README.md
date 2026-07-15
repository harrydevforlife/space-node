# space-node

`space-node` is planned as an educational streaming database in Go inspired by RisingWave-style concepts: streams, tables, incremental materialized views, stateful operators, checkpointing, and a streaming-oriented storage layer.

Start with the implementation plan in [`docs/streaming-db-plan.md`](docs/streaming-db-plan.md). The plan intentionally keeps the first version small and educational before introducing distributed execution or production-grade storage details.


The RisingWave exploration focus map is in [`docs/risingwave-focus-map.md`](docs/risingwave-focus-map.md).

Next steps are tracked in [`docs/next-steps.md`](docs/next-steps.md).

## Running and testing

See [`docs/run-and-test.md`](docs/run-and-test.md) for the current commands to format, test, and exercise the in-memory PR 1 library flow.
