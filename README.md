# dag-bench


## Benchmarks

All of the benchmarks are located in the `projects/` dir. Each directory contains a dbt project that either serves a particular benchmarking purpose or is derived
from a particular source. Currently we have:

1. **gym**: A hand craft DAG that uses the TPC-H schema. This was made specifically to test a couple of optimizations we had in mind.
2. **synth**: A set of 10 synthetic DAGs created by Claude Sonnet 4.6, where each DAG has only 1 sink.
3. **synth-multi**: A set of 10 synthetic DAGs created by Claude Sonnet 4.6, where each DAG has multiple sinks. Each of these 10 DAGs has a corresponding DAG in **synth** that mirrors
its business domain.
4. **tpcdi**: A dbt representation of the transformations in the TPC-DI benchmark. This is not designed to be entirely faithful to the original spec, and removes the "extract" component of the benchmark.
5. **tpch**: A dbt DAG that contains each of the 22 TPC-H queries in a completely flat graph.
6. **tpcds**: A dbt DAG that contains each of the 99 TPC-DS queries in a completely flat graph.

## Attribution

Queries in `tpch_dag/models/zhou07/` are from:

```
Jingren Zhou, Per-Ake Larson, Johann-Christoph Freytag, and Wolfgang Lehner. 2007. Efficient exploitation of similar subexpressions for query processing. In Proceedings of the 2007 ACM SIGMOD international conference on Management of data (SIGMOD '07). Association for Computing Machinery, New York, NY, USA, 533–544. https://doi.org/10.1145/1247480.1247540
```
