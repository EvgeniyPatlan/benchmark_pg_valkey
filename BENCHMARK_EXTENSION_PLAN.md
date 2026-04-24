# Benchmark Extension: Kafka + RabbitMQ

Adds two more single-node queue systems to the existing PG-vs-Valkey comparison, on two new VMs (VM3, VM4). Preserves the reviewer-driven methodology (service/E2E latency split, matched durability, bootstrap CIs, symmetric pgbench load) so all four backends sit in one unified results table.

---

## 1. Scope and fairness

**Added backends:**

| VM  | System     | Variants tested                           | Role in comparison |
|-----|------------|-------------------------------------------|--------------------|
| VM3 | Kafka      | `standard` (8 partitions, consumer group) | Log-based, pull-model, offset-commit |
| VM4 | RabbitMQ   | `classic`, `quorum`                       | Traditional broker + Raft-backed durable queue |

**Fairness stance (extends existing single-node principle):**

- **Single broker / single node per backend.** Kafka cluster and RabbitMQ cluster are different comparisons; stated explicitly in the README.
- **Partitioning to match Valkey.** Kafka topic uses 8 partitions (same as Valkey's 8 stream keys) so "best single-node config" is roughly matched.
- **Prefetch/batching tuned per backend.** Kafka `max.poll.records=50`, RabbitMQ `prefetch_count=50` — matching Valkey's `batch_size=50` so the reviewer's batched-vs-unbatched asymmetry concern (#3) doesn't re-emerge per backend.
- **Explicit caveat for quorum on single node.** Quorum queues need ≥3 nodes to actually provide their durability guarantee. On a single node they still *work* and their throughput/latency profile is informative, but we document in environment.txt and the paper that the durability result is degenerate.

---

## 2. Key design decisions (to confirm before implementation)

### 2.1 Kafka: KRaft mode, not ZooKeeper

KRaft is the modern default, removes the ZK dependency, is simpler to set up on a single node, and is what a team deploying Kafka in 2026 would actually use. Still a single broker.

### 2.2 RabbitMQ: classic + quorum variants

Two variants mirror the PG "multiple variants so comparison is fair against the best" principle:

- **classic** — traditional mnesia-backed queue (the historical default, what most older RabbitMQ deployments run).
- **quorum** — Raft-backed durable queue (the 2024+ recommended default for durability). Single-node caveat documented.

**Dropped:** RabbitMQ Streams and Kafka Streams don't add a new datapoint — they behave like Valkey Streams / Kafka with per-message ack. Including them bloats the matrix without new signal.

### 2.3 Durability mode extension

Extend the existing `DURABILITY_MODE={none|matched|strict}` convention:

| Mode     | PostgreSQL              | Valkey                 | Kafka                                | RabbitMQ classic            | RabbitMQ quorum       |
|----------|-------------------------|------------------------|--------------------------------------|-----------------------------|-----------------------|
| `none`   | `synchronous_commit=on` | `appendonly no`        | `acks=1`, `flush.ms=default`         | transient, no confirms      | (forced durable)      |
| `matched`| `synchronous_commit=off`| `appendfsync everysec` | `acks=all`, `flush.ms=1000`          | durable + publisher confirms| durable + confirms    |
| `strict` | `synchronous_commit=on` | `appendfsync always`   | `acks=all`, `flush.messages=1`       | durable + tx-based publish  | durable + tx-publish  |

For single-broker Kafka, `acks=all` with `min.insync.replicas=1` is the best single-node approximation of full durability.

### 2.4 Producer stays unified (`producer.py`)

Extend `--backend` with `kafka` and `rabbitmq`. Same rate-cap logic, same round-robin across partitions for Kafka. Each backend gets its own `produce_*_batch` method; the `run()` rate-limiting loop is shared.

Rationale: one producer keeps the arrival-side logic (rate, batch sizing, auto-cap, timestamp embedding) identical across backends, which is crucial for valid latency comparison.

### 2.5 Workers: new files, shared metrics contract

`worker_kafka.py` and `worker_rabbitmq.py` mirror the service-vs-E2E `MetricsCollector` from the Valkey/PG workers. Emit JSONL with the same schema — `analyze.py` needs no per-backend parsing logic.

### 2.6 Load scenario: keep pgbench on every VM

VM3 and VM4 install a local PostgreSQL just for pgbench, same as VM2 does today. Rationale: the "load" scenario measures how each queue system behaves when a noisy neighbor is competing for host resources (CPU, disk, memory bandwidth). Keeping pgbench as the load generator makes the cross-VM comparison symmetric.

Adds ~10 min of install time to VM3/VM4 scripts; cheap.

### 2.7 Capacity file extended

`results/capacity.json` gains Kafka + RabbitMQ keys:

```json
{
  "pg:skip_locked": 448.0,
  "pg:skip_locked_batch": 950.0,
  "pg:delete_returning": 520.0,
  "pg:partitioned": 693.0,
  "valkey:streams": 2100.0,
  "kafka:standard": null,
  "rabbitmq:classic": null,
  "rabbitmq:quorum": null
}
```

Values populated after an uncapped shakedown run per backend (same workflow as existing two).

---

## 3. File and directory layout

Additions only — existing files unchanged except where listed in §4.

```
benchmark_pg_valkey/            # Consider rename to benchmark_queues/ in a follow-up PR
├── benchmark/
│   ├── worker_kafka.py                 [NEW]
│   ├── worker_rabbitmq.py              [NEW]
│   ├── validate_kafka_latency.py       [NEW]  # mirrors validate_valkey_latency.py
│   ├── validate_rabbitmq_latency.py    [NEW]
│   ├── producer.py                     [EDIT] # add kafka + rabbitmq backends
│   ├── config.py                       [EDIT] # add KAFKA_CONFIG, RABBITMQ_CONFIG
│   └── test_durability.py              [EDIT] # extend crash test to cover all 4
├── setup/
│   ├── install_kafka.sh                [NEW]  # KRaft, single-broker, 8 partitions default
│   ├── install_rabbitmq.sh             [NEW]  # enable management, quorum plugin
│   └── requirements.txt                [EDIT] # + confluent-kafka, + pika
├── schema/
│   ├── init_kafka.sh                   [NEW]  # kafka-topics.sh create with partitions
│   └── init_rabbitmq.sh                [NEW]  # rabbitmqadmin declare classic + quorum
├── orchestration/
│   ├── run_kafka_tests.sh              [NEW]
│   └── run_rabbitmq_tests.sh           [NEW]
├── run_vm3_kafka.sh                    [NEW]  # self-contained VM bootstrap
├── run_vm4_rabbitmq.sh                 [NEW]
├── analysis/
│   ├── analyze.py                      [EDIT] # add 2 results dirs, 3 new variants
│   └── generate_graphs.py              [EDIT] # colors, best-per-backend, updated decision guide
└── README.md                           [EDIT] # update fairness framing, decision guide, VM count
```

---

## 4. Results directory convention (preserved)

Filename pattern `{queue_type}_{scenario}_run{N}_metrics.jsonl` stays stable. Per-backend `queue_type` values:

| Backend  | queue_type values                                          |
|----------|------------------------------------------------------------|
| PG       | `skip_locked`, `skip_locked_batch`, `delete_returning`, `partitioned` |
| Valkey   | `streams`                                                  |
| Kafka    | `standard`                                                 |
| RabbitMQ | `classic`, `quorum`                                        |

Top-level:
```
results/
├── vm1_pg/
├── vm2_valkey/
├── vm3_kafka/           [NEW]
├── vm4_rabbitmq/        [NEW]
├── capacity.json        [EXTENDED]
├── analysis/
└── graphs/
```

---

## 5. Implementation phases

Each phase is independently mergeable. Phases 2 and 3 can run in parallel (Kafka and RabbitMQ are independent).

### Phase 1 — Foundational scaffolding (no runtime yet)

1. `config.py`: add `KAFKA_CONFIG`, `RABBITMQ_CONFIG`, extend `QUEUE_TYPES` registry (or add parallel `BROKER_TOPICS` / `BROKER_QUEUES` dicts — the current `QUEUE_TYPES` is PG-specific). Decide: keep PG-only registry and add per-backend registries, or generalize. **Recommendation: per-backend registries** to avoid forcing Kafka/RabbitMQ through PG-shaped config.
2. `producer.py`: add `--backend kafka|rabbitmq` choices; stub `produce_kafka_batch` / `produce_rabbitmq_batch` that raise NotImplementedError so the CLI parses correctly before the workers land.
3. `results/capacity.json`: add the new keys with `null` sentinel values.

**Exit criterion:** `python3 benchmark/producer.py --backend kafka --help` works.

### Phase 2 — Kafka path

1. `setup/install_kafka.sh`: Kafka 3.9 (latest stable as of 2026), KRaft mode, single broker, `num.partitions=8`, systemd unit.
2. `schema/init_kafka.sh`: `kafka-topics.sh --create --topic bench_queue --partitions 8 --replication-factor 1`.
3. `benchmark/worker_kafka.py`: `confluent-kafka-python` consumer in a consumer group, `enable.auto.commit=false`, manual `commit()` after each batch's simulated processing. Service/E2E latency via same `MetricsCollector` pattern.
4. `benchmark/producer.py`: `produce_kafka_batch` using confluent-kafka producer with `acks` controlled by `DURABILITY_MODE`. Embeds `created_at` in message headers (matches Valkey's convention).
5. `benchmark/validate_kafka_latency.py`: end-to-end baseline latency on an empty topic, mirrors `validate_valkey_latency.py`.
6. `orchestration/run_kafka_tests.sh`: mirrors `run_valkey_tests.sh` structure — scenarios, NUM_RUNS, durability mode apply, environment.txt logging, pgbench load.
7. `run_vm3_kafka.sh`: self-contained bootstrap (install Java 17, Kafka, Python deps, local PG for pgbench, run benchmark).

**Exit criterion:** `./orchestration/run_kafka_tests.sh 1` produces `results/vm3_kafka/standard_cold_run1_metrics.jsonl` with both `latency_*` and `service_*` columns populated.

### Phase 3 — RabbitMQ path (parallel with Phase 2)

1. `setup/install_rabbitmq.sh`: RabbitMQ 3.13, `rabbitmq_management` plugin, Erlang from Ubuntu-maintained repo.
2. `schema/init_rabbitmq.sh`: declare `bench_queue_classic` (classic, durable) and `bench_queue_quorum` (quorum type).
3. `benchmark/worker_rabbitmq.py`: pika consumer with `basic_qos(prefetch_count=50)`, manual `basic_ack`. One worker per queue variant (not both simultaneously).
4. `benchmark/producer.py`: `produce_rabbitmq_batch` using pika `BlockingConnection` + publisher confirms when `DURABILITY_MODE != 'none'`.
5. `benchmark/validate_rabbitmq_latency.py`: baseline latency on empty queue.
6. `orchestration/run_rabbitmq_tests.sh`: loops over `("classic", "quorum")` × `("cold", "warm", "load")`.
7. `run_vm4_rabbitmq.sh`: self-contained bootstrap.

**Exit criterion:** `./orchestration/run_rabbitmq_tests.sh 1` produces both `classic_*_metrics.jsonl` and `quorum_*_metrics.jsonl`.

### Phase 4 — Analysis extension

1. `analyze.py`:
   - Add `--kafka-results` and `--rabbitmq-results` CLI args.
   - Extend `analyze_all()` to iterate these dirs with variant lists `['standard']` and `['classic', 'quorum']` respectively.
   - Normalize `backend` column values to `{postgresql, valkey, kafka, rabbitmq}` for grouping.
   - `run_significance_tests()` automatically picks up new groups (it iterates `groupby`); no changes needed.

2. Sanity-check that bootstrap CIs, Bonferroni-Holm, identical-runs detection still work with a 4-backend result set.

**Exit criterion:** `analyze.py --pg-results … --valkey-results … --kafka-results … --rabbitmq-results …` prints a summary table with 8 rows per scenario (4 PG variants + Valkey + Kafka + 2 RabbitMQ variants).

### Phase 5 — Graph extension

1. `generate_graphs.py`:
   - Add `COLOR_KAFKA = '#e74c3c'`, `COLOR_RABBITMQ = '#f39c12'`.
   - Update `_get_colors` to dispatch on all four backends.
   - `plot_valkey_vs_best_pg` → rename conceptually to `plot_best_per_backend` (backward-compat: keep filename `valkey_vs_best_pg.png` OR rename to `best_per_backend.png` — **recommend rename** to reflect the new reality).
   - `plot_decision_guide`: update from 2-column (PG/Valkey) to 4-column table. This is the biggest visual change.
   - All iterating plots (throughput, latency, CPU, service_vs_e2e, scenario_comparison) automatically pick up the new backends/variants since they iterate the dataframe.

2. Legend layout: 4 backends × up-to-4 variants = 8 series. Consider moving legends outside the plot area for the scenario_comparison figure.

**Exit criterion:** All 8 PNGs regenerate cleanly with 8 series visible.

### Phase 6 — Self-contained VM scripts + verify_setup.sh

1. `run_vm3_kafka.sh` and `run_vm4_rabbitmq.sh` as complete one-shot bootstraps matching the `run_vm1_pg.sh` / `run_vm2_valkey.sh` pattern.
2. `verify_setup.sh`: extend pre-flight checks to handle all four backends (detect which one is expected from hostname, `$BACKEND` env var, or a flag).

**Exit criterion:** Fresh Ubuntu 24.04 VM + `./run_vm3_kafka.sh 1` completes end-to-end.

### Phase 7 — Docs

1. `CLAUDE.md`: update architecture section (4 VMs, broker registry convention, per-backend durability table).
2. `README.md`:
   - Update fairness framing: "1 node per system, all tuned to best single-node config".
   - Expand decision guide from 2 columns to 4.
   - Update test matrix (10 total variants × 3 scenarios × N runs).
   - Update expected test duration.
3. `BENCHMARK_EXTENSION_PLAN.md` (this file): once implemented, move to `docs/history/` or delete.

---

## 6. Test matrix impact

Pre-extension: 15 scenarios (9 PG + 3 Valkey + 3 durability mode combos if matched added).
Post-extension (5 runs per scenario, all three durability modes): roughly ~150 runs.

**Practical recommendation:** don't run all durability modes × all variants × all scenarios. Suggest:

- Full matrix at `DURABILITY_MODE=none` (legacy baseline): 30 runs × 5 = 150.
- Just the Load scenario at `DURABILITY_MODE=matched` for the headline comparison: 8 variants × 5 runs = 40.
- Skip `strict` for the main report; include it as appendix if time permits.

---

## 7. Risks and open decisions

1. **Should `QUEUE_TYPES` be generalized?** Currently PG-specific (contains SQL function names). Kafka and RabbitMQ don't need those fields. Recommend separate `BROKER_TOPICS` / `BROKER_QUEUES` dicts to avoid awkward schema overloading. **Decision needed before Phase 1.**

2. **Kafka headers for timestamp?** `created_at` embedded in message headers (not body) keeps the payload size metric apples-to-apples across backends. But pika/RabbitMQ convention is header properties. Both support it; just need to be consistent.

3. **RabbitMQ memory alarm on single node.** Default is 40% of RAM; a large backlog during the `load` scenario could trigger producer throttling. Either (a) raise the threshold for the benchmark run, or (b) accept that RabbitMQ will throttle under overdrive — which is actually *the right behavior* and a point in its favor. **Recommend (b)** + document.

4. **Is Kafka worth it at this volume?** At 1000 j/s our producer rate is well below Kafka's sweet spot (10k+ msg/s). Results may show Kafka as "just fast" without revealing its scaling properties. That's fine — matches the "1 PG node" fairness — but note it in the paper.

5. **Quorum on single node durability caveat.** Re-stated here because it'll bite on review. Explicit note in environment.txt per run + one sentence in the paper.

6. **Rename `benchmark_pg_valkey/`?** The directory name is stale once Kafka and RabbitMQ land. Low priority; do in a follow-up PR to keep this one focused on functionality.

---

## 8. Estimated effort

| Phase | Effort | Parallelizable? |
|-------|--------|-----------------|
| 1     | ~2h    | No (blocks 2, 3) |
| 2 (Kafka)    | ~6–8h  | Yes (with 3) |
| 3 (RabbitMQ) | ~5–7h  | Yes (with 2) |
| 4 (Analysis) | ~2h    | No (after 2, 3) |
| 5 (Graphs)   | ~3h    | No (after 4) |
| 6 (VM scripts) | ~3h | Yes (with 5) |
| 7 (Docs)     | ~2h    | No (last) |

Total: ~23–27h implementation + validation VM time.

Not counting actual benchmark-run wall time — same ~10–12h per VM as today, so another ~20–24h of VM time for the full 5-run matrix across VM3 + VM4.

---

## 9. Decision points I need from you before Phase 1

1. **Confirm Kafka variant scope**: one (`standard`, 8 partitions) or two (add `single_partition` baseline)?
2. **Confirm RabbitMQ variant scope**: `classic` + `quorum`, or add `streams`?
3. **Config schema**: separate `BROKER_*` dicts (recommended) or extend `QUEUE_TYPES` with broker fields?
4. **Durability matrix**: full `{none, matched, strict}` × all variants, or just `none` full + `matched` for Load only (my recommendation)?
5. **Graph rename**: `valkey_vs_best_pg.png` → `best_per_backend.png`, or keep existing filename for backward-compat?
6. **Directory rename**: `benchmark_pg_valkey/` → `benchmark_queues/` in this PR or a follow-up?

Once decided, Phase 1 is ~2h of work; Phases 2 and 3 can then run in parallel.
