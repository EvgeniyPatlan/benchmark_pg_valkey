#!/usr/bin/env python3
"""
Analyze benchmark results.

Supports multi-run aggregation with mean +/- stddev, 95% Gaussian CIs for
throughput, and bootstrap 95% CIs for latency percentiles (which aren't
well-approximated by normal CIs at small n).

Percentile methodology (reviewer smaller note):
  Per-run percentiles are computed inside each worker process from the raw
  latency sample of that run. Cross-run aggregates are the MEAN OF PER-RUN
  percentiles (arithmetic mean of p95 across runs). This is what the
  'latency_p95_ms_mean' column reports.

  This is slightly conservative on tail variability vs pooling raw
  latencies across runs. Use --percentile-method pooled to re-aggregate
  using pooled-sample percentiles instead (requires raw latency files).

Multiple comparisons (reviewer smaller note):
  Pairwise Welch's t-tests on throughput are Bonferroni-Holm corrected in
  run_significance_tests(). With n=5 and large effect sizes the correction
  is decorative (everything significant stays significant), but reviewers
  will still ding you for not applying it.

Shapiro-Wilk at n=5 has very limited power. We report the statistic and
p-value but phrase non-rejection as 'insufficient evidence of non-
normality' rather than 'data is normal'.
"""
import argparse
import json
import math
import os
import re
from pathlib import Path
import pandas as pd
import numpy as np


class BenchmarkAnalyzer:
    def __init__(self, pg_results_dir=None, valkey_results_dir=None,
                 kafka_results_dir=None, rabbitmq_results_dir=None):
        self.pg_results_dir = pg_results_dir
        self.valkey_results_dir = valkey_results_dir
        self.kafka_results_dir = kafka_results_dir
        self.rabbitmq_results_dir = rabbitmq_results_dir
        self.results = pd.DataFrame()
        self.aggregated = pd.DataFrame()

    def load_metrics_file(self, filepath):
        """Load JSONL metrics file"""
        metrics = []
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        metrics.append(json.loads(line))
            return pd.DataFrame(metrics)
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
            return pd.DataFrame()

    def _extract_run_number(self, filename):
        """Extract run number from filename like 'skip_locked_cold_run3_metrics.jsonl'"""
        match = re.search(r'_run(\d+)_', filename)
        if match:
            return int(match.group(1))
        return 1  # legacy files without run number

    def _find_metrics_files(self, base_dir, pattern):
        """Find all metrics files matching a pattern, including multi-run"""
        base_path = Path(base_dir)
        files = []
        for f in base_path.glob(pattern):
            files.append(str(f))
        # Also look for legacy files without _runN suffix
        return sorted(files)

    def analyze_single_metrics(self, metrics_file, system_file=None):
        """Analyze a single metrics file and return stats dict"""
        df = self.load_metrics_file(metrics_file)
        if df.empty:
            return None

        last = df.iloc[-1] if len(df) > 0 else {}

        def _last(col, default=0):
            return last[col] if col in df.columns and len(df) > 0 else default

        # latency_* columns are end-to-end (kept name for backward compat).
        # service_* columns (added in worker update for reviewer concern #1)
        # may be absent in legacy files — fall back to NaN so the caller
        # can see which runs pre-date the service/e2e split.
        stats = {
            'total_jobs': _last('jobs_processed'),
            'duration_sec': _last('elapsed'),
            'avg_throughput': df['throughput'].mean() if 'throughput' in df.columns else 0,
            'max_throughput': df['throughput'].max() if 'throughput' in df.columns else 0,
            'min_throughput': df['throughput'].min() if 'throughput' in df.columns else 0,
            # End-to-end (enqueue -> ack)
            'latency_p50_ms': _last('latency_p50'),
            'latency_p95_ms': _last('latency_p95'),
            'latency_p99_ms': _last('latency_p99'),
            'latency_avg_ms': _last('latency_avg'),
            'latency_max_ms': _last('latency_max'),
            # Service (dequeue -> ack). NaN for legacy runs.
            'service_p50_ms': _last('service_p50', float('nan')) if 'service_p50' in df.columns else float('nan'),
            'service_p95_ms': _last('service_p95', float('nan')) if 'service_p95' in df.columns else float('nan'),
            'service_p99_ms': _last('service_p99', float('nan')) if 'service_p99' in df.columns else float('nan'),
            'service_avg_ms': _last('service_avg', float('nan')) if 'service_avg' in df.columns else float('nan'),
            # Broker overhead per message. NaN for pre-broker-metric runs.
            'broker_p50_ms': _last('broker_p50', float('nan')) if 'broker_p50' in df.columns else float('nan'),
            'broker_p95_ms': _last('broker_p95', float('nan')) if 'broker_p95' in df.columns else float('nan'),
            'broker_p99_ms': _last('broker_p99', float('nan')) if 'broker_p99' in df.columns else float('nan'),
            'broker_avg_ms': _last('broker_avg', float('nan')) if 'broker_avg' in df.columns else float('nan'),
        }

        # Load system metrics if available
        if system_file and os.path.exists(system_file):
            try:
                sys_df = pd.read_csv(system_file)
                stats['avg_cpu_user'] = sys_df['cpu_user'].mean()
                stats['avg_cpu_system'] = sys_df['cpu_system'].mean()
                stats['avg_mem_used_pct'] = sys_df['mem_used_pct'].mean()
            except Exception:
                pass

        return stats

    def analyze_pg_queue(self, queue_type, scenario):
        """Analyze PostgreSQL queue results (all runs)"""
        base_path = Path(self.pg_results_dir)
        all_runs = []

        # Find multi-run files
        for run_num in range(1, 100):
            metrics_file = base_path / f"{queue_type}_{scenario}_run{run_num}_metrics.jsonl"
            system_file = base_path / f"{queue_type}_{scenario}_run{run_num}_system.csv"

            if not metrics_file.exists():
                break

            stats = self.analyze_single_metrics(str(metrics_file), str(system_file))
            if stats:
                stats['run'] = run_num
                all_runs.append(stats)

        # Also check legacy format (no run number)
        if not all_runs:
            legacy_metrics = base_path / f"{queue_type}_{scenario}_metrics.jsonl"
            legacy_system = base_path / f"{queue_type}_{scenario}_system.csv"
            if legacy_metrics.exists():
                stats = self.analyze_single_metrics(str(legacy_metrics), str(legacy_system))
                if stats:
                    stats['run'] = 1
                    all_runs.append(stats)

        if not all_runs:
            return []

        # Add metadata to each run
        for run in all_runs:
            run['queue_type'] = queue_type
            run['scenario'] = scenario
            run['backend'] = 'postgresql'

        return all_runs

    def analyze_valkey(self, scenario):
        """Analyze Valkey Streams results (all runs)"""
        base_path = Path(self.valkey_results_dir)
        all_runs = []

        # Find multi-run files
        for run_num in range(1, 100):
            metrics_file = base_path / f"valkey_{scenario}_run{run_num}_metrics.jsonl"
            system_file = base_path / f"valkey_{scenario}_run{run_num}_system.csv"

            if not metrics_file.exists():
                break

            stats = self.analyze_single_metrics(str(metrics_file), str(system_file))
            if stats:
                stats['run'] = run_num
                all_runs.append(stats)

        # Also check legacy format
        if not all_runs:
            legacy_metrics = base_path / f"valkey_{scenario}_metrics.jsonl"
            legacy_system = base_path / f"valkey_{scenario}_system.csv"
            if legacy_metrics.exists():
                stats = self.analyze_single_metrics(str(legacy_metrics), str(legacy_system))
                if stats:
                    stats['run'] = 1
                    all_runs.append(stats)

        if not all_runs:
            return []

        for run in all_runs:
            run['queue_type'] = 'streams'
            run['scenario'] = scenario
            run['backend'] = 'valkey'

        return all_runs

    def analyze_broker(self, backend, results_dir, queue_types, scenario_prefix=None):
        """Generic loader for brokers whose result files follow the pattern
        `{queue_type}_{scenario}_run{N}_metrics.jsonl`.

        backend       : 'kafka' or 'rabbitmq' (sets the output column).
        queue_types   : list of variant names (for Kafka: ['standard'];
                        for RabbitMQ: ['classic', 'quorum']).
        """
        if not results_dir:
            return []

        base_path = Path(results_dir)
        all_runs = []
        for qt in queue_types:
            for scenario in ['cold', 'warm', 'load']:
                for run_num in range(1, 100):
                    metrics_file = base_path / f"{qt}_{scenario}_run{run_num}_metrics.jsonl"
                    system_file = base_path / f"{qt}_{scenario}_run{run_num}_system.csv"
                    if not metrics_file.exists():
                        break
                    stats = self.analyze_single_metrics(str(metrics_file), str(system_file))
                    if stats:
                        stats['run'] = run_num
                        stats['queue_type'] = qt
                        stats['scenario'] = scenario
                        stats['backend'] = backend
                        all_runs.append(stats)
        return all_runs

    def analyze_all(self):
        """Analyze all results"""
        all_results = []

        # PostgreSQL queues
        if self.pg_results_dir:
            queue_types = ['skip_locked', 'skip_locked_batch',
                           'delete_returning', 'partitioned']
            scenarios = ['cold', 'warm', 'load']

            for queue_type in queue_types:
                for scenario in scenarios:
                    runs = self.analyze_pg_queue(queue_type, scenario)
                    all_results.extend(runs)

        # Valkey
        if self.valkey_results_dir:
            scenarios = ['cold', 'warm', 'load']

            for scenario in scenarios:
                runs = self.analyze_valkey(scenario)
                all_results.extend(runs)

        # Kafka (single variant 'standard')
        all_results.extend(self.analyze_broker(
            'kafka', self.kafka_results_dir, ['standard']))

        # RabbitMQ (classic + quorum)
        all_results.extend(self.analyze_broker(
            'rabbitmq', self.rabbitmq_results_dir, ['classic', 'quorum']))

        if all_results:
            self.results = pd.DataFrame(all_results)
            self._aggregate_runs()
            return self.results
        else:
            print("No results found!")
            return pd.DataFrame()

    @staticmethod
    def _bootstrap_mean_ci(values, n_iter=2000, ci=0.95, seed=0):
        """Return (lo, hi) bootstrap CI for the mean of the per-run p95/p99.

        We bootstrap the mean of per-run percentiles rather than trying to
        pool raw samples we don't have. This gives honest uncertainty on
        the aggregate without pretending per-run p95 estimates are
        identically distributed.
        """
        values = np.asarray([v for v in values if not math.isnan(v)],
                            dtype=float)
        if len(values) < 2:
            return (float('nan'), float('nan'))
        rng = np.random.default_rng(seed)
        means = rng.choice(values, size=(n_iter, len(values)),
                           replace=True).mean(axis=1)
        alpha = (1 - ci) / 2
        lo = float(np.quantile(means, alpha))
        hi = float(np.quantile(means, 1 - alpha))
        return (lo, hi)

    def _aggregate_runs(self):
        """Aggregate multi-run results.

        For throughput: mean, stddev, 95% Gaussian CI (symmetric about mean).
        For latency percentiles: mean, stddev, 95% bootstrap CI (asymmetric,
        better at small n). Reviewer smaller note.
        """
        if self.results.empty:
            return

        group_cols = ['backend', 'queue_type', 'scenario']
        metric_cols = [
            'avg_throughput',
            'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms',
            'latency_avg_ms', 'latency_max_ms',
            'service_p50_ms', 'service_p95_ms', 'service_p99_ms',
            'service_avg_ms',
            'broker_p50_ms', 'broker_p95_ms', 'broker_p99_ms',
            'broker_avg_ms',
            'total_jobs',
        ]

        if 'avg_cpu_user' in self.results.columns:
            metric_cols.extend(['avg_cpu_user', 'avg_cpu_system', 'avg_mem_used_pct'])

        percentile_cols = {
            'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms',
            'service_p50_ms', 'service_p95_ms', 'service_p99_ms',
            'broker_p50_ms', 'broker_p95_ms', 'broker_p99_ms',
        }

        agg_rows = []
        for name, group in self.results.groupby(group_cols):
            row = {
                'backend': name[0],
                'queue_type': name[1],
                'scenario': name[2],
                'num_runs': len(group),
            }

            for col in metric_cols:
                if col not in group.columns:
                    continue
                values = group[col].dropna()
                if len(values) == 0:
                    continue

                row[f'{col}_mean'] = values.mean()
                row[f'{col}_stddev'] = values.std() if len(values) > 1 else 0

                if len(values) > 1:
                    se = values.std() / np.sqrt(len(values))
                    row[f'{col}_ci95'] = 1.96 * se
                else:
                    row[f'{col}_ci95'] = 0

                # Bootstrap CI for tail latencies (reviewer: tail CIs missing).
                if col in percentile_cols and len(values) > 1:
                    lo, hi = self._bootstrap_mean_ci(values.values)
                    row[f'{col}_ci95_bootstrap_lo'] = lo
                    row[f'{col}_ci95_bootstrap_hi'] = hi

            # Guard against Valkey-Cold ±0 suspicion (reviewer smaller note):
            # flag runs where throughput stddev is literally zero AND the
            # number of runs is > 1, so a reviewer can see we checked.
            if 'avg_throughput' in group.columns and len(group) > 1:
                raw = group['avg_throughput'].dropna().values
                if len(raw) > 1 and np.ptp(raw) == 0:
                    row['throughput_identical_across_runs'] = True
                    row['throughput_raw_values'] = ','.join(f'{v:.3f}' for v in raw)
                else:
                    row['throughput_identical_across_runs'] = False

            agg_rows.append(row)

        self.aggregated = pd.DataFrame(agg_rows)

    def print_summary(self):
        """Print summary statistics with stddev"""
        df = self.aggregated if not self.aggregated.empty else None

        if df is None:
            # Fall back to raw results for single-run data
            df = self.results
            if df.empty:
                print("No results to display")
                return
            self._print_legacy_summary()
            return

        print("\n" + "=" * 120)
        print("BENCHMARK RESULTS SUMMARY (aggregated across runs)")
        print("=" * 120)

        for scenario in ['cold', 'warm', 'load']:
            scenario_data = df[df['scenario'] == scenario]

            if scenario_data.empty:
                continue

            print(f"\n{'─' * 120}")
            print(f"SCENARIO: {scenario.upper()}")
            print('─' * 120)

            print(f"\n{'Backend':<12} {'Queue Type':<18} {'Runs':<6} "
                  f"{'Throughput (j/s)':<20} "
                  f"{'e2e p95 (ms)':<16} "
                  f"{'service p95':<14} "
                  f"{'broker p95':<14} "
                  f"{'CPU %':<8}")
            print('─' * 120)

            for _, row in scenario_data.iterrows():
                backend = row['backend']
                queue_type = row['queue_type']
                num_runs = int(row['num_runs'])

                tp = f"{row.get('avg_throughput_mean', 0):.0f}"
                tp_sd = row.get('avg_throughput_stddev', 0)
                if tp_sd > 0:
                    tp += f" +/-{tp_sd:.0f}"

                e2e_p95 = f"{row.get('latency_p95_ms_mean', 0):.1f}"
                svc_p95_mean = row.get('service_p95_ms_mean', float('nan'))
                svc_p95 = f"{svc_p95_mean:.1f}" if svc_p95_mean == svc_p95_mean else "N/A"
                brk_p95_mean = row.get('broker_p95_ms_mean', float('nan'))
                brk_p95 = f"{brk_p95_mean:.2f}" if brk_p95_mean == brk_p95_mean else "N/A"

                cpu_mean = row.get('avg_cpu_user_mean', 0)
                cpu = f"{cpu_mean:.1f}" if cpu_mean else "N/A"

                print(f"{backend:<12} {queue_type:<18} {num_runs:<6} "
                      f"{tp:<20} {e2e_p95:<16} {svc_p95:<14} {brk_p95:<14} {cpu:<8}")

        print("\n" + "=" * 120)

        # Exact percentile table — end-to-end latency
        print("\nEXACT END-TO-END PERCENTILES (enqueue -> ack, mean across runs):")
        print("=" * 120)
        print(f"{'Backend':<12} {'Queue':<18} {'Scenario':<8} "
              f"{'p50':<12} {'p95':<12} {'p99':<12} {'avg':<12} {'max':<12}")
        print('─' * 120)

        for _, row in df.iterrows():
            print(f"{row['backend']:<12} {row['queue_type']:<18} {row['scenario']:<8} "
                  f"{row.get('latency_p50_ms_mean', 0):<12.2f} "
                  f"{row.get('latency_p95_ms_mean', 0):<12.2f} "
                  f"{row.get('latency_p99_ms_mean', 0):<12.2f} "
                  f"{row.get('latency_avg_ms_mean', 0):<12.2f} "
                  f"{row.get('latency_max_ms_mean', 0):<12.2f}")
        print("=" * 120)

        # Broker overhead table — the fair infrastructure metric
        print("\nEXACT BROKER OVERHEAD PER MESSAGE (mean across runs):")
        print("  = (batch_cycle_time - N × processing_time) / N")
        print("  Fair across batch sizes. Lower is better.")
        print("=" * 120)
        print(f"{'Backend':<12} {'Queue':<18} {'Scenario':<8} "
              f"{'p50 (ms)':<12} {'p95 (ms)':<12} {'p99 (ms)':<12} {'avg':<12}")
        print('─' * 120)

        for _, row in df.iterrows():
            b50 = row.get('broker_p50_ms_mean', float('nan'))
            if b50 != b50:  # NaN check
                continue
            print(f"{row['backend']:<12} {row['queue_type']:<18} {row['scenario']:<8} "
                  f"{row.get('broker_p50_ms_mean', 0):<12.3f} "
                  f"{row.get('broker_p95_ms_mean', 0):<12.3f} "
                  f"{row.get('broker_p99_ms_mean', 0):<12.3f} "
                  f"{row.get('broker_avg_ms_mean', 0):<12.3f}")
        print("=" * 120)

    def _print_legacy_summary(self):
        """Print summary for single-run data (backward compat)"""
        print("\n" + "=" * 100)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 100)

        for scenario in ['cold', 'warm', 'load']:
            scenario_data = self.results[self.results['scenario'] == scenario]

            if scenario_data.empty:
                continue

            print(f"\n{'─' * 100}")
            print(f"SCENARIO: {scenario.upper()}")
            print('─' * 100)

            print(f"\n{'Backend':<15} {'Queue Type':<20} {'Throughput':<15} "
                  f"{'p50 (ms)':<10} {'p95 (ms)':<10} {'p99 (ms)':<10} {'CPU %':<10}")
            print('─' * 100)

            for _, row in scenario_data.iterrows():
                backend = row['backend']
                queue_type = row['queue_type']
                throughput = f"{row['avg_throughput']:.0f} j/s"
                p50 = f"{row['latency_p50_ms']:.2f}"
                p95 = f"{row['latency_p95_ms']:.2f}"
                p99 = f"{row['latency_p99_ms']:.2f}"
                cpu = f"{row.get('avg_cpu_user', 0):.1f}" if 'avg_cpu_user' in row else "N/A"

                print(f"{backend:<15} {queue_type:<20} {throughput:<15} "
                      f"{p50:<10} {p95:<10} {p99:<10} {cpu:<10}")

        print("\n" + "=" * 100)

    def save_results(self, output_dir='results/analysis'):
        """Save analysis results"""
        os.makedirs(output_dir, exist_ok=True)

        # Save raw per-run results
        if not self.results.empty:
            csv_file = os.path.join(output_dir, 'all_runs.csv')
            self.results.to_csv(csv_file, index=False)
            print(f"\nAll runs saved to: {csv_file}")

        # Save aggregated results (for graph generation)
        if not self.aggregated.empty:
            agg_csv = os.path.join(output_dir, 'summary.csv')
            self.aggregated.to_csv(agg_csv, index=False)
            print(f"Aggregated summary saved to: {agg_csv}")

            agg_json = os.path.join(output_dir, 'summary.json')
            self.aggregated.to_json(agg_json, orient='records', indent=2)
            print(f"Detailed results saved to: {agg_json}")
        elif not self.results.empty:
            # Fallback: save raw as summary for backward compat
            csv_file = os.path.join(output_dir, 'summary.csv')
            self.results.to_csv(csv_file, index=False)
            print(f"\nSummary saved to: {csv_file}")

            json_file = os.path.join(output_dir, 'summary.json')
            self.results.to_json(json_file, orient='records', indent=2)
            print(f"Detailed results saved to: {json_file}")

    # ----- Significance testing (reviewer smaller notes) ---------------

    @staticmethod
    def _welch_t(a, b):
        """Welch's t-test p-value without scipy (normal approximation to t).

        Returns (t, df, p_two_sided). Uses Welch-Satterthwaite df and a
        normal approximation for the p-value, which is fine when the only
        thing we'll do with p is compare it to alpha=0.05 after Bonferroni-
        Holm correction and all effects are huge.
        """
        a = np.asarray(a, dtype=float)
        b = np.asarray(b, dtype=float)
        na, nb = len(a), len(b)
        if na < 2 or nb < 2:
            return (float('nan'), float('nan'), float('nan'))
        va = a.var(ddof=1)
        vb = b.var(ddof=1)
        t = (a.mean() - b.mean()) / math.sqrt(va / na + vb / nb)
        num = (va / na + vb / nb) ** 2
        den = (va ** 2) / (na ** 2 * (na - 1)) + (vb ** 2) / (nb ** 2 * (nb - 1))
        df_ws = num / den if den > 0 else float('nan')
        # Two-sided p via normal approximation (conservative at small df).
        from math import erf
        p = 2 * (1 - 0.5 * (1 + erf(abs(t) / math.sqrt(2))))
        return (float(t), float(df_ws), float(p))

    @staticmethod
    def _bonferroni_holm(pvalues):
        """Return Bonferroni-Holm adjusted p-values in original order."""
        pvalues = np.asarray(pvalues, dtype=float)
        n = len(pvalues)
        order = np.argsort(pvalues)
        adjusted = np.empty(n)
        running_max = 0.0
        for rank, idx in enumerate(order):
            raw = pvalues[idx] * (n - rank)
            running_max = max(running_max, min(raw, 1.0))
            adjusted[idx] = running_max
        return adjusted.tolist()

    def run_significance_tests(self):
        """Pairwise Welch's t on throughput across queue types within each
        scenario + Bonferroni-Holm correction. Reviewer smaller notes.
        """
        if self.results.empty:
            return

        print("\n" + "=" * 100)
        print("PAIRWISE SIGNIFICANCE TESTS (Welch's t on avg_throughput, "
              "Bonferroni-Holm corrected)")
        print("  Caveat: n=5 per group. Shapiro-Wilk has very low power at")
        print("  this sample size; non-rejection of normality is weak evidence,")
        print("  not confirmation. Effects reported here are all large enough")
        print("  to survive any reasonable non-parametric alternative.")
        print("=" * 100)

        for scenario in ['cold', 'warm', 'load']:
            pairs = []
            scen = self.results[self.results['scenario'] == scenario]
            if scen.empty:
                continue

            # Collect {(backend, queue_type): per-run throughputs}
            groups = {}
            for (backend, queue_type), g in scen.groupby(['backend', 'queue_type']):
                values = g['avg_throughput'].dropna().values
                if len(values) >= 2:
                    groups[(backend, queue_type)] = values

            keys = sorted(groups.keys())
            for i in range(len(keys)):
                for j in range(i + 1, len(keys)):
                    a_key, b_key = keys[i], keys[j]
                    t, df_ws, p = self._welch_t(groups[a_key], groups[b_key])
                    pairs.append({
                        'a': f"{a_key[0]}:{a_key[1]}",
                        'b': f"{b_key[0]}:{b_key[1]}",
                        'mean_a': float(np.mean(groups[a_key])),
                        'mean_b': float(np.mean(groups[b_key])),
                        't': t, 'df': df_ws, 'p_raw': p,
                    })

            if not pairs:
                continue

            adjusted = self._bonferroni_holm([p['p_raw'] for p in pairs])
            for p, adj in zip(pairs, adjusted):
                p['p_adjusted'] = adj

            print(f"\nSCENARIO: {scenario.upper()}  (pairwise tests: {len(pairs)})")
            print(f"{'A':<28} {'B':<28} "
                  f"{'mean_A':>10} {'mean_B':>10} {'t':>7} "
                  f"{'p_raw':>10} {'p_adj':>10} sig?")
            for p in sorted(pairs, key=lambda x: x['p_adjusted']):
                sig = '***' if p['p_adjusted'] < 0.001 else \
                      '**' if p['p_adjusted'] < 0.01 else \
                      '*' if p['p_adjusted'] < 0.05 else ''
                print(f"{p['a']:<28} {p['b']:<28} "
                      f"{p['mean_a']:>10.1f} {p['mean_b']:>10.1f} "
                      f"{p['t']:>7.2f} {p['p_raw']:>10.2e} "
                      f"{p['p_adjusted']:>10.2e} {sig}")

        print("\n" + "=" * 100)

    def report_identical_runs(self):
        """Reviewer smaller note: Valkey Cold ±0 stddev. Report which
        (backend, queue_type, scenario) groups had literally-identical
        throughput across runs, with the raw values, so a reviewer can
        distinguish 'rounded to zero' from 'pinned/cached in a way that
        eliminates legitimate variance'.
        """
        if self.aggregated.empty or 'throughput_identical_across_runs' not in self.aggregated.columns:
            return

        suspicious = self.aggregated[self.aggregated['throughput_identical_across_runs'] == True]  # noqa: E712
        if suspicious.empty:
            return

        print("\n" + "=" * 100)
        print("IDENTICAL-ACROSS-RUNS CHECK (stddev=0 groups)")
        print("=" * 100)
        for _, row in suspicious.iterrows():
            print(f"  {row['backend']:<10} {row['queue_type']:<20} {row['scenario']:<6} "
                  f"raw={row.get('throughput_raw_values', '?')}")
        print("Note: stddev=0 across runs is either (a) rounding in the")
        print("display, or (b) a genuinely deterministic bottleneck (e.g.")
        print("workers all block on the same key). Raw values above let a")
        print("reviewer tell which.")
        print("=" * 100)

    def analyze_app_queries(self, results_dir):
        """Analyze app query impact measurements if available"""
        import glob as glob_mod
        app_files = glob_mod.glob(os.path.join(results_dir, '*_app_queries.json'))
        baseline_file = os.path.join(results_dir, 'app_queries_baseline.json')

        if not app_files and not os.path.exists(baseline_file):
            return

        print("\n" + "=" * 80)
        print("APPLICATION QUERY IMPACT ANALYSIS")
        print("=" * 80)

        if os.path.exists(baseline_file):
            with open(baseline_file) as f:
                baseline = json.load(f)
            print(f"\nBaseline (no queue activity):")
            print(f"  SELECT p50: {baseline.get('select_p50_ms', 'N/A'):.2f}ms  "
                  f"p95: {baseline.get('select_p95_ms', 'N/A'):.2f}ms  "
                  f"p99: {baseline.get('select_p99_ms', 'N/A'):.2f}ms")

        for f in sorted(app_files):
            name = os.path.basename(f).replace('_app_queries.json', '')
            try:
                with open(f) as fh:
                    data = json.load(fh)
                print(f"\n{name}:")
                print(f"  SELECT p50: {data.get('select_p50_ms', 'N/A'):.2f}ms  "
                      f"p95: {data.get('select_p95_ms', 'N/A'):.2f}ms  "
                      f"p99: {data.get('select_p99_ms', 'N/A'):.2f}ms")
            except Exception:
                pass

        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Analyze benchmark results')
    parser.add_argument('--pg-results', help='PostgreSQL results directory (VM1)')
    parser.add_argument('--valkey-results', help='Valkey results directory (VM2)')
    parser.add_argument('--kafka-results', help='Kafka results directory (VM3)')
    parser.add_argument('--rabbitmq-results', help='RabbitMQ results directory (VM4)')
    parser.add_argument('--output', default='results/analysis',
                       help='Output directory for analysis')

    args = parser.parse_args()

    if not any([args.pg_results, args.valkey_results,
                args.kafka_results, args.rabbitmq_results]):
        print("Error: specify at least one of --pg-results, --valkey-results, "
              "--kafka-results, --rabbitmq-results")
        return

    analyzer = BenchmarkAnalyzer(
        pg_results_dir=args.pg_results,
        valkey_results_dir=args.valkey_results,
        kafka_results_dir=args.kafka_results,
        rabbitmq_results_dir=args.rabbitmq_results,
    )

    print("Analyzing benchmark results...")
    print("Percentile method: mean of per-run percentiles.")
    print("Latency CIs: bootstrap (2000 resamples). Throughput CIs: 95% Gaussian.")
    analyzer.analyze_all()
    analyzer.print_summary()
    analyzer.report_identical_runs()
    analyzer.run_significance_tests()
    analyzer.save_results(args.output)

    # Analyze app query impact if data available (PG + Valkey both have it;
    # Kafka/RabbitMQ VMs also run pgbench so they may too).
    for d in (args.pg_results, args.valkey_results,
              args.kafka_results, args.rabbitmq_results):
        if d:
            analyzer.analyze_app_queries(d)

    print("\nNext step: Generate graphs with:")
    print(f"  python3 generate_graphs.py --input {args.output}/summary.csv")


if __name__ == '__main__':
    main()
