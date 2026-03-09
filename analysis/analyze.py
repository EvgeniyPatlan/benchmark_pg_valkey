#!/usr/bin/env python3
"""
Analyze benchmark results
Supports multi-run aggregation with mean +/- stddev and confidence intervals.
"""
import argparse
import json
import os
import re
from pathlib import Path
import pandas as pd
import numpy as np


class BenchmarkAnalyzer:
    def __init__(self, pg_results_dir=None, valkey_results_dir=None):
        self.pg_results_dir = pg_results_dir
        self.valkey_results_dir = valkey_results_dir
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

        stats = {
            'total_jobs': df['jobs_processed'].iloc[-1] if len(df) > 0 else 0,
            'duration_sec': df['elapsed'].iloc[-1] if len(df) > 0 else 0,
            'avg_throughput': df['throughput'].mean(),
            'max_throughput': df['throughput'].max(),
            'min_throughput': df['throughput'].min(),
            'latency_p50_ms': df['latency_p50'].iloc[-1] if len(df) > 0 else 0,
            'latency_p95_ms': df['latency_p95'].iloc[-1] if len(df) > 0 else 0,
            'latency_p99_ms': df['latency_p99'].iloc[-1] if len(df) > 0 else 0,
            'latency_avg_ms': df['latency_avg'].iloc[-1] if len(df) > 0 else 0,
            'latency_max_ms': df['latency_max'].iloc[-1] if len(df) > 0 else 0,
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

    def analyze_all(self):
        """Analyze all results"""
        all_results = []

        # PostgreSQL queues
        if self.pg_results_dir:
            queue_types = ['skip_locked', 'delete_returning', 'partitioned']
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

        if all_results:
            self.results = pd.DataFrame(all_results)
            self._aggregate_runs()
            return self.results
        else:
            print("No results found!")
            return pd.DataFrame()

    def _aggregate_runs(self):
        """Aggregate multi-run results into mean +/- stddev with confidence intervals"""
        if self.results.empty:
            return

        group_cols = ['backend', 'queue_type', 'scenario']
        metric_cols = ['avg_throughput', 'latency_p50_ms', 'latency_p95_ms',
                       'latency_p99_ms', 'latency_avg_ms', 'latency_max_ms',
                       'total_jobs']

        # Add CPU if available
        if 'avg_cpu_user' in self.results.columns:
            metric_cols.extend(['avg_cpu_user', 'avg_cpu_system', 'avg_mem_used_pct'])

        agg_rows = []
        for name, group in self.results.groupby(group_cols):
            row = {
                'backend': name[0],
                'queue_type': name[1],
                'scenario': name[2],
                'num_runs': len(group),
            }

            for col in metric_cols:
                if col in group.columns:
                    values = group[col].dropna()
                    if len(values) > 0:
                        row[f'{col}_mean'] = values.mean()
                        row[f'{col}_stddev'] = values.std() if len(values) > 1 else 0
                        # 95% confidence interval
                        if len(values) > 1:
                            se = values.std() / np.sqrt(len(values))
                            row[f'{col}_ci95'] = 1.96 * se
                        else:
                            row[f'{col}_ci95'] = 0

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
                  f"{'Throughput (j/s)':<22} {'p50 (ms)':<18} "
                  f"{'p95 (ms)':<18} {'p99 (ms)':<18} {'CPU %':<12}")
            print('─' * 120)

            for _, row in scenario_data.iterrows():
                backend = row['backend']
                queue_type = row['queue_type']
                num_runs = int(row['num_runs'])

                tp = f"{row.get('avg_throughput_mean', 0):.0f}"
                tp_sd = row.get('avg_throughput_stddev', 0)
                if tp_sd > 0:
                    tp += f" +/-{tp_sd:.0f}"

                p50 = f"{row.get('latency_p50_ms_mean', 0):.1f}"
                p50_sd = row.get('latency_p50_ms_stddev', 0)
                if p50_sd > 0:
                    p50 += f" +/-{p50_sd:.1f}"

                p95 = f"{row.get('latency_p95_ms_mean', 0):.1f}"
                p95_sd = row.get('latency_p95_ms_stddev', 0)
                if p95_sd > 0:
                    p95 += f" +/-{p95_sd:.1f}"

                p99 = f"{row.get('latency_p99_ms_mean', 0):.1f}"
                p99_sd = row.get('latency_p99_ms_stddev', 0)
                if p99_sd > 0:
                    p99 += f" +/-{p99_sd:.1f}"

                cpu_mean = row.get('avg_cpu_user_mean', 0)
                cpu = f"{cpu_mean:.1f}" if cpu_mean else "N/A"

                print(f"{backend:<12} {queue_type:<18} {num_runs:<6} "
                      f"{tp:<22} {p50:<18} {p95:<18} {p99:<18} {cpu:<12}")

        print("\n" + "=" * 120)

        # Exact percentile table
        print("\nEXACT PERCENTILE VALUES (mean across runs):")
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
    parser.add_argument('--pg-results', help='PostgreSQL results directory')
    parser.add_argument('--valkey-results', help='Valkey results directory')
    parser.add_argument('--output', default='results/analysis',
                       help='Output directory for analysis')

    args = parser.parse_args()

    if not args.pg_results and not args.valkey_results:
        print("Error: Must specify at least one of --pg-results or --valkey-results")
        return

    analyzer = BenchmarkAnalyzer(args.pg_results, args.valkey_results)

    print("Analyzing benchmark results...")
    analyzer.analyze_all()
    analyzer.print_summary()
    analyzer.save_results(args.output)

    # Analyze app query impact if data available
    if args.pg_results:
        analyzer.analyze_app_queries(args.pg_results)
    if args.valkey_results:
        analyzer.analyze_app_queries(args.valkey_results)

    print("\nNext step: Generate graphs with:")
    print(f"  python3 generate_graphs.py --input {args.output}/summary.csv")


if __name__ == '__main__':
    main()
