#!/usr/bin/env python3
"""
Generate comparison graphs from benchmark results.

Changes vs legacy version (all driven by reviewer concerns):
  - Latency error bars for p95/p99 use asymmetric bootstrap 95% CIs
    (from analyze.py's *_ci95_bootstrap_lo/hi columns) instead of
    symmetric Gaussian stddev, which is wrong for skewed tail metrics.
  - New plot_service_vs_e2e(): side-by-side bars showing service
    latency (dequeue -> ack) vs end-to-end latency (enqueue -> ack)
    so a reviewer can see at a glance how much of p95 is queue
    buildup vs database processing.
  - All per-variant plots include skip_locked_batch automatically
    because they iterate self.df; no hardcoded variant list.
"""
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Color scheme (per backend; one color for all variants of each backend)
COLOR_VALKEY = '#2ecc71'
COLOR_PG = '#3498db'
COLOR_PG_DARK = '#2980b9'
COLOR_KAFKA = '#e74c3c'       # red family for Kafka
COLOR_RABBITMQ = '#f39c12'    # orange family for RabbitMQ (broker tradition)
COLOR_SERVICE = '#9b59b6'     # service latency bars (dequeue -> ack)
COLOR_E2E = '#16a085'         # end-to-end latency bars (distinct from RabbitMQ orange)

BACKEND_COLORS = {
    'postgresql': COLOR_PG,
    'pg': COLOR_PG,
    'valkey': COLOR_VALKEY,
    'kafka': COLOR_KAFKA,
    'rabbitmq': COLOR_RABBITMQ,
}


def _has_aggregated_columns(df):
    """Check if dataframe has aggregated (mean/stddev) columns"""
    return 'avg_throughput_mean' in df.columns


class GraphGenerator:
    def __init__(self, summary_file, output_dir='results/graphs', all_runs_file=None):
        self.df = pd.read_csv(summary_file)
        self.output_dir = output_dir
        self.all_runs_df = pd.read_csv(all_runs_file) if all_runs_file and os.path.exists(all_runs_file) else None
        os.makedirs(output_dir, exist_ok=True)

        self.is_aggregated = _has_aggregated_columns(self.df)

        # Create readable labels
        self.df['label'] = self.df.apply(
            lambda row: f"{row['backend'].title()}\n{row['queue_type'].replace('_', ' ').title()}",
            axis=1
        )

        # Throughput column
        self.tp_col = 'avg_throughput_mean' if self.is_aggregated else 'avg_throughput'
        self.tp_err_col = 'avg_throughput_stddev' if self.is_aggregated else None

        # Latency columns (end-to-end = latency_*; service = service_*).
        # *_err is the stddev for symmetric error bars.
        # *_ci_lo/hi are column names for bootstrap CI bounds (absolute
        # latency values, which get_err_deltas() converts to offsets).
        self.lat_cols = {}
        for p in ['p50', 'p95', 'p99']:
            if self.is_aggregated:
                self.lat_cols[p] = f'latency_{p}_ms_mean'
                self.lat_cols[f'{p}_err'] = f'latency_{p}_ms_stddev'
                self.lat_cols[f'{p}_ci_lo'] = f'latency_{p}_ms_ci95_bootstrap_lo'
                self.lat_cols[f'{p}_ci_hi'] = f'latency_{p}_ms_ci95_bootstrap_hi'
                # Service latency counterparts (may be missing in legacy runs).
                self.lat_cols[f'service_{p}'] = f'service_{p}_ms_mean'
                self.lat_cols[f'service_{p}_err'] = f'service_{p}_ms_stddev'
                self.lat_cols[f'service_{p}_ci_lo'] = f'service_{p}_ms_ci95_bootstrap_lo'
                self.lat_cols[f'service_{p}_ci_hi'] = f'service_{p}_ms_ci95_bootstrap_hi'
            else:
                self.lat_cols[p] = f'latency_{p}_ms'
                self.lat_cols[f'{p}_err'] = None
                self.lat_cols[f'{p}_ci_lo'] = None
                self.lat_cols[f'{p}_ci_hi'] = None
                self.lat_cols[f'service_{p}'] = f'service_{p}_ms'
                self.lat_cols[f'service_{p}_err'] = None

        # True if this run has service latency columns (i.e., post-reviewer
        # worker changes). Drives whether plot_service_vs_e2e runs.
        self.has_service_latency = (
            self.is_aggregated
            and 'service_p95_ms_mean' in self.df.columns
            and not self.df['service_p95_ms_mean'].isna().all()
        )

        # CPU column
        self.cpu_col = 'avg_cpu_user_mean' if self.is_aggregated else 'avg_cpu_user'

    def _err_deltas(self, data, mean_col, lo_col, hi_col, stddev_col=None):
        """Return (lower_delta, upper_delta) arrays for matplotlib error bars.

        Prefers bootstrap CI columns if present; falls back to stddev for
        backward compat. matplotlib expects non-negative offsets from the
        central value, so we convert absolute CI bounds into deltas.
        """
        if (lo_col and hi_col
                and lo_col in data.columns and hi_col in data.columns
                and not data[lo_col].isna().all()):
            lower = (data[mean_col] - data[lo_col]).clip(lower=0).values
            upper = (data[hi_col] - data[mean_col]).clip(lower=0).values
            return np.vstack([lower, upper])
        if stddev_col and stddev_col in data.columns:
            return data[stddev_col].values
        return None

    def _get_colors(self, backends):
        """Map backend names to their representative color. Unknown backends
        fall back to PG blue so the graph doesn't crash on new backends."""
        return [BACKEND_COLORS.get(str(b).lower(), COLOR_PG) for b in backends]

    def plot_throughput_comparison(self):
        """Compare throughput across all implementations with error bars"""
        fig, axes = plt.subplots(1, 3, figsize=(16, 5.5))
        fig.suptitle('Throughput Comparison: PostgreSQL Queues vs Valkey Streams',
                     fontsize=13, fontweight='bold', y=1.02)
        scenarios = ['cold', 'warm', 'load']

        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario].sort_values(self.tp_col, ascending=False)

            if data.empty:
                continue

            colors = self._get_colors(data['backend'])
            yerr = data[self.tp_err_col].values if self.tp_err_col and self.tp_err_col in data.columns else None

            bars = ax.bar(range(len(data)), data[self.tp_col], color=colors, alpha=0.85,
                         yerr=yerr, capsize=4, error_kw={'linewidth': 1.5})
            ax.set_xticks(range(len(data)))
            ax.set_xticklabels(data['label'], rotation=45, ha='right', fontsize=8)
            ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)

            for i, (bar, val) in enumerate(zip(bars, data[self.tp_col])):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (yerr[i] if yerr is not None else 0) + 20,
                       f'{val:.0f}', ha='center', va='bottom', fontsize=8, fontweight='bold')

        # Legend reflects only the backends actually present in the data
        present = set(self.df['backend'].dropna().astype(str).str.lower())
        label_map = [
            ('postgresql', 'PostgreSQL', COLOR_PG),
            ('valkey',     'Valkey Streams', COLOR_VALKEY),
            ('kafka',      'Kafka', COLOR_KAFKA),
            ('rabbitmq',   'RabbitMQ', COLOR_RABBITMQ),
        ]
        legend_patches = [mpatches.Patch(color=c, label=lbl)
                          for key, lbl, c in label_map if key in present]
        fig.legend(handles=legend_patches, loc='upper right', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'throughput_comparison.png'), dpi=300, bbox_inches='tight')
        print("Saved: throughput_comparison.png")
        plt.close()

    def plot_latency_comparison(self):
        """Compare latency percentiles with error bars"""
        fig, axes = plt.subplots(3, 3, figsize=(16, 13))
        fig.suptitle('Latency Comparison by Scenario and Percentile\n'
                     '(lower is better; note scale differences between columns)',
                     fontsize=13, fontweight='bold', y=1.01)
        scenarios = ['cold', 'warm', 'load']
        percentiles = ['p50', 'p95', 'p99']
        percentile_labels = ['p50 (Median)', 'p95 (SLA target)', 'p99 (Worst case)']

        for row_idx, scenario in enumerate(scenarios):
            for col_idx, (pct, label) in enumerate(zip(percentiles, percentile_labels)):
                ax = axes[row_idx, col_idx]
                col = self.lat_cols[pct]
                data = self.df[self.df['scenario'] == scenario].sort_values(col)

                if data.empty:
                    continue

                colors = self._get_colors(data['backend'])
                # Prefer bootstrap CIs (asymmetric) for tails — Gaussian
                # symmetric CIs are badly wrong for right-skewed p95/p99.
                xerr = self._err_deltas(
                    data,
                    mean_col=col,
                    lo_col=self.lat_cols.get(f'{pct}_ci_lo'),
                    hi_col=self.lat_cols.get(f'{pct}_ci_hi'),
                    stddev_col=self.lat_cols.get(f'{pct}_err'),
                )

                bars = ax.barh(range(len(data)), data[col], color=colors, alpha=0.85,
                              xerr=xerr, capsize=3)
                ax.set_yticks(range(len(data)))
                ax.set_yticklabels(data['label'], fontsize=8)
                ax.set_xlabel('Latency (ms)', fontsize=9)
                ax.set_title(f'{scenario.title()} - {label}', fontsize=10, fontweight='bold')
                ax.grid(axis='x', alpha=0.3)

                # Pull per-row upper error for label positioning.
                if xerr is not None and np.ndim(xerr) == 2:
                    upper_err = xerr[1]
                elif xerr is not None:
                    upper_err = np.asarray(xerr)
                else:
                    upper_err = None

                for i, (bar, val) in enumerate(zip(bars, data[col])):
                    offset = (upper_err[i] if upper_err is not None else val * 0.02) + 1
                    ax.text(val + offset,
                           bar.get_y() + bar.get_height()/2,
                           f'{val:.1f}', ha='left', va='center', fontsize=7)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'latency_comparison.png'), dpi=300, bbox_inches='tight')
        print("Saved: latency_comparison.png")
        plt.close()

    def plot_latency_distribution(self):
        """Box plot showing latency distribution with annotation"""
        fig, axes = plt.subplots(1, 3, figsize=(16, 5.5))
        fig.suptitle('Latency Distribution: Valkey (tight, predictable) vs PostgreSQL (wide, variable)\n'
                     'Smaller boxes = more consistent performance',
                     fontsize=12, fontweight='bold', y=1.03)
        scenarios = ['cold', 'warm', 'load']

        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario]

            if data.empty:
                continue

            latency_data = []
            labels = []
            colors = []

            for _, row in data.iterrows():
                p50 = row[self.lat_cols['p50']]
                p95 = row[self.lat_cols['p95']]
                p99 = row[self.lat_cols['p99']]
                synthetic = [p50] * 50 + [p95] * 45 + [p99] * 5
                latency_data.append(synthetic)
                labels.append(row['label'])
                colors.append(BACKEND_COLORS.get(str(row['backend']).lower(), COLOR_PG))

            bp = ax.boxplot(latency_data, labels=labels, patch_artist=True)

            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.6)

            # Annotate the tightest distribution (smallest p99-p50 span) —
            # used to be Valkey-specific; generalized so Kafka/RabbitMQ
            # can win this on their data.
            if latency_data:
                spans = [(max(ld) - min(ld), i) for i, ld in enumerate(latency_data)]
                _, tight_idx = min(spans)
                ax.annotate('Tightest distribution',
                            xy=(tight_idx + 1, latency_data[tight_idx][-1]),
                            xytext=(tight_idx + 1.3, latency_data[tight_idx][-1] * 1.5),
                            fontsize=7, color=colors[tight_idx], fontweight='bold',
                            arrowprops=dict(arrowstyle='->', color=colors[tight_idx], lw=1))

            ax.set_ylabel('Latency (ms)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'latency_distribution.png'), dpi=300, bbox_inches='tight')
        print("Saved: latency_distribution.png")
        plt.close()

    def plot_cpu_usage(self):
        """Compare CPU usage with error bars"""
        if self.cpu_col not in self.df.columns:
            print("Warning: CPU usage data not available")
            return

        fig, axes = plt.subplots(1, 3, figsize=(16, 5.5))
        fig.suptitle('CPU Utilization: Lower = More Efficient\n'
                     'Valkey processes more jobs with less CPU',
                     fontsize=12, fontweight='bold', y=1.03)
        scenarios = ['cold', 'warm', 'load']

        cpu_err_col = 'avg_cpu_user_stddev' if self.is_aggregated else None

        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario].sort_values(self.cpu_col, ascending=False)

            if data.empty:
                continue

            colors = self._get_colors(data['backend'])
            yerr = data[cpu_err_col].values if cpu_err_col and cpu_err_col in data.columns else None

            bars = ax.bar(range(len(data)), data[self.cpu_col], color=colors, alpha=0.85,
                         yerr=yerr, capsize=4)
            ax.set_xticks(range(len(data)))
            ax.set_xticklabels(data['label'], rotation=45, ha='right', fontsize=8)
            ax.set_ylabel('CPU Usage (%)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.set_ylim(0, 100)
            ax.grid(axis='y', alpha=0.3)

            for bar, val in zip(bars, data[self.cpu_col]):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                       f'{val:.1f}%', ha='center', va='bottom', fontsize=8, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'cpu_usage.png'), dpi=300, bbox_inches='tight')
        print("Saved: cpu_usage.png")
        plt.close()

    def plot_scenario_comparison(self):
        """Compare same implementation across scenarios"""
        implementations = self.df['label'].unique()

        fig, axes = plt.subplots(2, 2, figsize=(16, 11))
        fig.suptitle('Performance Across Scenarios: How Each Implementation Degrades Under Stress\n'
                     'Flat lines = stable performance under load',
                     fontsize=12, fontweight='bold', y=1.02)

        scenarios = ['cold', 'warm', 'load']

        # Throughput across scenarios
        ax = axes[0, 0]
        for impl in implementations:
            data = self.df[self.df['label'] == impl]
            color = BACKEND_COLORS.get(str(data['backend'].iloc[0]).lower(), COLOR_PG)
            throughput = [data[data['scenario'] == s][self.tp_col].values[0]
                         if len(data[data['scenario'] == s]) > 0 else 0
                         for s in scenarios]
            ax.plot(scenarios, throughput, marker='o', label=impl.replace('\n', ' '),
                   linewidth=2.0, color=color, alpha=0.9)

        ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
        ax.set_title('Throughput Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # P95 latency across scenarios
        ax = axes[0, 1]
        for impl in implementations:
            data = self.df[self.df['label'] == impl]
            color = BACKEND_COLORS.get(str(data['backend'].iloc[0]).lower(), COLOR_PG)
            latency = [data[data['scenario'] == s][self.lat_cols['p95']].values[0]
                      if len(data[data['scenario'] == s]) > 0 else 0
                      for s in scenarios]
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '),
                   linewidth=2.0, color=color, alpha=0.9)

        ax.set_ylabel('p95 Latency (ms)', fontsize=10)
        ax.set_title('p95 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # P99 latency across scenarios
        ax = axes[1, 0]
        for impl in implementations:
            data = self.df[self.df['label'] == impl]
            color = BACKEND_COLORS.get(str(data['backend'].iloc[0]).lower(), COLOR_PG)
            latency = [data[data['scenario'] == s][self.lat_cols['p99']].values[0]
                      if len(data[data['scenario'] == s]) > 0 else 0
                      for s in scenarios]
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '),
                   linewidth=2.0, color=color, alpha=0.9)

        ax.set_ylabel('p99 Latency (ms)', fontsize=10)
        ax.set_title('p99 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # Throughput stability (coefficient of variation)
        ax = axes[1, 1]
        if self.is_aggregated and self.tp_err_col in self.df.columns:
            for impl in implementations:
                data = self.df[self.df['label'] == impl]
                color = BACKEND_COLORS.get(str(data['backend'].iloc[0]).lower(), COLOR_PG)
                cv = [data[data['scenario'] == s][self.tp_err_col].values[0] /
                      data[data['scenario'] == s][self.tp_col].values[0] * 100
                      if len(data[data['scenario'] == s]) > 0 and
                         data[data['scenario'] == s][self.tp_col].values[0] > 0
                      else 0
                      for s in scenarios]
                ax.plot(scenarios, cv, marker='o', label=impl.replace('\n', ' '),
                       linewidth=2.0, color=color, alpha=0.9)

            ax.set_ylabel('Throughput CV (%)', fontsize=10)
            ax.set_title('Throughput Variability (lower = more stable)', fontsize=12, fontweight='bold')
            ax.legend(fontsize=7, loc='best')
            ax.grid(alpha=0.3)
        else:
            ax.axis('off')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'scenario_comparison.png'), dpi=300, bbox_inches='tight')
        print("Saved: scenario_comparison.png")
        plt.close()

    def plot_best_per_backend(self):
        """Head-to-head: best variant of each backend, per scenario.

        Picks the highest-throughput variant within each backend and
        plots them side by side. Works with any subset of {pg, valkey,
        kafka, rabbitmq} — only backends actually present in the data
        show up.
        """
        backend_order = ['postgresql', 'valkey', 'kafka', 'rabbitmq']
        backend_labels = {
            'postgresql': 'PG', 'valkey': 'Valkey',
            'kafka': 'Kafka', 'rabbitmq': 'RabbitMQ',
        }

        fig, axes = plt.subplots(2, 3, figsize=(18, 10))
        fig.suptitle('Best Variant per Backend: Direct Comparison\n'
                     'Each bar = highest-throughput variant of that backend in that scenario',
                     fontsize=12, fontweight='bold', y=1.02)
        scenarios = ['cold', 'warm', 'load']

        for idx, scenario in enumerate(scenarios):
            scenario_data = self.df[self.df['scenario'] == scenario]
            if scenario_data.empty:
                continue

            # For each backend present, pick the row with the highest mean
            # throughput. Missing backends are silently skipped.
            best_rows = []
            for backend in backend_order:
                subset = scenario_data[scenario_data['backend'] == backend]
                if subset.empty:
                    continue
                winner = subset.sort_values(self.tp_col, ascending=False).iloc[0]
                best_rows.append((backend, winner))

            if not best_rows:
                continue

            # ---- Throughput subplot ----
            ax = axes[0, idx]
            labels = [f"{backend_labels[b]}\n({r['queue_type']})"
                      for b, r in best_rows]
            values = [r[self.tp_col] for _, r in best_rows]
            colors = [BACKEND_COLORS[b] for b, _ in best_rows]

            errs = None
            if self.tp_err_col and self.tp_err_col in self.df.columns:
                errs = [r.get(self.tp_err_col, 0) for _, r in best_rows]

            bars = ax.bar(range(len(labels)), values, color=colors, alpha=0.85,
                          yerr=errs, capsize=4)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, fontsize=8)
            ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
            ax.set_title(f'{scenario.title()} - Throughput', fontsize=11, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            for bar, v in zip(bars, values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
                        f'{v:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')

            # ---- Latency subplot ----
            ax = axes[1, idx]
            x = np.arange(3)
            width = 0.8 / max(len(best_rows), 1)

            for i, (backend, row) in enumerate(best_rows):
                lats = [row[self.lat_cols[p]] for p in ('p50', 'p95', 'p99')]
                # Asymmetric bootstrap errors where available.
                errs_2d = []
                for p in ('p50', 'p95', 'p99'):
                    lo_c = self.lat_cols.get(f'{p}_ci_lo')
                    hi_c = self.lat_cols.get(f'{p}_ci_hi')
                    sd_c = self.lat_cols.get(f'{p}_err')
                    mean = row[self.lat_cols[p]]
                    if (lo_c and hi_c and lo_c in row.index
                            and not pd.isna(row.get(lo_c, np.nan))):
                        errs_2d.append((max(0, mean - row[lo_c]),
                                        max(0, row[hi_c] - mean)))
                    elif sd_c and sd_c in row.index and not pd.isna(row.get(sd_c, np.nan)):
                        errs_2d.append((row[sd_c], row[sd_c]))
                    else:
                        errs_2d.append((0, 0))
                errs_2d = np.array(errs_2d).T

                offset = (i - (len(best_rows) - 1) / 2) * width
                ax.bar(x + offset, lats, width,
                       label=f"{backend_labels[backend]} ({row['queue_type']})",
                       color=BACKEND_COLORS[backend], alpha=0.85,
                       yerr=errs_2d, capsize=3)

            ax.set_ylabel('Latency (ms, log scale)', fontsize=10)
            ax.set_yscale('log')
            ax.set_title(f'{scenario.title()} - Latency (end-to-end)',
                         fontsize=11, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(['p50', 'p95', 'p99'])
            ax.legend(fontsize=7, loc='upper left')
            ax.grid(axis='y', alpha=0.3, which='both')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'best_per_backend.png'),
                    dpi=300, bbox_inches='tight')
        print("Saved: best_per_backend.png")
        plt.close()

    def plot_service_vs_e2e(self):
        """Side-by-side service vs end-to-end latency (reviewer concern #1).

        For each (scenario, percentile) cell, draw two bars per variant:
          orange = end-to-end (enqueue -> ack, the user-visible number)
          purple = service    (dequeue -> ack, the DB/broker's contribution)

        The gap between the two bars is queue wait time. If the producer
        is overdriven, e2e will tower over service and the chart makes
        that visible without the reader having to do arithmetic.
        """
        if not self.has_service_latency:
            print("Skipping: service_vs_e2e (no service_* columns; run with "
                  "updated workers)")
            return

        fig, axes = plt.subplots(3, 3, figsize=(18, 13))
        fig.suptitle('Service latency (dequeue->ack) vs End-to-end latency '
                     '(enqueue->ack)\n'
                     'Gap between bars = time spent waiting in queue. '
                     'Large gap => arrival rate exceeds service rate.',
                     fontsize=13, fontweight='bold', y=1.005)
        scenarios = ['cold', 'warm', 'load']
        percentiles = ['p50', 'p95', 'p99']

        for row_idx, scenario in enumerate(scenarios):
            for col_idx, pct in enumerate(percentiles):
                ax = axes[row_idx, col_idx]
                e2e_col = self.lat_cols[pct]
                svc_col = self.lat_cols[f'service_{pct}']
                data = self.df[self.df['scenario'] == scenario].sort_values(e2e_col)

                if data.empty or svc_col not in data.columns:
                    ax.axis('off')
                    continue

                y = np.arange(len(data))
                h = 0.4

                e2e_vals = data[e2e_col].values
                svc_vals = data[svc_col].fillna(0).values

                e2e_err = self._err_deltas(
                    data, e2e_col,
                    self.lat_cols.get(f'{pct}_ci_lo'),
                    self.lat_cols.get(f'{pct}_ci_hi'),
                    self.lat_cols.get(f'{pct}_err'),
                )
                svc_err = self._err_deltas(
                    data, svc_col,
                    self.lat_cols.get(f'service_{pct}_ci_lo'),
                    self.lat_cols.get(f'service_{pct}_ci_hi'),
                    self.lat_cols.get(f'service_{pct}_err'),
                )

                ax.barh(y + h / 2, e2e_vals, h, color=COLOR_E2E, alpha=0.85,
                        label='End-to-end', xerr=e2e_err, capsize=3)
                ax.barh(y - h / 2, svc_vals, h, color=COLOR_SERVICE, alpha=0.85,
                        label='Service', xerr=svc_err, capsize=3)

                ax.set_yticks(y)
                ax.set_yticklabels(data['label'], fontsize=8)
                ax.set_xlabel('Latency (ms, log scale)', fontsize=9)
                ax.set_xscale('log')
                ax.set_title(f'{scenario.title()} - {pct}', fontsize=10, fontweight='bold')
                ax.grid(axis='x', alpha=0.3, which='both')

                if row_idx == 0 and col_idx == 2:
                    ax.legend(loc='lower right', fontsize=8)

                # Annotate the largest queue-wait gap in each cell.
                if len(data) > 0:
                    gaps = e2e_vals - svc_vals
                    if np.isfinite(gaps).any():
                        worst = int(np.nanargmax(gaps))
                        if gaps[worst] > svc_vals[worst] * 0.5 and svc_vals[worst] > 0:
                            ratio = e2e_vals[worst] / max(svc_vals[worst], 0.001)
                            ax.annotate(
                                f'{ratio:.0f}x queue wait',
                                xy=(e2e_vals[worst], worst + h / 2),
                                xytext=(e2e_vals[worst] * 1.15, worst - 0.3),
                                fontsize=7, color='black',
                                arrowprops=dict(arrowstyle='->', color='black', lw=0.8))

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'service_vs_e2e_latency.png'),
                    dpi=300, bbox_inches='tight')
        print("Saved: service_vs_e2e_latency.png")
        plt.close()

    def plot_decision_guide(self):
        """Visual decision guide across 4 backends.

        Each row = decision criterion. Each column = backend.
        Cell text = which use-case that backend fits best under that criterion.
        """
        fig, ax = plt.subplots(figsize=(18, 8))

        # Columns: (backend_key, display_name, color)
        columns = [
            ('postgresql', 'PostgreSQL Queue', COLOR_PG),
            ('valkey',     'Valkey Streams',   COLOR_VALKEY),
            ('kafka',      'Kafka',            COLOR_KAFKA),
            ('rabbitmq',   'RabbitMQ',         COLOR_RABBITMQ),
        ]

        # Rows: (criterion, [value per column]) — order matches `columns` above
        criteria = [
            ('Throughput',         ['< 500 j/s',          '> 500 j/s, bursty',   '> 10k j/s',              '500 – 5k j/s']),
            ('Traffic pattern',    ['Stable',             'Bursty / variable',   'Sustained high-rate',    'Mixed, short-lived jobs']),
            ('p95/p99 SLA',        ['Not required',       'Required',            'Required (high-vol)',    'Moderate; quorum relaxes tails']),
            ('Durability',         ['Critical (WAL)',     'Best-effort',         'Strong (replicated log)','Strong (quorum) / medium (classic)']),
            ('Ops overhead',       ['Prefer fewer systems','Dedicated infra OK', 'Significant (ZK/KRaft)', 'Moderate (Erlang)']),
            ('Replay / history',   ['Not supported',      'Limited (stream)',    'First-class',            'Not supported']),
            ('Routing flexibility',['SQL filter only',    'None',                'Partition keys only',    'Exchanges, bindings, topics']),
        ]

        n_rows = len(criteria)
        n_cols = len(columns)

        ax.set_xlim(-0.5, n_cols + 0.5)
        ax.set_ylim(-0.5, n_rows - 0.5 + 0.8)

        for i, (criterion, values) in enumerate(criteria):
            y = n_rows - 1 - i
            ax.text(0, y, criterion, ha='center', va='center',
                    fontsize=10, fontweight='bold')

            for j, ((key, _name, color), val) in enumerate(zip(columns, values)):
                cx = j + 1
                ax.add_patch(plt.Rectangle((cx - 0.45, y - 0.4), 0.9, 0.8,
                                            facecolor=color, alpha=0.25,
                                            edgecolor=color, linewidth=1.5))
                ax.text(cx, y, val, ha='center', va='center', fontsize=8)

        # Column headers
        ax.text(0, n_rows - 0.5 + 0.5, 'Criterion', ha='center', va='bottom',
                fontsize=11, fontweight='bold')
        for j, (key, name, color) in enumerate(columns):
            ax.text(j + 1, n_rows - 0.5 + 0.5, name, ha='center', va='bottom',
                    fontsize=11, fontweight='bold', color=color)

        ax.set_title(
            'Decision Guide: choose the right queue for your workload\n'
            '(single-node deployment — cluster deployments shift the tradeoffs)',
            fontsize=13, fontweight='bold', pad=20)
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'decision_guide.png'),
                    dpi=300, bbox_inches='tight')
        print("Saved: decision_guide.png")
        plt.close()

    def generate_all(self):
        """Generate all graphs"""
        print("Generating comparison graphs...")
        print("-" * 50)

        self.plot_throughput_comparison()
        self.plot_latency_comparison()
        self.plot_service_vs_e2e()
        self.plot_latency_distribution()
        self.plot_cpu_usage()
        self.plot_scenario_comparison()
        self.plot_best_per_backend()
        self.plot_decision_guide()

        print("-" * 50)
        print(f"All graphs saved to: {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(description='Generate benchmark comparison graphs')
    parser.add_argument('--input', required=True,
                       help='Input CSV file (e.g., results/analysis/summary.csv)')
    parser.add_argument('--all-runs', default=None,
                       help='All runs CSV file (e.g., results/analysis/all_runs.csv)')
    parser.add_argument('--output', default='results/graphs',
                       help='Output directory for graphs')

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        return

    generator = GraphGenerator(args.input, args.output, args.all_runs)
    generator.generate_all()

    print("\nGraphs generated successfully!")
    print(f"View graphs in: {args.output}/")


if __name__ == '__main__':
    main()
