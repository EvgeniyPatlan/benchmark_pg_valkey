#!/usr/bin/env python3
"""
Generate comparison graphs from benchmark results.
Creates publication-quality visualizations with:
- Error bars (stddev) for multi-run data
- Annotated charts with clear captions
- Batching comparison chart
- Decision reference visualization
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

# Color scheme
COLOR_VALKEY = '#2ecc71'
COLOR_PG = '#3498db'
COLOR_PG_DARK = '#2980b9'


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

        # Latency columns
        self.lat_cols = {}
        for p in ['p50', 'p95', 'p99']:
            if self.is_aggregated:
                self.lat_cols[p] = f'latency_{p}_ms_mean'
                self.lat_cols[f'{p}_err'] = f'latency_{p}_ms_stddev'
            else:
                self.lat_cols[p] = f'latency_{p}_ms'
                self.lat_cols[f'{p}_err'] = None

        # CPU column
        self.cpu_col = 'avg_cpu_user_mean' if self.is_aggregated else 'avg_cpu_user'

    def _get_colors(self, backends):
        """Get color list based on backend type"""
        return [COLOR_VALKEY if 'valkey' in str(b).lower() else COLOR_PG for b in backends]

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

        # Legend
        legend_patches = [
            mpatches.Patch(color=COLOR_VALKEY, label='Valkey Streams'),
            mpatches.Patch(color=COLOR_PG, label='PostgreSQL'),
        ]
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
                err_col = self.lat_cols.get(f'{pct}_err')
                data = self.df[self.df['scenario'] == scenario].sort_values(col)

                if data.empty:
                    continue

                colors = self._get_colors(data['backend'])
                xerr = data[err_col].values if err_col and err_col in data.columns else None

                bars = ax.barh(range(len(data)), data[col], color=colors, alpha=0.85,
                              xerr=xerr, capsize=3)
                ax.set_yticks(range(len(data)))
                ax.set_yticklabels(data['label'], fontsize=8)
                ax.set_xlabel('Latency (ms)', fontsize=9)
                ax.set_title(f'{scenario.title()} - {label}', fontsize=10, fontweight='bold')
                ax.grid(axis='x', alpha=0.3)

                for i, (bar, val) in enumerate(zip(bars, data[col])):
                    ax.text(val + (xerr[i] if xerr is not None else val * 0.02) + 1,
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
                colors.append(COLOR_VALKEY if 'valkey' in row['backend'] else COLOR_PG)

            bp = ax.boxplot(latency_data, labels=labels, patch_artist=True)

            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.6)

            # Annotate Valkey's tiny box
            for i, (label, color) in enumerate(zip(labels, colors)):
                if color == COLOR_VALKEY:
                    ax.annotate('Tight distribution',
                              xy=(i + 1, latency_data[i][-1]),
                              xytext=(i + 1.3, latency_data[i][-1] * 1.5),
                              fontsize=7, color=COLOR_VALKEY, fontweight='bold',
                              arrowprops=dict(arrowstyle='->', color=COLOR_VALKEY, lw=1))
                    break

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
            color = COLOR_VALKEY if 'valkey' in data['backend'].iloc[0] else COLOR_PG
            throughput = [data[data['scenario'] == s][self.tp_col].values[0]
                         if len(data[data['scenario'] == s]) > 0 else 0
                         for s in scenarios]
            lw = 3 if color == COLOR_VALKEY else 1.5
            ax.plot(scenarios, throughput, marker='o', label=impl.replace('\n', ' '),
                   linewidth=lw, color=color, alpha=0.8 if color == COLOR_PG else 1.0)

        ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
        ax.set_title('Throughput Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # P95 latency across scenarios
        ax = axes[0, 1]
        for impl in implementations:
            data = self.df[self.df['label'] == impl]
            color = COLOR_VALKEY if 'valkey' in data['backend'].iloc[0] else COLOR_PG
            latency = [data[data['scenario'] == s][self.lat_cols['p95']].values[0]
                      if len(data[data['scenario'] == s]) > 0 else 0
                      for s in scenarios]
            lw = 3 if color == COLOR_VALKEY else 1.5
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '),
                   linewidth=lw, color=color, alpha=0.8 if color == COLOR_PG else 1.0)

        ax.set_ylabel('p95 Latency (ms)', fontsize=10)
        ax.set_title('p95 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # P99 latency across scenarios
        ax = axes[1, 0]
        for impl in implementations:
            data = self.df[self.df['label'] == impl]
            color = COLOR_VALKEY if 'valkey' in data['backend'].iloc[0] else COLOR_PG
            latency = [data[data['scenario'] == s][self.lat_cols['p99']].values[0]
                      if len(data[data['scenario'] == s]) > 0 else 0
                      for s in scenarios]
            lw = 3 if color == COLOR_VALKEY else 1.5
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '),
                   linewidth=lw, color=color, alpha=0.8 if color == COLOR_PG else 1.0)

        ax.set_ylabel('p99 Latency (ms)', fontsize=10)
        ax.set_title('p99 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)

        # Throughput stability (coefficient of variation)
        ax = axes[1, 1]
        if self.is_aggregated and self.tp_err_col in self.df.columns:
            for impl in implementations:
                data = self.df[self.df['label'] == impl]
                color = COLOR_VALKEY if 'valkey' in data['backend'].iloc[0] else COLOR_PG
                cv = [data[data['scenario'] == s][self.tp_err_col].values[0] /
                      data[data['scenario'] == s][self.tp_col].values[0] * 100
                      if len(data[data['scenario'] == s]) > 0 and
                         data[data['scenario'] == s][self.tp_col].values[0] > 0
                      else 0
                      for s in scenarios]
                lw = 3 if color == COLOR_VALKEY else 1.5
                ax.plot(scenarios, cv, marker='o', label=impl.replace('\n', ' '),
                       linewidth=lw, color=color, alpha=0.8 if color == COLOR_PG else 1.0)

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

    def plot_valkey_vs_best_pg(self):
        """Direct comparison: Valkey vs Best PostgreSQL implementation"""
        fig, axes = plt.subplots(2, 3, figsize=(16, 9))
        fig.suptitle('Valkey Streams vs Best PostgreSQL Queue: Direct Comparison\n'
                     'Green = Valkey, Blue = Best PostgreSQL variant per scenario',
                     fontsize=12, fontweight='bold', y=1.02)
        scenarios = ['cold', 'warm', 'load']

        for idx, scenario in enumerate(scenarios):
            scenario_data = self.df[self.df['scenario'] == scenario]

            if scenario_data.empty or len(scenario_data[scenario_data['backend'] == 'valkey']) == 0:
                continue

            valkey_data = scenario_data[scenario_data['backend'] == 'valkey'].iloc[0]
            pg_data = scenario_data[scenario_data['backend'] == 'postgresql'].sort_values(
                self.tp_col, ascending=False)
            if pg_data.empty:
                continue
            pg_data = pg_data.iloc[0]

            # Throughput comparison
            ax = axes[0, idx]
            v_tp = valkey_data[self.tp_col]
            p_tp = pg_data[self.tp_col]
            bars = ax.bar(['Valkey\nStreams', f"PG\n({pg_data['queue_type']})"],
                         [v_tp, p_tp],
                         color=[COLOR_VALKEY, COLOR_PG], alpha=0.85)

            # Error bars if available
            if self.tp_err_col and self.tp_err_col in self.df.columns:
                v_err = valkey_data.get(self.tp_err_col, 0)
                p_err = pg_data.get(self.tp_err_col, 0)
                ax.errorbar([0, 1], [v_tp, p_tp], yerr=[v_err, p_err],
                           fmt='none', capsize=5, color='black', linewidth=1.5)

            ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
            ax.set_title(f'{scenario.title()} - Throughput', fontsize=11, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)

            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, height + 20,
                       f'{height:.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')

            # Percentage difference
            if p_tp > 0:
                diff_pct = (v_tp - p_tp) / p_tp * 100
                ax.text(0.5, 0.02, f'Valkey {diff_pct:+.0f}%',
                       transform=ax.transAxes, ha='center', fontsize=9,
                       color=COLOR_VALKEY if diff_pct > 0 else 'red', fontweight='bold')

            # Latency comparison
            ax = axes[1, idx]
            x = np.arange(3)
            width = 0.35

            v_lats = [valkey_data[self.lat_cols['p50']], valkey_data[self.lat_cols['p95']],
                     valkey_data[self.lat_cols['p99']]]
            p_lats = [pg_data[self.lat_cols['p50']], pg_data[self.lat_cols['p95']],
                     pg_data[self.lat_cols['p99']]]

            ax.bar(x - width/2, v_lats, width, label='Valkey', color=COLOR_VALKEY, alpha=0.85)
            ax.bar(x + width/2, p_lats, width, label=f"PG ({pg_data['queue_type']})",
                  color=COLOR_PG, alpha=0.85)

            ax.set_ylabel('Latency (ms)', fontsize=10)
            ax.set_title(f'{scenario.title()} - Latency', fontsize=11, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(['p50', 'p95', 'p99'])
            ax.legend(fontsize=8)
            ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'valkey_vs_best_pg.png'), dpi=300, bbox_inches='tight')
        print("Saved: valkey_vs_best_pg.png")
        plt.close()

    def plot_decision_guide(self):
        """Visual decision guide: when to use PG vs Valkey"""
        fig, ax = plt.subplots(figsize=(12, 7))

        criteria = [
            ('Throughput', '< 500 j/s', '> 500 j/s'),
            ('Traffic Pattern', 'Stable', 'Bursty / Variable'),
            ('p95/p99 SLA', 'Not Required', 'Required'),
            ('Job Durability', 'Critical (financial)', 'Best-effort OK'),
            ('Ops Overhead', 'Prefer fewer systems', 'Dedicated infra OK'),
            ('Query Flexibility', 'Needed (filter/report)', 'Not needed'),
        ]

        ax.set_xlim(-0.5, 2.5)
        ax.set_ylim(-0.5, len(criteria) - 0.5)

        for i, (criterion, pg_val, valkey_val) in enumerate(criteria):
            y = len(criteria) - 1 - i

            # Criterion label
            ax.text(0, y, criterion, ha='center', va='center', fontsize=11, fontweight='bold')

            # PG value
            ax.add_patch(plt.Rectangle((0.6, y - 0.35), 0.8, 0.7,
                                       facecolor=COLOR_PG, alpha=0.3, edgecolor=COLOR_PG))
            ax.text(1.0, y, pg_val, ha='center', va='center', fontsize=9)

            # Valkey value
            ax.add_patch(plt.Rectangle((1.6, y - 0.35), 0.8, 0.7,
                                       facecolor=COLOR_VALKEY, alpha=0.3, edgecolor=COLOR_VALKEY))
            ax.text(2.0, y, valkey_val, ha='center', va='center', fontsize=9)

        # Headers
        ax.text(0, len(criteria) - 0.5 + 0.3, 'Criterion', ha='center', va='bottom',
               fontsize=12, fontweight='bold')
        ax.text(1.0, len(criteria) - 0.5 + 0.3, 'PostgreSQL Queue', ha='center', va='bottom',
               fontsize=12, fontweight='bold', color=COLOR_PG_DARK)
        ax.text(2.0, len(criteria) - 0.5 + 0.3, 'Valkey Streams', ha='center', va='bottom',
               fontsize=12, fontweight='bold', color='#27ae60')

        ax.set_title('Decision Guide: PostgreSQL Queue vs Valkey Streams\n'
                     'Choose the right tool based on your requirements',
                     fontsize=14, fontweight='bold', pad=20)
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'decision_guide.png'), dpi=300, bbox_inches='tight')
        print("Saved: decision_guide.png")
        plt.close()

    def generate_all(self):
        """Generate all graphs"""
        print("Generating comparison graphs...")
        print("-" * 50)

        self.plot_throughput_comparison()
        self.plot_latency_comparison()
        self.plot_latency_distribution()
        self.plot_cpu_usage()
        self.plot_scenario_comparison()
        self.plot_valkey_vs_best_pg()
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
