#!/usr/bin/env python3
"""
Generate comparison graphs from benchmark results
Creates publication-quality visualizations
"""
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10


class GraphGenerator:
    def __init__(self, summary_file, output_dir='results/graphs'):
        self.df = pd.read_csv(summary_file)
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Create readable labels
        self.df['label'] = self.df.apply(
            lambda row: f"{row['backend'].title()}\n{row['queue_type'].replace('_', ' ').title()}", 
            axis=1
        )
    
    def plot_throughput_comparison(self):
        """Compare throughput across all implementations"""
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        scenarios = ['cold', 'warm', 'load']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario].sort_values('avg_throughput', ascending=False)
            
            colors = ['#2ecc71' if 'valkey' in b else '#3498db' for b in data['backend']]
            
            bars = ax.bar(range(len(data)), data['avg_throughput'], color=colors, alpha=0.8)
            ax.set_xticks(range(len(data)))
            ax.set_xticklabels(data['label'], rotation=45, ha='right', fontsize=8)
            ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Add value labels on bars
            for i, (bar, val) in enumerate(zip(bars, data['avg_throughput'])):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                       f'{val:.0f}', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'throughput_comparison.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: throughput_comparison.png")
        plt.close()
    
    def plot_latency_comparison(self):
        """Compare latency percentiles"""
        fig, axes = plt.subplots(3, 3, figsize=(15, 12))
        scenarios = ['cold', 'warm', 'load']
        percentiles = ['latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms']
        percentile_labels = ['p50 (Median)', 'p95', 'p99']
        
        for row_idx, scenario in enumerate(scenarios):
            for col_idx, (percentile, label) in enumerate(zip(percentiles, percentile_labels)):
                ax = axes[row_idx, col_idx]
                data = self.df[self.df['scenario'] == scenario].sort_values(percentile)
                
                colors = ['#2ecc71' if 'valkey' in b else '#3498db' for b in data['backend']]
                
                bars = ax.barh(range(len(data)), data[percentile], color=colors, alpha=0.8)
                ax.set_yticks(range(len(data)))
                ax.set_yticklabels(data['label'], fontsize=8)
                ax.set_xlabel('Latency (ms)', fontsize=9)
                ax.set_title(f'{scenario.title()} - {label}', fontsize=10, fontweight='bold')
                ax.grid(axis='x', alpha=0.3)
                
                # Add value labels
                for i, (bar, val) in enumerate(zip(bars, data[percentile])):
                    ax.text(val + 1, bar.get_y() + bar.get_height()/2,
                           f'{val:.1f}', ha='left', va='center', fontsize=7)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'latency_comparison.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: latency_comparison.png")
        plt.close()
    
    def plot_latency_distribution(self):
        """Box plot showing latency distribution"""
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        scenarios = ['cold', 'warm', 'load']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario]
            
            # Prepare data for box plot
            latency_data = []
            labels = []
            colors = []
            
            for _, row in data.iterrows():
                # Create synthetic distribution from percentiles
                p50, p95, p99 = row['latency_p50_ms'], row['latency_p95_ms'], row['latency_p99_ms']
                synthetic = [p50] * 50 + [p95] * 45 + [p99] * 5
                latency_data.append(synthetic)
                labels.append(row['label'])
                colors.append('#2ecc71' if 'valkey' in row['backend'] else '#3498db')
            
            bp = ax.boxplot(latency_data, labels=labels, patch_artist=True)
            
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.6)
            
            ax.set_ylabel('Latency (ms)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'latency_distribution.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: latency_distribution.png")
        plt.close()
    
    def plot_cpu_usage(self):
        """Compare CPU usage"""
        # Check if CPU data is available
        if 'avg_cpu_user' not in self.df.columns:
            print("Warning: CPU usage data not available")
            return
        
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        scenarios = ['cold', 'warm', 'load']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario].sort_values('avg_cpu_user', ascending=False)
            
            colors = ['#2ecc71' if 'valkey' in b else '#3498db' for b in data['backend']]
            
            bars = ax.bar(range(len(data)), data['avg_cpu_user'], color=colors, alpha=0.8)
            ax.set_xticks(range(len(data)))
            ax.set_xticklabels(data['label'], rotation=45, ha='right', fontsize=8)
            ax.set_ylabel('CPU Usage (%)', fontsize=10)
            ax.set_title(f'{scenario.title()} Start', fontsize=12, fontweight='bold')
            ax.set_ylim(0, 100)
            ax.grid(axis='y', alpha=0.3)
            
            # Add value labels
            for bar, val in zip(bars, data['avg_cpu_user']):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                       f'{val:.1f}%', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'cpu_usage.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: cpu_usage.png")
        plt.close()
    
    def plot_scenario_comparison(self):
        """Compare same implementation across scenarios"""
        implementations = self.df['label'].unique()
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        axes = axes.flatten()
        
        # Throughput across scenarios
        ax = axes[0]
        for impl in implementations:
            data = self.df[self.df['label'] == impl].sort_values('scenario')
            scenarios = ['cold', 'warm', 'load']
            throughput = [data[data['scenario'] == s]['avg_throughput'].values[0] 
                         if len(data[data['scenario'] == s]) > 0 else 0 
                         for s in scenarios]
            ax.plot(scenarios, throughput, marker='o', label=impl.replace('\n', ' '), linewidth=2)
        
        ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
        ax.set_title('Throughput Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)
        
        # P95 latency across scenarios
        ax = axes[1]
        for impl in implementations:
            data = self.df[self.df['label'] == impl].sort_values('scenario')
            scenarios = ['cold', 'warm', 'load']
            latency = [data[data['scenario'] == s]['latency_p95_ms'].values[0] 
                      if len(data[data['scenario'] == s]) > 0 else 0 
                      for s in scenarios]
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '), linewidth=2)
        
        ax.set_ylabel('p95 Latency (ms)', fontsize=10)
        ax.set_title('p95 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)
        
        # P99 latency across scenarios
        ax = axes[2]
        for impl in implementations:
            data = self.df[self.df['label'] == impl].sort_values('scenario')
            scenarios = ['cold', 'warm', 'load']
            latency = [data[data['scenario'] == s]['latency_p99_ms'].values[0] 
                      if len(data[data['scenario'] == s]) > 0 else 0 
                      for s in scenarios]
            ax.plot(scenarios, latency, marker='o', label=impl.replace('\n', ' '), linewidth=2)
        
        ax.set_ylabel('p99 Latency (ms)', fontsize=10)
        ax.set_title('p99 Latency Across Scenarios', fontsize=12, fontweight='bold')
        ax.legend(fontsize=7, loc='best')
        ax.grid(alpha=0.3)
        
        # Hide unused subplot
        axes[3].axis('off')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'scenario_comparison.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: scenario_comparison.png")
        plt.close()
    
    def plot_valkey_vs_best_pg(self):
        """Direct comparison: Valkey vs Best PostgreSQL implementation"""
        fig, axes = plt.subplots(2, 3, figsize=(15, 8))
        scenarios = ['cold', 'warm', 'load']
        
        for idx, scenario in enumerate(scenarios):
            scenario_data = self.df[self.df['scenario'] == scenario]
            
            # Get Valkey data
            valkey_data = scenario_data[scenario_data['backend'] == 'valkey'].iloc[0]
            
            # Get best PG data (highest throughput)
            pg_data = scenario_data[scenario_data['backend'] == 'postgresql'].sort_values('avg_throughput', ascending=False).iloc[0]
            
            # Throughput comparison
            ax = axes[0, idx]
            bars = ax.bar(['Valkey', f"PG ({pg_data['queue_type']})"], 
                         [valkey_data['avg_throughput'], pg_data['avg_throughput']],
                         color=['#2ecc71', '#3498db'], alpha=0.8)
            ax.set_ylabel('Throughput (jobs/sec)', fontsize=10)
            ax.set_title(f'{scenario.title()} - Throughput', fontsize=11, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, height + 50,
                       f'{height:.0f}', ha='center', va='bottom', fontsize=9)
            
            # Latency comparison
            ax = axes[1, idx]
            x = np.arange(3)
            width = 0.35
            
            valkey_latencies = [valkey_data['latency_p50_ms'], valkey_data['latency_p95_ms'], valkey_data['latency_p99_ms']]
            pg_latencies = [pg_data['latency_p50_ms'], pg_data['latency_p95_ms'], pg_data['latency_p99_ms']]
            
            ax.bar(x - width/2, valkey_latencies, width, label='Valkey', color='#2ecc71', alpha=0.8)
            ax.bar(x + width/2, pg_latencies, width, label=f"PG ({pg_data['queue_type']})", color='#3498db', alpha=0.8)
            
            ax.set_ylabel('Latency (ms)', fontsize=10)
            ax.set_title(f'{scenario.title()} - Latency', fontsize=11, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(['p50', 'p95', 'p99'])
            ax.legend(fontsize=8)
            ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'valkey_vs_best_pg.png'), dpi=300, bbox_inches='tight')
        print(f"Saved: valkey_vs_best_pg.png")
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
        
        print("-" * 50)
        print(f"All graphs saved to: {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(description='Generate benchmark comparison graphs')
    parser.add_argument('--input', required=True,
                       help='Input CSV file (e.g., results/analysis/summary.csv)')
    parser.add_argument('--output', default='results/graphs',
                       help='Output directory for graphs')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        return
    
    generator = GraphGenerator(args.input, args.output)
    generator.generate_all()
    
    print("\nGraphs generated successfully!")
    print(f"View graphs in: {args.output}/")


if __name__ == '__main__':
    main()
