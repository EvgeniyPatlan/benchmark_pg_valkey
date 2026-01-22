#!/usr/bin/env python3
"""
Analyze benchmark results
Processes metrics from both PostgreSQL and Valkey tests
"""
import argparse
import json
import os
import glob
from pathlib import Path
import pandas as pd
import numpy as np


class BenchmarkAnalyzer:
    def __init__(self, pg_results_dir=None, valkey_results_dir=None):
        self.pg_results_dir = pg_results_dir
        self.valkey_results_dir = valkey_results_dir
        self.results = {}
    
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
    
    def analyze_pg_queue(self, queue_type, scenario):
        """Analyze PostgreSQL queue results"""
        prefix = f"{queue_type}_{scenario}"
        base_path = Path(self.pg_results_dir) / prefix
        
        # Load metrics
        metrics_file = f"{base_path}_metrics.jsonl"
        if not os.path.exists(metrics_file):
            print(f"Warning: {metrics_file} not found")
            return None
        
        df = self.load_metrics_file(metrics_file)
        if df.empty:
            return None
        
        # Calculate statistics
        stats = {
            'queue_type': queue_type,
            'scenario': scenario,
            'backend': 'postgresql',
            
            # Throughput
            'total_jobs': df['jobs_processed'].iloc[-1] if len(df) > 0 else 0,
            'duration_sec': df['elapsed'].iloc[-1] if len(df) > 0 else 0,
            'avg_throughput': df['throughput'].mean(),
            'max_throughput': df['throughput'].max(),
            'min_throughput': df['throughput'].min(),
            
            # Latency (from final metrics)
            'latency_p50_ms': df['latency_p50'].iloc[-1] if len(df) > 0 else 0,
            'latency_p95_ms': df['latency_p95'].iloc[-1] if len(df) > 0 else 0,
            'latency_p99_ms': df['latency_p99'].iloc[-1] if len(df) > 0 else 0,
            'latency_avg_ms': df['latency_avg'].iloc[-1] if len(df) > 0 else 0,
            'latency_max_ms': df['latency_max'].iloc[-1] if len(df) > 0 else 0,
            
            # System metrics
            'system_metrics_file': f"{base_path}_system.csv",
            'db_stats_file': f"{base_path}_db_stats.csv",
        }
        
        # Load system metrics if available
        system_file = f"{base_path}_system.csv"
        if os.path.exists(system_file):
            sys_df = pd.read_csv(system_file)
            stats['avg_cpu_user'] = sys_df['cpu_user'].mean()
            stats['avg_cpu_system'] = sys_df['cpu_system'].mean()
            stats['avg_mem_used_pct'] = sys_df['mem_used_pct'].mean()
        
        return stats
    
    def analyze_valkey(self, scenario):
        """Analyze Valkey Streams results"""
        prefix = f"valkey_{scenario}"
        base_path = Path(self.valkey_results_dir) / prefix
        
        # Load metrics
        metrics_file = f"{base_path}_metrics.jsonl"
        if not os.path.exists(metrics_file):
            print(f"Warning: {metrics_file} not found")
            return None
        
        df = self.load_metrics_file(metrics_file)
        if df.empty:
            return None
        
        # Calculate statistics
        stats = {
            'queue_type': 'streams',
            'scenario': scenario,
            'backend': 'valkey',
            
            # Throughput
            'total_jobs': df['jobs_processed'].iloc[-1] if len(df) > 0 else 0,
            'duration_sec': df['elapsed'].iloc[-1] if len(df) > 0 else 0,
            'avg_throughput': df['throughput'].mean(),
            'max_throughput': df['throughput'].max(),
            'min_throughput': df['throughput'].min(),
            
            # Latency (from final metrics)
            'latency_p50_ms': df['latency_p50'].iloc[-1] if len(df) > 0 else 0,
            'latency_p95_ms': df['latency_p95'].iloc[-1] if len(df) > 0 else 0,
            'latency_p99_ms': df['latency_p99'].iloc[-1] if len(df) > 0 else 0,
            'latency_avg_ms': df['latency_avg'].iloc[-1] if len(df) > 0 else 0,
            'latency_max_ms': df['latency_max'].iloc[-1] if len(df) > 0 else 0,
            
            # System metrics
            'system_metrics_file': f"{base_path}_system.csv",
            'valkey_stats_file': f"{base_path}_valkey_stats.csv",
        }
        
        # Load system metrics if available
        system_file = f"{base_path}_system.csv"
        if os.path.exists(system_file):
            sys_df = pd.read_csv(system_file)
            stats['avg_cpu_user'] = sys_df['cpu_user'].mean()
            stats['avg_cpu_system'] = sys_df['cpu_system'].mean()
            stats['avg_mem_used_pct'] = sys_df['mem_used_pct'].mean()
        
        return stats
    
    def analyze_all(self):
        """Analyze all results"""
        all_results = []
        
        # PostgreSQL queues
        if self.pg_results_dir:
            queue_types = ['skip_locked', 'delete_returning', 'partitioned']
            scenarios = ['cold', 'warm', 'load']
            
            for queue_type in queue_types:
                for scenario in scenarios:
                    stats = self.analyze_pg_queue(queue_type, scenario)
                    if stats:
                        all_results.append(stats)
        
        # Valkey
        if self.valkey_results_dir:
            scenarios = ['cold', 'warm', 'load']
            
            for scenario in scenarios:
                stats = self.analyze_valkey(scenario)
                if stats:
                    all_results.append(stats)
        
        # Convert to DataFrame
        if all_results:
            self.results = pd.DataFrame(all_results)
            return self.results
        else:
            print("No results found!")
            return pd.DataFrame()
    
    def print_summary(self):
        """Print summary statistics"""
        if self.results.empty:
            print("No results to display")
            return
        
        print("\n" + "=" * 100)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 100)
        
        # Group by scenario
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
        
        # Comparison table
        print("\nQUICK COMPARISON (by scenario):")
        print("=" * 100)
        
        comparison = self.results.pivot_table(
            index=['backend', 'queue_type'],
            columns='scenario',
            values=['avg_throughput', 'latency_p95_ms'],
            aggfunc='first'
        )
        
        print(comparison.to_string())
        print("=" * 100)
    
    def save_results(self, output_dir='results/analysis'):
        """Save analysis results"""
        os.makedirs(output_dir, exist_ok=True)
        
        if not self.results.empty:
            # Save CSV
            csv_file = os.path.join(output_dir, 'summary.csv')
            self.results.to_csv(csv_file, index=False)
            print(f"\nSummary saved to: {csv_file}")
            
            # Save detailed JSON
            json_file = os.path.join(output_dir, 'summary.json')
            self.results.to_json(json_file, orient='records', indent=2)
            print(f"Detailed results saved to: {json_file}")


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
    
    print("\nNext step: Generate graphs with:")
    print(f"  python3 generate_graphs.py --input {args.output}/summary.csv")


if __name__ == '__main__':
    main()
