"""
Multi-Layered Anomaly Detection for Checkout Data
Implements 4 detection layers:
1. Statistical Z-Score
2. Volume-Aware Rules
3. Drop-to-Zero / Spike Detection
4. Rate-of-Change (Slope) Detection
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Configuration
sns.set_theme(style='whitegrid')
plt.rcParams['figure.figsize'] = (18, 7)


def detect_anomalies(df, z_threshold=2.0, spike_multiplier=3.0, min_volume_for_outage=5):
    """
    Multi-layered anomaly detection for hourly checkout data.
    
    Parameters:
    -----------
    df : DataFrame with columns [hour, today, yesterday, same_day_last_week, avg_last_week, avg_last_month]
    z_threshold : float, Z-score threshold for statistical anomaly (default 2.0 = ~95% confidence)
    spike_multiplier : float, multiplier for spike detection (default 3.0x max historical)
    min_volume_for_outage : float, minimum expected volume to consider a zero as an outage
    
    Returns:
    --------
    DataFrame with anomaly flags and scores
    """
    result = df.copy()
    
    # ==========================================
    # LAYER 1: Statistical Z-Score
    # ==========================================
    ref_cols = ['yesterday', 'same_day_last_week', 'avg_last_week', 'avg_last_month']
    
    # Expected value: weighted mean
    result['expected'] = (
        0.30 * result['avg_last_week'] +
        0.25 * result['avg_last_month'] +
        0.25 * result['same_day_last_week'] +
        0.20 * result['yesterday']
    )
    
    # Estimate std from reference points
    result['estimated_std'] = result[ref_cols].std(axis=1)
    
    # Floor the std to avoid division by zero
    result['estimated_std'] = result[['estimated_std']].apply(
        lambda row: max(row['estimated_std'], 1.0, 0.3 * result.loc[row.name, 'expected']),
        axis=1
    )
    
    result['z_score'] = (result['today'] - result['expected']) / result['estimated_std']
    result['is_zscore_anomaly'] = result['z_score'].abs() > z_threshold
    
    # ==========================================
    # LAYER 2: Volume-Aware Rules
    # ==========================================
    result['is_low_traffic'] = result['avg_last_month'] < 5
    result['abs_deviation'] = (result['today'] - result['expected']).abs()
    result['is_volume_anomaly'] = False
    
    # Low traffic
    low_mask = result['is_low_traffic']
    result.loc[low_mask, 'is_volume_anomaly'] = (
        (result.loc[low_mask, 'abs_deviation'] > 10) & 
        (result.loc[low_mask, 'z_score'].abs() > z_threshold)
    )
    
    # High traffic
    high_mask = ~result['is_low_traffic']
    pct_dev = ((result['today'] - result['expected']) / result['expected'].replace(0, np.nan)).abs()
    result.loc[high_mask, 'is_volume_anomaly'] = (
        (pct_dev[high_mask] > 1.0) &
        (result.loc[high_mask, 'z_score'].abs() > z_threshold * 0.8)
    )
    
    # ==========================================
    # LAYER 3: Drop-to-Zero / Spike Detection
    # ==========================================
    result['is_outage'] = (result['today'] == 0) & (result['expected'] >= min_volume_for_outage)
    
    result['historical_max'] = result[ref_cols].max(axis=1)
    result['is_spike'] = (
        (result['today'] > spike_multiplier * result['historical_max']) & 
        (result['today'] > 10)
    )
    
    # ==========================================
    # LAYER 4: Rate-of-Change (Slope) Detection
    # ==========================================
    # Detect abnormal hour-over-hour drops that may precede outages
    
    # Actual hour-over-hour change
    result['actual_change'] = result['today'].diff()
    
    # Expected hour-over-hour change
    result['expected_change'] = result['expected'].diff()
    
    # Change deviation
    result['change_deviation'] = result['actual_change'] - result['expected_change']
    
    # Estimate variability of transitions
    ref_diffs = result[ref_cols].apply(lambda col: col.diff())
    result['change_std'] = ref_diffs.std(axis=1).clip(lower=3.0)
    
    # Z-score of rate of change
    result['change_z_score'] = result['change_deviation'] / result['change_std']
    
    # Flag significant DROPS only
    result['is_slope_anomaly'] = (
        (result['change_z_score'] < -z_threshold) &  
        (result['actual_change'] < -10)
    )
    
    # ==========================================
    # COMBINED SCORING
    # ==========================================
    result['anomaly_score'] = (
        result['is_zscore_anomaly'].astype(int) + 
        result['is_volume_anomaly'].astype(int) + 
        result['is_outage'].astype(int) * 2 +
        result['is_spike'].astype(int) * 2 +
        result['is_slope_anomaly'].astype(int)
    )
    
    # Severity classification
    def classify_severity(row):
        if row['is_outage']:
            return 'CRITICAL'
        if row['is_spike']:
            return 'WARNING'
        if row['anomaly_score'] >= 2:
            return 'WARNING'
        if row['anomaly_score'] == 1:
            return 'INFO'
        return 'NORMAL'
    
    result['severity'] = result.apply(classify_severity, axis=1)
    result['is_anomalous'] = result['anomaly_score'] > 0
    
    return result


def visualize_anomalies(results, save_path=None):
    """
    Visualize anomaly detection results for multiple checkouts.
    
    Parameters:
    -----------
    results : dict of {checkout_name: anomaly_results_df}
    save_path : optional path to save the figure
    """
    fig, axes = plt.subplots(1, len(results), figsize=(18, 7), sharey=True)
    if len(results) == 1:
        axes = [axes]
    
    for ax, (name, r) in zip(axes, results.items()):
        hours = r['hour']
        
        # Expected value and confidence bands
        expected = r['expected']
        std = r['estimated_std']
        
        # Plot confidence bands
        ax.fill_between(hours, (expected - 2*std).clip(lower=0), expected + 2*std, 
                        alpha=0.1, color='blue', label='±2σ band (normal range)')
        ax.fill_between(hours, (expected - std).clip(lower=0), expected + std, 
                        alpha=0.15, color='blue', label='±1σ band')
        
        # Plot expected and today
        ax.plot(hours, expected, '--', color='navy', linewidth=1.5, label='Expected', alpha=0.7)
        ax.plot(hours, r['today'], 'o-', color='#333333', linewidth=2, markersize=5, label='Today', zorder=4)
        
        # Mark anomalies
        anomalies = r[r['is_anomalous']]
        for _, row in anomalies.iterrows():
            color = {'CRITICAL': '#ff0000', 'WARNING': '#ff8800', 'INFO': '#cccc00'}.get(row['severity'], 'gray')
            marker = {'CRITICAL': 'X', 'WARNING': 'D', 'INFO': 'o'}.get(row['severity'], 'o')
            ax.scatter(row['hour'], row['today'], color=color, s=150, marker=marker, 
                       edgecolors='black', linewidths=1.5, zorder=5)
            ax.annotate(row['severity'], (row['hour'], row['today']), 
                        textcoords='offset points', xytext=(5, 10), fontsize=7,
                        color=color, fontweight='bold')
        
        ax.set_title(f'{name.replace("_", " ").title()}', fontsize=14, fontweight='bold')
        ax.set_xlabel('Hour of Day')
        ax.set_xticks(range(0, 24, 2))
        ax.legend(fontsize=8, loc='upper left')
        ax.grid(True, alpha=0.3)
    
    axes[0].set_ylabel('Transaction Count')
    fig.suptitle('4-Layer Anomaly Detection: Today vs Expected Range (±2σ)', 
                 fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f'Saved visualization to {save_path}')
    
    plt.show()


def main():
    """Main execution: load data, detect anomalies, visualize results."""
    data_dir = Path('../sample_data/checkout')
    
    # Load checkout data
    checkouts = {}
    for file_path in sorted(data_dir.glob('checkout_*.csv')):
        name = file_path.stem
        df = pd.read_csv(file_path)
        df['hour'] = df['time'].str.replace('h', '').astype(int)
        checkouts[name] = df
        print(f'Loaded {name}: {len(df)} hours')
    
    # Detect anomalies
    results = {}
    print('\n' + '='*80)
    print('4-LAYER ANOMALY DETECTION RESULTS')
    print('='*80)
    
    for name, df in checkouts.items():
        results[name] = detect_anomalies(df)
        
        r = results[name]
        n_anomalies = r['is_anomalous'].sum()
        n_total = len(r)
        
        print(f'\n=== {name} ===')
        print(f'Anomalous hours: {n_anomalies}/{n_total} ({n_anomalies/n_total*100:.0f}%)')
        print(f'  - Z-score anomalies: {r["is_zscore_anomaly"].sum()}')
        print(f'  - Volume anomalies:  {r["is_volume_anomaly"].sum()}')
        print(f'  - Outages detected:  {r["is_outage"].sum()}')
        print(f'  - Spikes detected:   {r["is_spike"].sum()}')
        print(f'  - Slope anomalies:   {r["is_slope_anomaly"].sum()}')
        print(f'\nSeverity distribution:')
        print(r['severity'].value_counts().to_string())
        
        # Show details of anomalies
        anomalies = r[r['is_anomalous']].sort_values('anomaly_score', ascending=False)
        if len(anomalies) > 0:
            print(f'\nDetailed anomalies:')
            for _, row in anomalies.iterrows():
                flags = []
                if row['is_outage']: flags.append('OUTAGE')
                if row['is_spike']: flags.append('SPIKE')
                if row['is_zscore_anomaly']: flags.append('Z-SCORE')
                if row['is_volume_anomaly']: flags.append('VOLUME')
                if row['is_slope_anomaly']: flags.append('SLOPE')
                print(f"  {row['time']} | {row['severity']:>8} | "
                      f"today={row['today']:>3.0f} expected={row['expected']:>5.1f} "
                      f"Z={row['z_score']:+.1f} | {', '.join(flags)}")
    
    # Visualize
    print('\n' + '='*80)
    print('Generating visualization...')
    visualize_anomalies(results, save_path='../report/images/anomaly_checkout.png')
    
    # Export results to CSV
    for name, r in results.items():
        output_path = f'anomaly_results_{name}.csv'
        r.to_csv(output_path, index=False)
        print(f'Exported {name} results to {output_path}')


if __name__ == '__main__':
    main()
