"""
Evaluate Multi-Coin Prediction Model
Calculate IC, Rank IC, and Top-N accuracy
"""
import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import spearmanr

QLIB_DATA_DIR = Path("qlib_data")

def evaluate_model():
    print("🔍 Evaluating Multi-Coin Prediction Model...\n")
    
    # Load predictions
    pred_df = pd.read_csv(QLIB_DATA_DIR / "multi_coin_pred.csv")
    pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
    
    # Load actual data
    data_df = pd.read_csv(QLIB_DATA_DIR / "multi_coin_features.csv")
    data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    
    # Merge
    # Include sentiment columns if they exist
    cols_to_merge = ['datetime', 'instrument', 'future_24h_ret']
    optional_cols = ['funding_rate', 'oi_change', 'funding_rate_zscore']
    for col in optional_cols:
        if col in data_df.columns:
            cols_to_merge.append(col)
            
    merged = pd.merge(
        pred_df,
        data_df[cols_to_merge],
        on=['datetime', 'instrument'],
        how='inner'
    )
    
    print(f"📊 Merged {len(merged)} predictions\n")
    
    # Calculate IC (Information Coefficient)
    ic_values = []
    rank_ic_values = []
    
    for ts in merged['datetime'].unique():
        subset = merged[merged['datetime'] == ts]
        if len(subset) >= 3:  # Need at least 3 coins
            # Pearson IC
            ic = subset['score'].corr(subset['future_24h_ret'])
            if not np.isnan(ic):
                ic_values.append(ic)
            
            # Spearman Rank IC
            rank_ic, _ = spearmanr(subset['score'], subset['future_24h_ret'])
            if not np.isnan(rank_ic):
                rank_ic_values.append(rank_ic)
    
    print(f"📈 Information Coefficient (IC):")
    print(f"   Mean IC: {np.mean(ic_values):.4f}")
    print(f"   Median IC: {np.median(ic_values):.4f}")
    print(f"   Std IC: {np.std(ic_values):.4f}")
    print(f"   IC > 0: {(np.array(ic_values) > 0).sum()}/{len(ic_values)} ({(np.array(ic_values) > 0).sum()/len(ic_values)*100:.1f}%)")
    
    print(f"\n📊 Rank IC (Spearman):")
    print(f"   Mean Rank IC: {np.mean(rank_ic_values):.4f}")
    print(f"   Median Rank IC: {np.median(rank_ic_values):.4f}")
    print(f"   Std Rank IC: {np.std(rank_ic_values):.4f}")
    
    # Sentiment Factor Analysis
    print(f"\n📊 Sentiment Factor Analysis:")
    if 'funding_rate' in merged.columns:
        fr_ic_list = []
        for ts in merged['datetime'].unique():
            subset = merged[merged['datetime'] == ts]
            if len(subset) >= 3 and subset['funding_rate'].std() > 0:
                fr_ic_list.append(subset['funding_rate'].corr(subset['future_24h_ret']))
        print(f"   Funding Rate IC: {np.mean(fr_ic_list):.4f}")
        
    if 'oi_change' in merged.columns:
        oi_ic_list = []
        for ts in merged['datetime'].unique():
            subset = merged[merged['datetime'] == ts]
            if len(subset) >= 3 and subset['oi_change'].std() > 0:
                oi_ic_list.append(subset['oi_change'].corr(subset['future_24h_ret']))
        print(f"   OI Change IC: {np.mean(oi_ic_list):.4f}")
    
    # Top-N accuracy
    top1_correct = 0
    top2_correct = 0
    total = 0
    
    for ts in merged['datetime'].unique():
        subset = merged[merged['datetime'] == ts].copy()
        if len(subset) >= 3:
            # Predicted top
            subset = subset.sort_values('score', ascending=False)
            pred_top1 = subset.iloc[0]['instrument']
            pred_top2 = set(subset.iloc[:2]['instrument'])
            
            # Actual top
            subset = subset.sort_values('future_24h_ret', ascending=False)
            actual_top1 = subset.iloc[0]['instrument']
            actual_top2 = set(subset.iloc[:2]['instrument'])
            
            if pred_top1 == actual_top1:
                top1_correct += 1
            if len(pred_top2 & actual_top2) > 0:
                top2_correct += 1
            total += 1
    
    print(f"\n🎯 Top-N Accuracy:")
    print(f"   Top-1 Accuracy: {top1_correct/total*100:.2f}% ({top1_correct}/{total})")
    print(f"   Top-2 Hit Rate: {top2_correct/total*100:.2f}% ({top2_correct}/{total})")
    
    # Sample predictions
    print(f"\n📋 Sample Predictions (Last 5 timestamps):")
    display_cols = ['instrument', 'score', 'future_24h_ret']
    if 'funding_rate' in merged.columns:
        display_cols.append('funding_rate')
    if 'oi_change' in merged.columns:
        display_cols.append('oi_change')
        
    for ts in sorted(merged['datetime'].unique())[-5:]:
        subset = merged[merged['datetime'] == ts].copy()
        subset = subset.sort_values('score', ascending=False)
        print(f"\n   {ts}:")
        print(subset[display_cols].to_string(index=False))
    
    # Portfolio simulation
    print(f"\n💰 Portfolio Simulation Strategies:")
    
    # 1. Long-Only Top-1 (Original)
    long_returns = []
    
    # 2. Long-Short (Long Top-1, Short Bottom-1)
    ls_returns = []
    
    # 3. Market Timing (Long Top-1 only if predicted return > threshold)
    timing_returns = []
    threshold = 0.005  # 0.5% threshold
    
    for ts in sorted(merged['datetime'].unique()):
        subset = merged[merged['datetime'] == ts]
        if len(subset) >= 3:
            # Sort by score
            subset = subset.sort_values('score', ascending=False)
            
            # Top-1 coin
            top_coin = subset.iloc[0]['instrument']
            top_ret = subset.iloc[0]['future_24h_ret']
            top_score = subset.iloc[0]['score']
            
            # Bottom-1 coin
            bot_coin = subset.iloc[-1]['instrument']
            bot_ret = subset.iloc[-1]['future_24h_ret']
            
            # 1. Long-Only
            long_returns.append(top_ret)
            
            # 2. Long-Short
            ls_ret = (top_ret - bot_ret) / 2  # No leverage, 50% long, 50% short
            ls_returns.append(ls_ret)
            
            # 3. Market Timing
            if top_score > 0.5: # Simple threshold, can be optimized
                 timing_returns.append(top_ret)
            else:
                 timing_returns.append(0) # Cash
    
    def print_stats(name, returns):
        cum_ret = (1 + pd.Series(returns)).cumprod().iloc[-1] - 1
        sharpe = np.mean(returns) / np.std(returns) * np.sqrt(365/24*6) if np.std(returns) > 0 else 0
        print(f"\n   {name}:")
        print(f"      Cumulative Return: {cum_ret*100:.2f}%")
        print(f"      Sharpe Ratio: {sharpe:.2f}")
        print(f"      Avg Return: {np.mean(returns)*100:.4f}%")

    print_stats("Long-Only Top-1", long_returns)
    print_stats("Long-Short (Top-Bottom)", ls_returns)
    print_stats("Market Timing (Score > 0.5)", timing_returns)
    
    # Equal weight comparison
    equal_weight_returns = merged.groupby('datetime')['future_24h_ret'].mean()
    print_stats("Equal Weight (Benchmark)", equal_weight_returns)

if __name__ == "__main__":
    evaluate_model()
