import sqlite3
import pandas as pd
import argparse

def scan_market(db_path="data/ah_prices.sqlite3", min_volume=500, min_listings=10, min_price=5.0):
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT observed_at, item_name, weighted_avg_unit_price, total_quantity, listing_count
    FROM observations
    WHERE source = 'commodity:region';
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} total rows. Processing timestamps...")
    
    df['observed_at'] = pd.to_datetime(df['observed_at'], utc = True)
    df['local_time'] = df['observed_at'].dt.tz_convert('America/Denver')
    df['hour'] = df['local_time'].dt.hour
    df['day_name'] = df['local_time'].dt.day_name()
    df['price_gold'] = df['weighted_avg_unit_price'] / 10000.0
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    results = []
    
    for item_name, group in df.groupby('item_name'):
        avg_qty = group['total_quantity'].mean()
        avg_listings = group['listing_count'].mean()
        med_price = group['price_gold'].median()
        
        if len(group) < 150 or avg_qty < min_volume or avg_listings < min_listings or med_price < min_price:
            continue
        
        pivot = group.pivot_table(index='day_name', columns='hour', values = 'price_gold', aggfunc='median').reindex(day_order)
        
        min_val = pivot.min().min()
        max_val = pivot.max().max()
        
        # Outlier Guard: Filter out items with 4x max/min spikes (troll listings)
        if min_val <= 0 or pd.isna(min_val) or pd.isna(max_val) or (max_val / min_val > 4.0):
            continue
        min_pos = [(d,h) for d in day_order for h in range(24) if pivot.loc[d,h] == min_val][0]
        max_pos = [(d,h) for d in day_order for h in range(24) if pivot.loc[d,h] == max_val][0]
        
        net_payout = max_val * 0.95
        net_profit_gold = net_payout - min_val
        net_roi_pct = (net_profit_gold/min_val) * 100.0
        
        if net_roi_pct >= 15.0:
            results.append({
                'Item Name': item_name,
                'Avg Volume': int(group['total_quantity'].mean()),
                'Buy Window': f"{min_pos[0][:3]} {min_pos[1]:02d}:00",
                'Buy Price (g)' : round(min_val, 1),
                'Sell Window' : f"{max_pos[0][:3]} {max_pos[1]:02d}:00",
                'Sell Price (g)': round(max_val,1),
                'Net Profit (g)': round(net_profit_gold,1),
                'Net ROI %': round(net_roi_pct,1)
            })
    results_df = pd.DataFrame(results)
    
    print("\n=== Top 10 High Volume Arbitrage Opportunities (by Gold Profit) ===")
    print(results_df.sort_values('Net Profit (g)', ascending=False).head(10).to_string(index=False))
    
if __name__=='__main__':
    scan_market()
    