import requests
import pandas as pd
import time
import os
import urllib3
from datetime import datetime, timedelta

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_binance_data(symbol, days=1825):
    print(f"Fetching {symbol} from Binance...")
    base_url = "https://api.binance.com/api/v3/klines"
    
    # Calculate start time (milliseconds)
    end_time = int(time.time() * 1000)
    start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
    all_data = []
    current_start = start_time
    
    while current_start < end_time:
        params = {
            'symbol': symbol,
            'interval': '1d',
            'limit': 1000,
            'startTime': current_start,
            'endTime': end_time
        }
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(base_url, params=params, headers=headers, verify=False, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break
                
                all_data.extend(data)
                
                # Update start time for next batch (last close time + 1ms)
                # kline format: [Open time, Open, High, Low, Close, Volume, Close time, ...]
                last_close_time = data[-1][6]
                current_start = last_close_time + 1
                
                print(f"Fetched {len(data)} records. Total: {len(all_data)}")
                
                # Sleep to avoid rate limits
                time.sleep(0.5)
            else:
                print(f"Error: {response.status_code} - {response.text}")
                break
        except Exception as e:
            print(f"Exception: {e}")
            break
            
    # Process data
    if all_data:
        # Binance returns lists. We need: [0] Open time, [1] Open, [2] High, [3] Low, [4] Close, [5] Volume
        df = pd.DataFrame(all_data, columns=[
            'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 
            'close_time', 'quote_asset_volume', 'trades', 
            'taker_buy_base', 'taker_buy_quote', 'ignore'
        ])
        
        # Select relevant columns
        df = df[['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # Convert types
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = df[col].astype(float)
            
        # Add Adj Close (same as Close for crypto usually)
        df['Adj Close'] = df['Close']
        
        # Convert timestamp to date
        df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Reorder to match Yahoo Finance format
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        
        return df
    else:
        return None

if __name__ == "__main__":
    if not os.path.exists('Dataset'):
        os.makedirs('Dataset')

    # Download BTC
    df_btc = get_binance_data('BTCUSDT')
    if df_btc is not None:
        df_btc.to_csv('Dataset/btc_usd.csv', index=False)
        print(f"Saved BTC data. Shape: {df_btc.shape}")

    # Download ETH
    df_eth = get_binance_data('ETHUSDT')
    if df_eth is not None:
        df_eth.to_csv('Dataset/eth_usd.csv', index=False)
        print(f"Saved ETH data. Shape: {df_eth.shape}")
