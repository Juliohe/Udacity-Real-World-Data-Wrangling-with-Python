import pandas as pd
import numpy as np

# Load the clean ETH dataset
file_path = 'Dataset/eth_usd.csv'
try:
    df = pd.read_csv(file_path)
    
    # Check if we are working with clean data (has High and Low) or already dirty data
    # Reset to base clean state if needed (re-reading raw if we had a backup would be better, 
    # but here we assume the file on disk might be clean or dirty. 
    # If it's already dirty (has High_Low), we should probably recreate it from logic if possible, 
    # but simplest is to just proceed if High/Low exist.
    
    if 'High' in df.columns and 'Low' in df.columns:
        print(f"Original ETH shape: {df.shape}")

        # 1. Inject Missing Values (NaN) in Close
        random_indices = np.random.choice(df.index, 10, replace=False)
        df.loc[random_indices, 'Close'] = np.nan
        print(f"Injected NaN values. Nulls in Close: {df['Close'].isnull().sum()}")

        # 2. Inject Duplicates
        duplicates = df.sample(n=5)
        df = pd.concat([df, duplicates], ignore_index=True)
        print(f"Injected 5 duplicates. New shape: {df.shape}")

        # 3. Inject Tidiness Issue: Combine High and Low into 'High_Low'
        # Format: "High/Low"
        df['High_Low'] = df['High'].astype(str) + '/' + df['Low'].astype(str)
        
        # Drop the original High and Low columns
        df.drop(columns=['High', 'Low'], inplace=True)
        print("Injected Tidiness Issue: Merged 'High' and 'Low' into 'High_Low' column.")
        
        # Save the dirty dataset back to the same file
        df.to_csv(file_path, index=False)
        print("Dataset 2 (ETH) has been successfully corrupted with Missing Values, Duplicates, and Untidy Columns.")
    
    elif 'High_Low' in df.columns:
         print("Dataset already contains 'High_Low' column. Skipping corruption to avoid double-processing.")
         
    else:
        print("Unexpected columns found. Could not inject specific tidiness issue.")

except FileNotFoundError:
    print(f"Error: {file_path} not found. Please run the notebook first to download the data.")
