import pandas as pd
import numpy as np

# Load the clean ETH dataset
file_path = 'Dataset/eth_usd.csv'
try:
    df = pd.read_csv(file_path)
    print(f"Original ETH shape: {df.shape}")

    # 1. Inject Missing Values (NaN)
    # Set 10 random 'Close' values to NaN
    random_indices = np.random.choice(df.index, 10, replace=False)
    df.loc[random_indices, 'Close'] = np.nan
    print(f"Injected NaN values. Nulls in Close: {df['Close'].isnull().sum()}")

    # 2. Inject Duplicates
    # Sample 5 random rows and append them to the dataframe
    duplicates = df.sample(n=5)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    print(f"Injected 5 duplicates. New shape: {df.shape}")

    # Save the dirty dataset back to the same file
    df.to_csv(file_path, index=False)
    print("Dataset 2 (ETH) has been successfully corrupted with missing values and duplicates.")

except FileNotFoundError:
    print(f"Error: {file_path} not found. Please run the notebook first to download the data.")
