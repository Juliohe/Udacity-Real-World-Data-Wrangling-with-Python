import pandas as pd
import numpy as np

# Load the clean BTC dataset
file_path = 'Dataset/btc_usd.csv'
df = pd.read_csv(file_path)

print(f"Original shape: {df.shape}")

# 1. Inject Missing Values (NaN)
# Set 15 random 'Close' values to NaN
random_indices_close = np.random.choice(df.index, 15, replace=False)
df.loc[random_indices_close, 'Close'] = np.nan

# Set 15 random 'Volume' values to NaN
random_indices_vol = np.random.choice(df.index, 15, replace=False)
df.loc[random_indices_vol, 'Volume'] = np.nan

print(f"Injected NaN values. Nulls in Close: {df['Close'].isnull().sum()}, Nulls in Volume: {df['Volume'].isnull().sum()}")

# 2. Inject Duplicates
# Sample 5 random rows and append them to the dataframe
duplicates = df.sample(n=5)
df = pd.concat([df, duplicates], ignore_index=True) # ignore_index=True to simulate re-indexing after append, or False to keep indices? 
# Usually dirty data might have reset indices or messy indices. Let's keep it simple.

print(f"Injected 5 duplicates. New shape: {df.shape}")

# Save the dirty dataset back to the same file
df.to_csv(file_path, index=False)
print("Dataset 1 (BTC) has been successfully corrupted with missing values and duplicates.")
