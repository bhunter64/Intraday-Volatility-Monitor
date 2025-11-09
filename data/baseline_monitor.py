# Calculating 30 min rolling std and finding alarm flags (Saanvi)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Loading data from CSV (SPY_Datafull.csv)
df = pd.read_csv('SPY_Datafull.csv', parse_dates=['timestamp']) # convert to Python datetime objects to sort chronologically
df = df.sort_values('timestamp')
df = df.reset_index(drop=True) # discard old index

# Calculating returns
df['returns'] = df['price'].pct_change()
# Printing first 10 or so return values to check
print(f"\nFirst 10 returns:")
print(df[['timestamp', 'price', 'returns']].head(10))

# Calculating 30 min rolling std
df['rolling_std'] = df['returns'].rolling(window=30, min_periods=30).std() # min_periods=30 to compute only after 30 values
# Printing first 10 or so rows of rolling std values to check
print(f"\nFirst 10 values of rolling std:")
print(f"\n{df[['timestamp', 'price', 'rolling_std']].head(10)}") # shows needs min 30 values to compute
# Printing first 10 real rolling std values to show that calculations happen
print(f"\nFirst 10 real values of rolling std (index 30:40):")
print(f"\n{df[['timestamp', 'price', 'rolling_std']].iloc[30:40]}\n") # shows NaN to rolling std

# Calculating threshold (Using 95th percentile to start)
# Can adjust threshold to find optimal sensitivity of alarm
threshold = df['rolling_std'].quantile(0.95)

# Alarm flag condition
df['alarm'] = df['rolling_std'] > threshold

# Calculating alarm rate
total_alarms = df['alarm'].sum()
print(f"Total alarms: {total_alarms}/{len(df)}\n")
print(f"Alarm rate (%): {100 * total_alarms / len(df):.2f}%\n")

# Grouping by day to make finding the quietest 3 and the busiest 3 easier
df['date'] = df['timestamp'].dt.date
daily_stats = df.groupby('date').agg({
    'alarm': 'sum', 
    'rolling_std': ['mean', 'max', 'std'], 
    'returns': 'std'
}).round(5)

daily_stats.columns = ['num_alarms', 'avg_rolling_std', 'max_rolling_std', 
                       'std_rolling_std', 'daily_return_std'
                       ]

sorted_by_alarms = daily_stats.sort_values('num_alarms')

# Getting 3 most quiet and 3 most busy days
print(f"\nThree quietest days:")
print(sorted_by_alarms.head(3))
print(f"\nThree busiest days:")
print(sorted_by_alarms.tail(3))


# Plotting price, rolling std, and alarm flags (Marco)