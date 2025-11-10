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
def plot_daily_volatility(target_date, data_df, alarm_threshold):
    """
    Plots the price, rolling standard deviation, and alarm flags for a given date.
    Parameters:
        target_date (str): The date to plot in 'YYYY-MM-DD'
        data_df (pd.DataFrame): The DataFrame containing the data.
        alarm_threshold (float): The threshold for alarm flags.
    Uses a dual-axis plot to show price and standard deviation on different scales.
    """
    print(f"\nPlotting data for {target_date}...\n")
    # Filter data for the target date
    day_df = data_df[data_df['date'] == target_date].copy()

    #create the column for alarm markers
    day_df['alarm_point'] = np.where(day_df['alarm'], day_df['price'], np.nan)

    #set up the plot
    fig, ax1 = plt.subplots(figsize=(14, 7))

    #create ax2, a second y-axis that shares the same x-axis
    ax2 = ax1.twinx()

    #plot price on ax1
    ax1.plot(day_df['timestamp'], day_df['price'], color='blue', label='price')
    ax1.scatter(day_df['timestamp'], day_df['alarm_point'], color='red', marker='x', s=100, label='Alarm Flag')

    #plot rolling std on ax2
    ax2.plot(day_df['timestamp'], day_df['rolling_std'], color='green', label='30min Rolling Std')
    ax2.axhline(alarm_threshold, color='red', linestyle='--', label='Alarm Threshold')

    #set labels
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Price', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')

    ax2.set_ylabel('30min Rolling Std', color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    #set title
    plt.title(f'Intraday Volatility Monitoring for {target_date}')

    #combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

    fig.tight_layout()
    plt.show()

#get the lists of quiet days and busy days
quiet_days = sorted_by_alarms.head(3).index
busy_days = sorted_by_alarms.tail(3).index

#plot quiet days
print("\n--- Plotting 3 Quietest Days ---")
for day in quiet_days:
    plot_daily_volatility(day, df, threshold)

#plot busy days
print("\n--- Plotting 3 Busiest Days ---")
for day in busy_days:
    plot_daily_volatility(day, df, threshold)