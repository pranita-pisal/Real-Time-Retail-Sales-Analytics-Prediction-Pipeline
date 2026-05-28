import pandas as pd

# 1. Load both CSVs
train_df = pd.read_csv('train.csv')
features_df = pd.read_csv('features.csv')

# 2. Merge them together based on the Store and Date
merged_df = pd.merge(train_df, features_df, on=['Store', 'Date', 'IsHoliday'], how='inner')

# 3. Drop the 'MarkDown' columns as we aren't using them in our ML model
columns_to_keep = ['Store', 'Dept', 'Date', 'Weekly_Sales', 'IsHoliday', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']
final_df = merged_df[columns_to_keep]

# 4. Save as Walmart.csv
final_df.to_csv('Walmart.csv', index=False)
print("Walmart.csv successfully created!")
