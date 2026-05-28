import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import os

print("Loading data...")
# Load a subset of data to train quickly
df = pd.read_csv('data/Walmart.csv')

# Feature Engineering
print("Engineering features...")
df['Date'] = pd.to_datetime(df['Date'])
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Week'] = df['Date'].dt.isocalendar().week
df['IsHoliday'] = df['IsHoliday'].astype(int)

features = ['Store', 'Dept', 'Week', 'Month', 'Year', 'IsHoliday', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']
target = 'Weekly_Sales'

X = df[features]
y = df[target]

print("Training Random Forest model (this may take a minute)...")
# Using a small number of estimators for speed since this is a local prototype
model = RandomForestRegressor(n_estimators=10, random_state=42, n_jobs=-1)
model.fit(X, y)

print("Saving model...")
os.makedirs('models', exist_ok=True)
joblib.dump(model, 'models/sales_model.pkl')
print("Model successfully saved to models/sales_model.pkl!")
