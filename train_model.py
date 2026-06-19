import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import joblib

# Load dataset
df = pd.read_csv("weatherHistory.csv")

print("Dataset Loaded Successfully!")
print(df.head())

# Select useful columns
df = df[[
    'Temperature (C)',
    'Humidity',
    'Wind Speed (km/h)',
    'Pressure (millibars)'
]]

# Remove missing values
df = df.dropna()

# Features (inputs)
X = df[['Humidity', 'Wind Speed (km/h)', 'Pressure (millibars)']]

# Target (prediction)
y = df['Temperature (C)']

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model = LinearRegression()

model.fit(X_train, y_train)

# Predictions
predictions = model.predict(X_test)

# Evaluate
error = mean_absolute_error(y_test, predictions)

print(f"\nModel trained successfully!")
print(f"Mean Absolute Error: {error}")

# Save model
joblib.dump(model, "weather_model.pkl")

print("\nModel saved as weather_model.pkl")