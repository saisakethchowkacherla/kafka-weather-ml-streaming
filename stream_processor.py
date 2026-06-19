import faust
import joblib

# Load trained ML model
model = joblib.load("weather_model.pkl")

# Create Faust app
app = faust.App(
    'weather-stream-app',
    broker='kafka://localhost:9092',
    value_serializer='json'
)

# Input topic
weather_topic = app.topic('weather-data')

# Output topic
prediction_topic = app.topic('predictions')

# Stream processor
@app.agent(weather_topic)
async def process_weather(stream):

    async for event in stream:

        # Prepare data for prediction
        features = [[
            event['Humidity'],
            event['WindSpeed'],
            event['Pressure']
        ]]

        # Predict temperature
        prediction = model.predict(features)[0]

        result = {
            "Humidity": event['Humidity'],
            "WindSpeed": event['WindSpeed'],
            "Pressure": event['Pressure'],
            "ActualTemperature": event['ActualTemperature'],
            "PredictedTemperature": float(round(prediction, 2))
        }

        # Print processor output
        print(f"\nProcessed: {result}")

        # Send prediction to output topic
        await prediction_topic.send(value=result)

# Start Faust app
if __name__ == '__main__':
    app.main()