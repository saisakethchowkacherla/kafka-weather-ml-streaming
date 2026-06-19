# Kafka Weather ML Streaming

## Project Overview
Real-time weather prediction system using Apache Kafka and Faust Streams API.

## Dataset
weatherHistory.csv

## Technologies
- Python
- Apache Kafka
- Faust
- Scikit-learn
- Docker

## Components
1. weather_producer.py
2. stream_processor.py
3. prediction_consumer.py

## Setup

pip install -r requirements.txt

python train_model.py

faust -A stream_processor worker -l info

python prediction_consumer.py

python weather_producer.py

## Model
Linear Regression

Metric: MAE

## Video Demo
Video Demo:
https://drive.google.com/file/d/1NrWDLOQdcp7LACZcKv7kBYyYTzssuBN0/view?usp=drivesdk
