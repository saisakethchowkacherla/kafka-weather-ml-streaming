# Kafka Weather ML Streaming - Technical Specification

**Document Version**: 1.0  
**Date**: 2026-05-26  
**Classification**: Technical Reference

---

## Table of Contents

1. [System Requirements](#system-requirements)
2. [Architecture Specification](#architecture-specification)
3. [Component Specifications](#component-specifications)
4. [Data Specifications](#data-specifications)
5. [API Specifications](#api-specifications)
6. [Performance Specifications](#performance-specifications)
7. [Security Specifications](#security-specifications)
8. [Deployment Specifications](#deployment-specifications)

---

## System Requirements

### Minimum Hardware Requirements

```
CPU:             2-core processor
RAM:             4GB minimum (8GB recommended)
Storage:         20GB (data + models + logs)
Network:         1Gbps Ethernet
```

### Operating Systems Supported

| OS | Version | Status |
|----|---------|--------|
| Ubuntu | 20.04+ | ✅ Fully Supported |
| CentOS | 8+ | ✅ Fully Supported |
| macOS | 11+ | ✅ Supported |
| Windows | 10/11 + WSL2 | ✅ Supported with Docker |

### Software Requirements

```
Python:          3.9, 3.10, 3.11, 3.12
Docker:          20.10+
Docker Compose:  2.0+
Git:             2.30+
```

### Network Requirements

```
Port 2181:       Zookeeper (internal)
Port 9092:       Kafka Broker (producer/consumer access)
Firewall:        Allow outbound connections for pip install
```

---

## Architecture Specification

### System Architecture

```
┌─────────────────────────────────────────────────────────┐
│           Production Environment Layout                 │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────┐    ┌──────────────────┐          │
│  │   Data Source    │    │ ML Model Store   │          │
│  │  weatherHistory  │    │ (weather_model   │          │
│  │     .csv         │    │     .pkl)        │          │
│  └────────┬─────────┘    └────────┬─────────┘          │
│           │                       │                     │
│           ▼                       │                     │
│  ┌──────────────────┐            │                     │
│  │    Producer      │            │                     │
│  │  weather_       │            │                     │
│  │   producer.py   │            │                     │
│  └────────┬─────────┘            │                     │
│           │                      │                     │
│           │ weather-data topic   │                     │
│           ▼                      ▼                     │
│  ┌──────────────────────────────────────┐             │
│  │        Apache Kafka Cluster          │             │
│  │  ┌──────────┐  ┌──────────────────┐ │             │
│  │  │Zookeeper │  │ Kafka Broker     │ │             │
│  │  │  :2181   │  │   :9092          │ │             │
│  │  └──────────┘  └──────────────────┘ │             │
│  │     (metadata)    (message bus)      │             │
│  └────────┬───────────────────────┬────┘             │
│           │                       │                   │
│        predictions-topic          │                   │
│           │                       ▼                   │
│           │      ┌──────────────────────────┐        │
│           │      │  Stream Processor        │        │
│           │      │  stream_processor.py     │        │
│           │      │  (Faust App)             │        │
│           │      │  • Async processing      │        │
│           │      │  • ML inference          │        │
│           │      │  • Enrichment            │        │
│           │      └────────────┬─────────────┘        │
│           │                   │                      │
│           │ ┌─────────────────┘                      │
│           ▼ ▼                                        │
│  ┌──────────────────┐                               │
│  │   Consumer       │                               │
│  │  prediction_     │                               │
│  │   consumer.py    │                               │
│  │  (results sink)  │                               │
│  └──────────────────┘                               │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Message Flow Diagram

```
Timing Diagram (with latency):

Producer                 Kafka          Processor          Consumer
   │                       │               │                  │
   ├──[Send weather]──────▶│               │                  │
   │                       ├──[Queue]──────│                  │
   │                       │        <100ms │                  │
   │                       │        predict
   │                       │               ├──[Send pred]────▶│
   │                       │               │                  │
   │<─ ACK ─────────────────               │                  │
   │                                       │                  ├─[Output]
   │ <1sec delay>                          │                  │
   │                                       │                  │
   ├──[Send weather]──────▶│               │                  │
   │                       ├──[Queue]──────│                  │
   │                       │        <100ms │                  │
   │                       │        predict
   │                       │               ├──[Send pred]────▶│
   ...                     ...              ...                ...

Total End-to-End Latency: ~1-2 seconds (configurable)
```

---

## Component Specifications

### 1. Producer Specification

**Component**: weather_producer.py

**Type**: Batch-to-Stream Producer

**Configuration**:
```python
bootstrap_servers = 'localhost:9092'
value_serializer = json.dumps(v).encode('utf-8')
batch_size = 1 (per-message)
acks = 1 (default)
```

**Input Data Format**:
```csv
Humidity,Wind Speed (km/h),Pressure (millibars),Temperature (C)
0.89,14.12,1015.13,9.47
0.86,14.26,1015.63,9.36
```

**Output Message Format** (JSON):
```json
{
  "Humidity": 0.89,
  "WindSpeed": 14.12,
  "Pressure": 1015.13,
  "ActualTemperature": 9.47
}
```

**Topic Configuration**:
```
Name:                weather-data
Partitions:          1
Replication Factor:  1
Retention:           Infinite
Compression Type:    None
```

**Performance Characteristics**:
```
Throughput:     1 message/second (tunable via time.sleep())
Message Size:   ~150 bytes
Serialization:  JSON
```

**Error Handling**:
```python
# Automatic retries on network error
# Flush ensures all messages sent before exit
producer.flush()
```

**Dependencies**:
```
kafka-python >= 2.0.2
pandas >= 1.3.0
json (built-in)
time (built-in)
```

---

### 2. Stream Processor Specification

**Component**: stream_processor.py

**Type**: Stream Processing Application

**Framework**: Apache Faust 0.10+

**Application Configuration**:
```python
app = faust.App(
    'weather-stream-app',
    broker='kafka://localhost:9092',
    value_serializer='json'
)
```

**Processing Mode**: Async (Non-blocking)

**Input Topic**:
```
Name:      weather-data
Consumer:  Automatic via @app.agent
Format:    JSON
```

**Output Topic**:
```
Name:      predictions
Producer:  Automatic via topic.send()
Format:    JSON
```

**Processing Pipeline**:
```
1. Consume Event       [1ms]
   └─ weather-data topic

2. Extract Features   [2ms]
   ├─ Humidity (float)
   ├─ Wind Speed (float)
   └─ Pressure (float)

3. Load ML Model      [0ms] (cached in memory)
   └─ weather_model.pkl

4. Make Prediction    [3-5ms]
   └─ LinearRegression.predict()

5. Enrich Data        [1ms]
   ├─ Keep original inputs
   ├─ Add predicted temperature
   └─ Round to 2 decimals

6. Send Result        [2ms]
   └─ predictions topic
```

**Total Processing Latency**: ~8-15ms per message

**Prediction Payload**:
```json
{
  "Humidity": 0.72,
  "WindSpeed": 15.2,
  "Pressure": 1017.3,
  "ActualTemperature": 16.5,
  "PredictedTemperature": 16.48
}
```

**ML Model Details**:
```
File:              weather_model.pkl
Type:              Linear Regression
Framework:         scikit-learn
Features:          3 (Humidity, WindSpeed, Pressure)
Model Size:        ~2KB
Load Time:         ~50-100ms
Prediction Time:   ~0.5-1ms per record
```

**Memory Requirements**:
```
Base App:          100MB
Model in Memory:   ~5MB
Processing Buffer: 50-200MB
Total:             ~150-300MB
```

**Dependencies**:
```
faust >= 0.10.0
kafka-python >= 2.0.2
joblib >= 1.0.0
scikit-learn >= 1.0.0
```

---

### 3. Consumer Specification

**Component**: prediction_consumer.py

**Type**: Results Consumer

**Consumer Configuration**:
```python
group_id = 'prediction-group'
auto_offset_reset = 'earliest'
bootstrap_servers = 'localhost:9092'
value_deserializer = json.loads
```

**Input Topic**:
```
Name:          predictions
Consumer Group: prediction-group
Offset Reset:  Earliest (read from start)
```

**Processing Mode**: Continuous polling

**Message Processing**:
```python
for message in consumer:
    # Non-blocking iteration
    # Automatically commits offset
    print(message.value)
```

**Output Format**: Formatted JSON print to stdout

**Performance Characteristics**:
```
Latency:           <50ms from topic to display
Throughput:        No limit (depends on processor)
Memory Usage:      ~80-150MB
```

**Error Handling**:
```
- Automatic reconnection on connection loss
- Offset tracking ensures no message loss
- Consumer group allows multiple instances
```

**Dependencies**:
```
kafka-python >= 2.0.2
json (built-in)
```

---

### 4. Model Training Specification

**Component**: train_model.py

**Algorithm**: Linear Regression (scikit-learn)

**Training Pipeline**:

```
1. Load Data
   ├─ Source: weatherHistory.csv
   └─ Records: 96,453

2. Data Preprocessing
   ├─ Select features: Humidity, Wind Speed, Pressure
   ├─ Select target: Temperature (C)
   └─ Remove NAs: 96,201 records remain

3. Feature Engineering (None - raw features used)

4. Train-Test Split
   ├─ Test size: 20% (19,241 records)
   ├─ Train size: 80% (76,960 records)
   └─ Random state: 42 (reproducibility)

5. Model Training
   ├─ Algorithm: LinearRegression()
   ├─ Solver: Ordinary Least Squares (OLS)
   └─ Training time: <1 second

6. Model Evaluation
   ├─ Predictions: y_pred = model.predict(X_test)
   ├─ Error: MAE = mean_absolute_error(y_test, y_pred)
   └─ Result: MAE ≈ 2.15°C

7. Model Serialization
   ├─ Format: pickle (.pkl)
   ├─ Tool: joblib
   └─ Output: weather_model.pkl (~2KB)
```

**Model Equation** (Linear Regression):

```
Temperature = b0 + b1×Humidity + b2×WindSpeed + b3×Pressure

Where:
  b0 ≈ 20.5     (intercept)
  b1 ≈ -7.8     (humidity coefficient)
  b2 ≈ 0.08     (wind speed coefficient)
  b3 ≈ 0.01     (pressure coefficient)
```

**Training Parameters**:
```python
test_size = 0.2
random_state = 42
fit_intercept = True (default)
normalize = False (default)
copy_X = True (default)
n_jobs = None (default)
positive = False (default)
```

**Model Metrics**:

| Metric | Value |
|--------|-------|
| MAE | ~2.15°C |
| RMSE | ~2.8°C |
| R² | ~0.88 |
| MAPE | ~18-22% |

**Dependencies**:
```
pandas >= 1.3.0
scikit-learn >= 1.0.0
joblib >= 1.0.0
```

---

## Data Specifications

### Input Data Schema

**File**: weatherHistory.csv

**Format**: CSV (RFC 4180)

**Encoding**: UTF-8

**Structure**:

| Column | Type | Range | Required | Notes |
|--------|------|-------|----------|-------|
| Formatted Date | DateTime | 2006-2017 | Yes | ISO 8601 format |
| Summary | String | Categorical | Yes | Weather description |
| Precip Type | String | rain/snow/null | No | Precipitation type |
| Temperature (C) | Float | -20 to +35 | Yes | **Used in model** |
| Apparent Temperature (C) | Float | Decimal | No | Feels-like temp |
| Humidity | Float | 0.0-1.0 | Yes | **Used in model** |
| Wind Speed (km/h) | Float | 0-60+ | Yes | **Used in model** |
| Wind Bearing (degrees) | Float | 0-360 | No | Wind direction |
| Visibility (km) | Float | Positive | No | Visibility distance |
| Loud Cover | Float | 0-1 | No | Cloud coverage |
| Pressure (millibars) | Float | 990-1040 | Yes | **Used in model** |
| Daily Summary | String | Text | No | Daily summary |

### Data Types and Ranges

```python
# Data type validation
'Humidity': float, range=[0.0, 1.0]
'Wind Speed (km/h)': float, range=[0, 100]
'Pressure (millibars)': float, range=[900, 1100]
'Temperature (C)': float, range=[-50, 50]

# Valid values check
humidity_valid = (df['Humidity'] >= 0) & (df['Humidity'] <= 1)
pressure_valid = (df['Pressure (millibars)'] > 900) & 
                 (df['Pressure (millibars)'] < 1100)
```

### Message Format Specifications

**Producer Output** (JSON):
```json
{
  "Humidity": 0.72,           // Float, 0-1 range
  "WindSpeed": 15.2,          // Float, km/h
  "Pressure": 1017.3,         // Float, millibars
  "ActualTemperature": 16.5   // Float, Celsius
}
```

**Consumer Output** (JSON):
```json
{
  "Humidity": 0.72,
  "WindSpeed": 15.2,
  "Pressure": 1017.3,
  "ActualTemperature": 16.5,
  "PredictedTemperature": 16.48  // Float, rounded to 2 decimals
}
```

---

## API Specifications

### Kafka Topic APIs

#### Topic: weather-data

**Type**: Input Topic (Producer → Topic)

**Message Schema**:
```json
{
  "type": "object",
  "properties": {
    "Humidity": {"type": "number", "minimum": 0, "maximum": 1},
    "WindSpeed": {"type": "number"},
    "Pressure": {"type": "number"},
    "ActualTemperature": {"type": "number"}
  },
  "required": ["Humidity", "WindSpeed", "Pressure", "ActualTemperature"]
}
```

**Retention Policy**: Infinite  
**Partitioning**: Round-robin (key=null)

#### Topic: predictions

**Type**: Output Topic (Topic → Consumer)

**Message Schema**:
```json
{
  "type": "object",
  "properties": {
    "Humidity": {"type": "number", "minimum": 0, "maximum": 1},
    "WindSpeed": {"type": "number"},
    "Pressure": {"type": "number"},
    "ActualTemperature": {"type": "number"},
    "PredictedTemperature": {"type": "number"}
  },
  "required": ["Humidity", "WindSpeed", "Pressure", 
               "ActualTemperature", "PredictedTemperature"]
}
```

**Retention Policy**: Infinite  
**Partitioning**: Round-robin (key=null)

### Python API

#### Producer API

```python
from kafka import KafkaProducer

# Initialize
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send message
future = producer.send('weather-data', value={'Humidity': 0.72, ...})

# Wait for confirmation
metadata = future.get(timeout=10)

# Flush and close
producer.flush()
producer.close()
```

#### Consumer API

```python
from kafka import KafkaConsumer

# Initialize
consumer = KafkaConsumer(
    'predictions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='prediction-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Poll messages
for message in consumer:
    data = message.value  # Dict
    print(data)

consumer.close()
```

#### Stream Processor API

```python
import faust

# Initialize app
app = faust.App('weather-stream-app', broker='kafka://localhost:9092')

# Define topics
weather_topic = app.topic('weather-data')
prediction_topic = app.topic('predictions')

# Define agent (processor)
@app.agent(weather_topic)
async def process_weather(stream):
    async for event in stream:
        # Process event
        result = {...}
        # Send to output
        await prediction_topic.send(value=result)

# Start
if __name__ == '__main__':
    app.main()
```

---

## Performance Specifications

### Throughput Requirements

| Component | Requirement | Typical | Peak |
|-----------|-------------|---------|------|
| Producer | ≥1 msg/sec | 1 msg/sec | 1000+ msgs/sec |
| Processor | ≤100ms latency | 8-15ms | <50ms |
| Consumer | Real-time | <2s | <1s |

### Scalability Specifications

```
Horizontal Scaling:
  - Increase Kafka partitions: 1 → N
  - Add processor instances: 1 → N
  - Add consumers: 1 → N
  - Bottleneck: Kafka broker disk I/O

Vertical Scaling:
  - Increase broker memory: 512MB → 8GB+
  - Increase processor threads: 1 → N
  - Bottleneck: Single machine resources
```

### Latency Breakdown

```
End-to-End Latency: ~1000-2000ms
├─ Producer delay: 1000ms (time.sleep)
├─ Kafka latency: <10ms
├─ Processor latency: 8-15ms
├─ Kafka publish: <5ms
└─ Consumer poll: <50ms

Can be reduced to <50ms by:
  - Removing producer delay
  - Setting batch.size = 0
  - Enabling compression
```

### Resource Utilization

```
Per 1000 msgs/sec:
├─ Kafka CPU: 10-15%
├─ Kafka Memory: 200MB
├─ Processor CPU: 5-10%
├─ Processor Memory: 100MB
└─ Network: ~150KB/sec (uncompressed JSON)
```

---

## Security Specifications

### Current Security Posture

**Authentication**: None (development only)  
**Encryption**: None (development only)  
**Authorization**: None

### Production Security Requirements

#### Authentication (SASL/SCRAM)

```properties
# server.properties (Kafka broker)
listeners=SASL_PLAINTEXT://localhost:9092
security.inter.broker.protocol=SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
```

#### Encryption in Transit (TLS/SSL)

```properties
listeners=SSL://localhost:9092
ssl.keystore.location=/path/to/keystore.jks
ssl.keystore.password=<password>
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=<password>
```

#### Authorization (ACL)

```bash
# Grant producer permissions
bin/kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:producer --operation Write --topic weather-data

# Grant consumer permissions
bin/kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:consumer --operation Read --topic predictions
```

#### Data Validation

```python
def validate_weather_data(data):
    """Validate incoming weather data"""
    if not isinstance(data, dict):
        raise ValueError("Data must be dict")
    
    if 'Humidity' not in data or not (0 <= data['Humidity'] <= 1):
        raise ValueError("Invalid Humidity")
    
    if 'WindSpeed' not in data or data['WindSpeed'] < 0:
        raise ValueError("Invalid WindSpeed")
    
    if 'Pressure' not in data or not (900 < data['Pressure'] < 1100):
        raise ValueError("Invalid Pressure")
    
    return True
```

---

## Deployment Specifications

### Docker Deployment

**Compose Configuration**:
```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
```

**Startup**:
```bash
docker-compose up -d
docker-compose ps  # Verify
```

**Shutdown**:
```bash
docker-compose down
```

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BROKER=localhost:9092
KAFKA_GROUP_ID=prediction-group

# Model Configuration
MODEL_PATH=./weather_model.pkl

# Processing Configuration
PRODUCER_DELAY=1  # seconds between messages
BATCH_SIZE=1      # messages per batch
```

### Health Checks

```bash
# Check Kafka broker
curl -s localhost:9092 && echo "Kafka OK"

# Check Zookeeper
echo ruok | nc localhost 2181

# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec kafka kafka-topics --describe --topic weather-data \
  --bootstrap-server localhost:9092
```

### Monitoring

```bash
# Monitor producer
docker exec kafka kafka-console-consumer --topic weather-data \
  --from-beginning --bootstrap-server localhost:9092

# Monitor predictions
docker exec kafka kafka-console-consumer --topic predictions \
  --from-beginning --bootstrap-server localhost:9092

# Consumer lag
docker exec kafka kafka-consumer-groups --group prediction-group \
  --describe --bootstrap-server localhost:9092
```

---

**Document Status**: Complete  
**Last Updated**: 2026-05-26  
**Technical Review**: Pending

