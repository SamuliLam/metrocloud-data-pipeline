# IoT Data Ingestion Pipeline with Multi-Broker Kafka and Schema Registry
A scalable, fault-tolerant data pipeline for ingesting, processing, and analyzing IoT sensor data using Apache Kafka with multiple brokers in KRaft mode and Schema Registry for data validation and evolution.

## Project Overview
This project implements a complete data ingestion pipeline for IoT devices with the following components:

- Data Simulation: Simulates multiple IoT sensors (temperature, humidity, pressure, motion, light, etc.) generating data
- Data Ingestion: Uses Confluet-Kafka-Python for reliable, scalable data ingestion
- Data Processing: Processes and filters the data, generating alerts for anomalies
- Monitoring: Kafka UI for observability and operational insights

## Key Features
- High availability: Replication across multiple Kafka brokers
- Data Validation: Schema enforcement at produce time
- Fault Tolerance: Automatic failover and recovery
- Efficient Serialization: Compact binary format with Avro
- Anomaly Detection: Real-time monitoring for unusual sensor readings
- Horizontal Scaling: Add more brokers to handle increased load
- Schema Management: Centralized control of data formats

## Architecture
![architecture.png](images/architecture1.png)

## Project Structure
```
metrocloud-data-pipeline/
├── docker/
│   ├── .env
│   ├── docker-compose.yml              # Docker Compose configuration for all services
│   ├── Dockerfile.producer             # Dockerfile for the producer service
│   └── Dockerfile.consumer             # Dockerfile for the consumer service
├── images/
│   ├── architecture.png                
│   └── architecture1.png
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── config.py                   # Configuration using pydantic-settings and Schema Registry
│   ├── data_generator/
│   │   ├── __init__.py
│   │   └── iot_simulator.py            # IoT device simulator (Avro-compatible)
│   ├── data_ingestion/
│   │   ├── __init__.py
│   │   ├── producer.py                 # Kafka producer implementation with Avro serialization
│   │   └── consumer.py                 # Kafka consumer implementation with Avro deserialization
│   ├── schemas/
│   │   └── iot_sensor_reading.avsc     # Avro schema for IoT sensor data
│   └── utils/
│       ├── __init__.py
│       ├── logger.py                   # Enhanced logger configuration
│       └── schema_registry.py          # Schema Registry client implementation
├── README.md                           # Project documentation
├── requirements.txt                    # Python dependencies
├── run_producer.py                     # Standalone script for producer in Docker
├── run_consumer.py                     # Standalone script for consumer in Docker
```

## Technologies Used
- Apache Kafka: Message broker for data ingestion
- KRaft Mode: Kafka's Raft implementation (no ZooKeeper dependency)
- Confluent Python Client: For producing and consuming Kafka messags
- Pydantic: For configuration management and data validation
- Docker & Docker Compose: For containerization and orchestration
- Kafka UI: Web interface for monitoring Kafka
- Schema Registry: Centralized schema management for data validation and evolution
- Avro Serialization: Efficient binary format with embedded schema information

## Getting Started
### Prerequisites
- Docker and Docker Compose
- Python 3.8+ (for local development)

### Running the Project with Docker Compose
1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2. Start all services using Docker Compose:
    ```bash
    cd docker
    docker-compose up -d
    ```
    This will start:
    - 3 Kafka brokers in KRaft mode (each acting as both broker and controller)
    - Schema Registry for centralized schema management
    - Kafka UI for monitoring (accessible at http://localhost:8080)
    - Producer service that generates and sends IoT data with Avro serialization
    - Consumer service that processes the data with Avro deserialization

3. View the logs
    ```bash
    # View all logs
    docker-compose logs -f

    # View logs for a specific service
    docker-compose logs -f kafka-broker-1 kafka-broker-2 kafka-broker-3
    docker-compose logs -f schema-registry
    docker-compose logs -f kafka-producer
    docker-compose logs -f kafka-consumer
    ```

4. Interact with Kafka using command-line tools
    ```bash
    # List Kafka topics
    docker exec kafka-broker kafka-topics --bootstrap-server kafka:9092 --list

    # View messages on the topic
    docker exec kafka-broker kafka-console-consumer --bootstrap-server kafka:9092 --topic iot-sensor-data --from-beginning
    ```

5. Access the Kafka UI:
    - Open your browser and navigate to http://localhost:8080
    - This allows you to monitor:
        - Kafka broker health
        - Schema Registry to view registered schemas
        - Topics and messages
        - Consumer groups
        - Partitions and their replication status

6. Shutdown
    ```bash
    docker-compose down -v
    ```
## IoT Data Simulation
The system simulates the following types of IoT devices:
- Temperature sensors (°C)
- Humidity sensors (%)
- Pressure sensors (hPa)
- Motion sensors (boolean)
- Light sensors (lux)

Data generation includes occasional anomalies to test alert detection:

- Extreme temperature readings
- High humidity
- Low pressure
- Motion detection events
- Low light conditions

## Kafka Cluster Configuration
### Multi-Broker Setup
This project uses 3 Kafka brokers in a KRaft quorum:

| Broker | Internal Port | External Port |         Role        |
|--------|---------------|---------------|---------------------|
| kafka1 |      9092     |     29092     | broker + controller |
| Kafka2 |      9092     |     29093     | broker + controller |
| Kafka3 |      9092     |     29094     | broker + controller |

### KRaft Mode (Kafka Raft)
This setup uses KRaft mode which eliminates the ZooKeeper dependency:
- All brokers participate in the Raft quorum
- Controllers quorum handles metadata management
- Each broker runs both controller and broker roles

### Topic Configuration
Topics are created with fault tolerance in mind:
- Replication Factor: 3 (data stored on all brokers)
- Partitions: 6 (allows parallel consumption)
- Min In-Sync Replicas: 2(requires at least 3 brokers to acknowledge writes)

## Avro Schema and Schema Registry
### Schema Registry
The Schema Registry provides:
- Centralized schema storage and versioninng
- Schema compatibility enforcement
- Schema evolution management
- Integration with Kafka producers and consumers

### Avro Schema
The IoT sensor data schema (`iot_sensor_reading.avsc`) includes:
- Basic sensor information (device_id, device_type, timestamp)
- Measurement data (value, unit)
- Location information (latitude, longitude, building, floor, zone)
- Device status (battery_level, signal_strength, firmware_version)
- Anomaly detection (is_anomaly)
- Extensibility with metadata field

## Configuration Options
The application can be configured through environment variables:

### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS`kafka1:9092,kafka2:9092,kafka3:9092
- `KAFKA_TOPIC_NAME`=iot-sensor-data
- `KAFKA_CONSUMER_GROUP_ID`=iot-data-consumer
- `KAFKA_AUTO_OFFSET_RESET`=earliest
- `KAFKA_REPLICATION_FACTOR`=3
- `KAFKA_PARTITIONS`=6

### IoT Simulator Configuration
- `IOT_NUM_DEVICES`=8
- `IOT_DATA_INTERVAL_SEC`=1.0
- `IOT_DEVICE_TYPES`=temperature,humidity,pressure,motion,light
- `IOT_ANOMALY_PROBABILITY`=0.05

### Logging Configuration
- `LOG_LEVEL`=INFO

### Schema Registry configuration
- `SCHEMA_REGISTRY_URL`=http://schema-registry:8081
- `SCHEMA_AUTO_REGISTER`=True
- `SCHEMA_COMPATIBILITY_LEVEL`=BACKWARD
- `SCHEMA_SUBJECT_STRATEGY`=TopicNameStrategy

## Understanding the Multi-Broker Setup
### Kafka Listeners Configuration
The Kafka configuration contains several listener configurations that are essential for proper network communication:

`KAFKA_LISTENERS`: 'PLAINTEXT://kafka1:9092,CONTROLLER://kafka1:29093,PLAINTEXT_HOST://0.0.0.0:29092'
`KAFKA_ADVERTISED_LISTENERS`: 'PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://localhost:29092'

- `PLAINTEXT`: Used for internal communication between brokers and clients within Docker network
- `CONTROLLER`: Used for controller-to-controller communication in KRaft mode
- `PLAINTEXT_HOST`: Used for external access from the host machine

### Controller Quorum
The KRaft controller quorum is configured with:

`KAFKA_CONTROLLER_QUORUM_VOTERS`: '1@kafka1:29093,2@kafka2:29093,3@kafka3:29093'

This defines the voting members of the Raft quorum, where each broker participates in the controller election process.

## Multi-Broker Kafka Benefits
- High Availabliity: No single point of failure with data replicated across multiple brokers
- Scalability: Horizontal scaling by adding more brokers to handle increased load
- Fault Tolerance: System continues to operate even if one or more brokers fail
- Performance: Multiple brokers can handle more concurrent produceres and consumers 

## Fault Tolerance Features
### Producer Resilience    
The producer is configured with:
- Connection to all brokers for automatic failover
- Retry mechanism with exponential backoff
- Delivery acknowledgement from all replicas (acks=all)
- Message batching for efficiency

### Consumer Resilience
The consumer is configured with:
- Connection to all brokers for automatic failover
- Cooperative rebalancing for smooth partition transitions
- Auto-commit of offsets for recovery
- Error handling and retry logic

## Monitoring
The Kafka UI provides insights into the health and performance of Kafka cluster:
- Broker status and configuration
- Topic information including partitions and replication
- Messages viewer with filtering capabilities
- Consumer group status and lag

## Advanced Operations
### Scaling the Cluster
To add a fourth broker to the cluster:
1. Add a new broker configuration to `docker-compose.yml`
2. Update the controller quorum voters list to include the new broker
3. Restart the cluster with `docker-compose up -d

### Monitoring Performance
One can monitor Kafka cluster performacne metrics using JMX and tools like Prometheus and Grafana.

### Data Recovery
The system is designed to automatically recover from most failure scenarios. In case of complete cluster failure:
1. Ensure all configuration files are intact
2. Start the cluster with `docker-compose up -d`
3. The brokers will recover data from the persistent volumes

## Troubleshooting
### Brokers Won't Start
- Check logs for configuration errors:
    ```bash
    docker-compose logs kafka1
    ```

### Kafka Connection Issues
- Ensure all containers are on the same Docker network
- Check that Kafka brokers have had enough time to initialize before producers/consumers connect
- Verify the advertised listeners are configured correctly for both internal and external access

### Producer/Consumer Issues
- Check the logs for connection errors or exceptions
- Ensure the topic has been created
- Verify environment variables are set correctly

### Network Connectivity Issues
- Verify Docker network configuration:
    ```bash
    docker network inspect docker_kafka-net
    ```

## Next Steps and Enhancements
Further enhancements for this project:
1. Add data storage with ... 
2. Implement data processing with ...
3. Create visualization dashboards with ...
4. Implement machine learning for anomaly detection
5. Add authentication and TLS encryption for Kafka