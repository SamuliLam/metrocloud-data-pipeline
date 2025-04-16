# IoT Data Ingestion Pipeline with Kafka
A scalable data pipeline for ingesting IoT sensor data using Apache Kafka with KRaft mode.

## Project Overview
This project implements a complete data ingestion pipeline for IoT devices with the following components:

- Data Simulation: Simulates multiple IoT sensors (temperature, humidity, pressure, motion, light) generating data
- Data Ingestion: Uses Apache Kafka for reliable, scalable data ingestion

## Architecture
![architecture.png](architecture.png)

## Project Structure
```
metrocloud-data-pipeline/
├── docker/
│   ├── docker-compose.yml           # Docker Compose configuration for all services
│   ├── Dockerfile.producer          # Dockerfile for the producer service
│   └── Dockerfile.consumer          # Dockerfile for the consumer service
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── config.py                # Configuration using pydantic-settings
│   ├── data_generator/
│   │   ├── __init__.py
│   │   └── iot_simulator.py         # IoT device simulator
│   ├── data_ingestion/
│   │   ├── __init__.py
│   │   ├── producer.py              # Kafka producer implementation
│   │   └── consumer.py              # Kafka consumer implementation
│   └── utils/
│       ├── __init__.py
│       └── logger.py                # Logger configuration
├── main.py                          # Main application for local development
├── README.md                        # Project documentation
├── run_producer.py                  # Standalone script for producer in Docker
├── run_consumer.py                  # Standalone script for consumer in Docker
├── requirements.txt                 # Python dependencies
```

## Technologies Used
- Apache Kafka: Message broker for data ingestion
- KRaft Mode: Kafka's Raft implementation (no ZooKeeper dependency)
- Confluent Python Client: For producing and consuming Kafka messags
- Pydantic: For configuration management and data validation
- Docker & Docker Compose: For containerization and orchestration
- Kafka UI: Web interface for monitoring Kafka

## Getting Started
### Prerequisites
- Docker and Docker Compose
- Python 3.8+ (for local development)

### Running the Project with Docker Compose
1. Clone the repository:
    ```bash
    git clone https://github.com/AI-Skaalaajat/metrocloud-data-pipeline.git<repository-url>
    cd metrocloud-data-pipeline
    ```

2. Start all services using Docker Compose:
    ```bash
    cd docker
    docker-compose up -d
    ```
    This will start:
    - Kafka broker in KRaft mode
    - Kafka UI for monitoring (accessible at http://localhost:8080)
    - Producer service that generates and sends IoT data
    - Consumer service that processes the data and generates alerts

3. View the logs
    ```bash
    # View all logs
    docker-compose logs -f

    # View logs for a specific service
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
        - Topics and messages
        - Consumer groups

6. Shutdown
    ```bash
    docker-compose down -v
    ```

### Running Locally for Development
For development purposes, you can run the components separately:
1. Start only the Kafka broker and UI:
    ```bash
    cd docker
    docker-compose up -d kafka kafka-ui
    ```

2. Create a virtual python environment
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3. Install Python dependencies
    ```bash
    pip install -r requirements.txt
    ```

3. Run the main application
    ```bash
    python main.py
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

## Configuration
The application can be configured through environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: "localhost:29092")
- `KAFKA_TOPIC_NAME`: Kafka topic name (default: "iot-sensor-data")
- `KAFKA_CONSUMER_GROUP_ID`: Consumer group ID (default: "iot-data-consumer")
- `KAFKA_AUTO_OFFSET_RESET`: Auto offset reset (default: "earliest")
- `IOT_NUM_DEVICES`: Number of simulated IoT devices (default: 5)
- `IOT_DATA_INTERVAL_SEC`: Interval between data generation in seconds (default: 1.0)

## Troubleshooting
### Kafka Connection Issues
- Ensure all containers are on the same Docker network
- Check that Kafka has had enough time to initialize before producer/consumer connect
- Verify the advertised listeners are configured correctly for both internal and external access

### Producer/Consumer Issues
- Check the logs for connection errors or exceptions
- Ensure the topic has been created
- Verify environment variables are set correctly

### Network Connectivity
- Test connectivity between containers with:
    ```bash
    docker exec kafka-producer ping kafka
    ```

## Next Steps and Enhancements
Further enhancements for this project:
1. Add data storage (TimescaleDB or InfluxDB) for IoT data
2. Implement data processing with Apache Spark
3. Create visualization dashboards with Grafana
4. Implement machine learning for anomaly detection
5. Add authentication and TLS encryption for Kafka
6. Scale to multiple Kafka brokers for high availability


