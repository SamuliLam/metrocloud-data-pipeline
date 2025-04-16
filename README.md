# Start the Kafka Service with Docker Compose
docker-compose up -d

# Monitor logs the ensure kafka initialization
docker logs -f kafka-broker

# Run the IoT Data Pipeline Application
python3 main.py

# Testing
## Interact with Kafka using command-line tools
### List Kafka topics
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

### View messages on the topic
docker exec kafka-broker kafka-console-consumer --bootstrap-server kafka:9092 --topic iot-sensor-data --from-beginning

# Shutdown
docker-compose down -v