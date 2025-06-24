"""
Prometheus metrics middleware for IoT application components.

This module provides comprehensive metrics collection for all components of the IoT data pipeline,
including Kafka producers/consumers, MQTT adapters, TimescaleDB sinks, and system monitoring.
"""

import time
import threading
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, start_http_server, CollectorRegistry, REGISTRY
from functools import wraps

from src.utils.logger import log


class PrometheusMetrics:
    """
    Prometheus metrics collector for IoT pipeline components.
    """
    
    def __init__(self, service_name: str, registry: CollectorRegistry = None):
        """
        Initialize metrics for a specific service.
        
        Args:
            service_name: Name of the service (producer, consumer, etc.)
            registry: Optional custom registry
        """
        self.service_name = service_name
        self.registry = registry or REGISTRY
        
        # Common labels
        self.common_labels = ['service', 'instance']
        
        # Initialize metrics
        self._init_metrics()
        
        log.info(f"Prometheus metrics initialized for service: {service_name}")
    
    def _init_metrics(self):
        """
        Initialize all metrics for the service.
        """
        # Message processing metrics
        self.messages_received_total = Counter(
            'iot_messages_received_total',
            'Total number of messages received',
            self.common_labels,
            registry=self.registry
        )
        
        self.messages_processed_total = Counter(
            'iot_messages_processed_total',
            'Total number of messages successfully processed',
            self.common_labels,
            registry=self.registry
        )
        
        self.messages_failed_total = Counter(
            'iot_messages_failed_total',
            'Total number of messages that failed processing',
            self.common_labels + ['error_type'],
            registry=self.registry
        )
        
        # Processing time metrics
        self.processing_duration_seconds = Histogram(
            'iot_processing_duration_seconds',
            'Time spent processing messages',
            self.common_labels + ['operation'], # ['service', 'instance', 'operation']
            registry=self.registry
        )
        
        # Queue/buffer metrics
        self.queue_size = Gauge(
            'iot_queue_size',
            'Current size of processing queue/buffer',
            self.common_labels,
            registry=self.registry
        )
        
        # Connection status metrics
        self.connection_status = Gauge(
            'iot_connection_status',
            'Connection status (1=connected, 0=disconnected)',
            self.common_labels + ['connection_type'],
            registry=self.registry
        )
        
        # Data quality metrics
        self.anomaly_detected_total = Counter(
            'iot_anomaly_detected_total',
            'Total number of anomalies detected',
            self.common_labels + ['device_type'],
            registry=self.registry
        )
        
        self.validation_failures_total = Counter(
            'iot_validation_failures_total',
            'Total number of validation failures',
            self.common_labels + ['failure_type'],
            registry=self.registry
        )
        
        # Application-specific metrics will be added by subclasses
    
    def get_common_labels_dict(self, **extra_labels) -> Dict[str, str]:
        """
        Get common labels as a dictionary.
        
        Args:
            extra_labels: Additional labels to include
            
        Returns:
            Dictionary of labels
        """
        labels = {
            'service': self.service_name,
            'instance': f"{self.service_name}-1"  # Could be made configurable
        }
        labels.update(extra_labels)
        return labels
    
    def record_message_received(self, **labels):
        """Record a message received."""
        self.messages_received_total.labels(**self.get_common_labels_dict(**labels)).inc()
    
    def record_message_processed(self, **labels):
        """Record a message successfully processed."""
        self.messages_processed_total.labels(**self.get_common_labels_dict(**labels)).inc()
    
    def record_message_failed(self, error_type: str = "unknown", **labels):
        """Record a message processing failure."""
        self.messages_failed_total.labels(
            **self.get_common_labels_dict(error_type=error_type, **labels)
        ).inc()
    
    def record_processing_time(self, duration: float, **labels):
        """Record processing time."""
        self.processing_duration_seconds.labels(**self.get_common_labels_dict(**labels)).observe(duration)
    
    def set_queue_size(self, size: int, **labels):
        """Set current queue size."""
        self.queue_size.labels(**self.get_common_labels_dict(**labels)).set(size)
    
    def set_connection_status(self, connected: bool, connection_type: str, **labels):
        """Set connection status."""
        status = 1 if connected else 0
        self.connection_status.labels(
            **self.get_common_labels_dict(connection_type=connection_type, **labels)
        ).set(status)
    
    def record_anomaly_detected(self, device_type: str = "unknown", **labels):
        """Record an anomaly detection."""
        self.anomaly_detected_total.labels(
            **self.get_common_labels_dict(device_type=device_type, **labels)
        ).inc()
    
    def record_validation_failure(self, failure_type: str = "unknown", **labels):
        """Record a validation failure."""
        self.validation_failures_total.labels(
            **self.get_common_labels_dict(failure_type=failure_type, **labels)
        ).inc()


class KafkaProducerMetrics(PrometheusMetrics):
    """
    Kafka Producer specific metrics.
    """
    
    def __init__(self, registry: CollectorRegistry = None):
        super().__init__("kafka-producer", registry)
        self._init_producer_metrics()
    
    def _init_producer_metrics(self):
        """Initialize producer-specific metrics."""
        self.messages_sent_total = Counter(
            'kafka_producer_messages_sent_total',
            'Total number of messages sent to Kafka',
            self.common_labels + ['topic'],
            registry=self.registry
        )
        
        self.send_failures_total = Counter(
            'kafka_producer_send_failures_total',
            'Total number of send failures',
            self.common_labels + ['topic', 'error_type'],
            registry=self.registry
        )
        
        self.send_duration_seconds = Histogram(
            'kafka_producer_send_duration_seconds',
            'Time spent sending messages to Kafka',
            self.common_labels + ['topic'],
            registry=self.registry
        )
        
        self.batch_size = Histogram(
            'kafka_producer_batch_size',
            'Size of message batches sent',
            self.common_labels,
            registry=self.registry
        )
    
    def record_message_sent(self, topic: str = "unknown", **labels):
        """Record a message sent to Kafka."""
        self.messages_sent_total.labels(
            **self.get_common_labels_dict(topic=topic, **labels)
        ).inc()
    
    def record_send_failure(self, topic: str = "unknown", error_type: str = "unknown", **labels):
        """Record a send failure."""
        self.send_failures_total.labels(
            **self.get_common_labels_dict(topic=topic, error_type=error_type, **labels)
        ).inc()
    
    def record_send_duration(self, duration: float, topic: str = "unknown", **labels):
        """Record send duration."""
        self.send_duration_seconds.labels(
            **self.get_common_labels_dict(topic=topic, **labels)
        ).observe(duration)
    
    def record_batch_size(self, size: int, **labels):
        """Record batch size."""
        self.batch_size.labels(**self.get_common_labels_dict(**labels)).observe(size)


class KafkaConsumerMetrics(PrometheusMetrics):
    """
    Kafka Consumer specific metrics.
    """
    
    def __init__(self, registry: CollectorRegistry = None):
        super().__init__("kafka-consumer", registry)
        self._init_consumer_metrics()
    
    def _init_consumer_metrics(self):
        """Initialize consumer-specific metrics."""
        self.messages_consumed_total = Counter(
            'kafka_consumer_messages_consumed_total',
            'Total number of messages consumed from Kafka',
            self.common_labels + ['topic', 'partition'],
            registry=self.registry
        )
        
        self.consumer_lag = Gauge(
            'kafka_consumer_lag',
            'Current consumer lag',
            self.common_labels + ['topic', 'partition'],
            registry=self.registry
        )
        
        self.commit_duration_seconds = Histogram(
            'kafka_consumer_commit_duration_seconds',
            'Time spent committing offsets',
            self.common_labels,
            registry=self.registry
        )
        
        self.rebalance_total = Counter(
            'kafka_consumer_rebalance_total',
            'Total number of partition rebalances',
            self.common_labels,
            registry=self.registry
        )
    
    def record_message_consumed(self, topic: str = "unknown", partition: str = "unknown", **labels):
        """Record a message consumed from Kafka."""
        self.messages_consumed_total.labels(
            **self.get_common_labels_dict(topic=topic, partition=partition, **labels)
        ).inc()
    
    def set_consumer_lag(self, lag: int, topic: str = "unknown", partition: str = "unknown", **labels):
        """Set consumer lag."""
        self.consumer_lag.labels(
            **self.get_common_labels_dict(topic=topic, partition=partition, **labels)
        ).set(lag)
    
    def record_commit_duration(self, duration: float, **labels):
        """Record commit duration."""
        self.commit_duration_seconds.labels(**self.get_common_labels_dict(**labels)).observe(duration)
    
    def record_rebalance(self, **labels):
        """Record a partition rebalance."""
        self.rebalance_total.labels(**self.get_common_labels_dict(**labels)).inc()


class TimescaleDBSinkMetrics(PrometheusMetrics):
    """
    TimescaleDB Sink specific metrics.
    """
    
    def __init__(self, registry: CollectorRegistry = None):
        super().__init__("timescaledb-sink", registry)
        self._init_sink_metrics()
    
    def _init_sink_metrics(self):
        """Initialize sink-specific metrics."""
        self.records_inserted_total = Counter(
            'timescaledb_sink_records_inserted_total',
            'Total number of records inserted into TimescaleDB',
            self.common_labels + ['table'],
            registry=self.registry
        )
        
        self.insert_duration_seconds = Histogram(
            'timescaledb_sink_insert_duration_seconds',
            'Time spent inserting batches into TimescaleDB',
            self.common_labels + ['table'],
            registry=self.registry
        )
        
        self.batch_insert_size = Histogram(
            'timescaledb_sink_batch_insert_size',
            'Size of batches inserted into TimescaleDB',
            self.common_labels + ['table'],
            registry=self.registry
        )
        
        self.database_connections = Gauge(
            'timescaledb_sink_database_connections',
            'Current number of database connections',
            self.common_labels,
            registry=self.registry
        )
        
        self.maintenance_runs_total = Counter(
            'timescaledb_sink_maintenance_runs_total',
            'Total number of maintenance operations',
            self.common_labels + ['operation_type'],
            registry=self.registry
        )
    
    def record_records_inserted(self, count: int, table: str = "unknown", **labels):
        """Record records inserted."""
        self.records_inserted_total.labels(
            **self.get_common_labels_dict(table=table, **labels)
        ).inc(count)
    
    def record_insert_duration(self, duration: float, table: str = "unknown", **labels):
        """Record insert duration."""
        self.insert_duration_seconds.labels(
            **self.get_common_labels_dict(table=table, **labels)
        ).observe(duration)
    
    def record_batch_size(self, size: int, table: str = "unknown", **labels):
        """Record batch insert size."""
        self.batch_insert_size.labels(
            **self.get_common_labels_dict(table=table, **labels)
        ).observe(size)
    
    def set_database_connections(self, count: int, **labels):
        """Set database connection count."""
        self.database_connections.labels(**self.get_common_labels_dict(**labels)).set(count)
    
    def record_maintenance_run(self, operation_type: str = "unknown", **labels):
        """Record a maintenance operation."""
        self.maintenance_runs_total.labels(
            **self.get_common_labels_dict(operation_type=operation_type, **labels)
        ).inc()


class MQTTAdapterMetrics(PrometheusMetrics):
    """
    MQTT Adapter specific metrics.
    """
    
    def __init__(self, registry: CollectorRegistry = None):
        super().__init__("mqtt-adapter", registry)
        self._init_mqtt_metrics()
    
    def _init_mqtt_metrics(self):
        """Initialize MQTT-specific metrics."""
        self.mqtt_messages_received_total = Counter(
            'mqtt_adapter_messages_received_total',
            'Total number of MQTT messages received',
            self.common_labels + ['topic'],
            registry=self.registry
        )
        
        self.mqtt_connection_status = Gauge(
            'mqtt_adapter_connection_status',
            'MQTT connection status (1=connected, 0=disconnected)',
            self.common_labels,
            registry=self.registry
        )
        
        self.data_transformation_duration_seconds = Histogram(
            'mqtt_adapter_transformation_duration_seconds',
            'Time spent transforming MQTT messages',
            self.common_labels,
            registry=self.registry
        )
        
        self.ruuvitag_devices_seen = Gauge(
            'mqtt_adapter_ruuvitag_devices_seen',
            'Number of unique RuuviTag devices seen',
            self.common_labels,
            registry=self.registry
        )

        self.messages_sent_total = Counter(
            'mqtt_adapter_messages_sent_total',
            'Total number of messages sent to Kafka',
            self.common_labels + ['topic'],
            registry=self.registry
        )
        
        self.send_failures_total = Counter(
            'mqtt_adapter_send_failures_total',
            'Total number of send failures',
            self.common_labels + ['topic', 'error_type'],
            registry=self.registry
        )
        
        self.send_duration_seconds = Histogram(
            'mqtt_adapter_send_duration_seconds',
            'Time spent sending messages to Kafka',
            self.common_labels + ['topic'],
            registry=self.registry
        )
    
    def record_mqtt_message_received(self, topic: str = "unknown", **labels):
        """Record an MQTT message received."""
        self.mqtt_messages_received_total.labels(
            **self.get_common_labels_dict(topic=topic, **labels)
        ).inc()
    
    def set_mqtt_connection_status(self, connected: bool, **labels):
        """Set MQTT connection status."""
        status = 1 if connected else 0
        self.mqtt_connection_status.labels(**self.get_common_labels_dict(**labels)).set(status)
    
    def record_transformation_duration(self, duration: float, **labels):
        """Record data transformation duration."""
        self.data_transformation_duration_seconds.labels(
            **self.get_common_labels_dict(**labels)
        ).observe(duration)
    
    def set_ruuvitag_devices_count(self, count: int, **labels):
        """Set the number of RuuviTag devices seen."""
        self.ruuvitag_devices_seen.labels(**self.get_common_labels_dict(**labels)).set(count)

    def record_message_sent(self, topic: str = "unknown", **labels):
        """Record a message sent to Kafka."""
        self.messages_sent_total.labels(
            **self.get_common_labels_dict(topic=topic, **labels)
        ).inc()
    
    def record_send_failure(self, topic: str = "unknown", error_type: str = "unknown", **labels):
        """Record a send failure."""
        self.send_failures_total.labels(
            **self.get_common_labels_dict(topic=topic, error_type=error_type, **labels)
        ).inc()
    
    def record_send_duration(self, duration: float, topic: str = "unknown", **labels):
        """Record send duration."""
        self.send_duration_seconds.labels(
            **self.get_common_labels_dict(topic=topic, **labels)
        ).observe(duration)

def timed_operation(metrics_instance, operation_name: str = "operation"):
    """
    Decorator to time operations and record metrics.
    
    Args:
        metrics_instance: Instance of PrometheusMetrics
        operation_name: Name of the operation being timed
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                metrics_instance.record_processing_time(duration, operation=operation_name)
                return result
            except Exception as e:
                duration = time.time() - start_time
                metrics_instance.record_processing_time(duration, operation=operation_name)
                metrics_instance.record_message_failed(error_type=type(e).__name__)
                raise
        return wrapper
    return decorator


class MetricsServer:
    """
    HTTP server for exposing Prometheus metrics.
    """
    
    def __init__(self, port: int = 8000, registry: CollectorRegistry = None):
        """
        Initialize metrics server.
        
        Args:
            port: Port to run the metrics server on
            registry: Optional custom registry
        """
        self.port = port
        self.registry = registry or REGISTRY
        self.server_thread = None
        self.running = False
    
    def start(self):
        """Start the metrics server in a background thread."""
        if self.running:
            log.warning("Metrics server is already running")
            return
        
        def run_server():
            try:
                start_http_server(self.port, registry=self.registry)
                log.info(f"Metrics server started on port {self.port}")
                self.running = True
                # Keep the thread alive
                while self.running:
                    time.sleep(1)
            except Exception as e:
                log.error(f"Failed to start metrics server: {str(e)}")
        
        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
    
    def stop(self):
        """Stop the metrics server."""
        self.running = False
        if self.server_thread:
            self.server_thread.join(timeout=5)
        log.info("Metrics server stopped")


# Singleton instances for different services
_metrics_instances = {}

def get_metrics_instance(service_type: str) -> PrometheusMetrics:
    """
    Get or create a metrics instance for a specific service type.
    
    Args:
        service_type: Type of service (producer, consumer, sink, adapter)
        
    Returns:
        Appropriate metrics instance
    """
    if service_type not in _metrics_instances:
        if service_type == "producer":
            _metrics_instances[service_type] = KafkaProducerMetrics()
        elif service_type == "consumer":
            _metrics_instances[service_type] = KafkaConsumerMetrics()
        elif service_type == "sink":
            _metrics_instances[service_type] = TimescaleDBSinkMetrics()
        elif service_type == "adapter":
            _metrics_instances[service_type] = MQTTAdapterMetrics()
        else:
            _metrics_instances[service_type] = PrometheusMetrics(service_type)
    
    return _metrics_instances[service_type]