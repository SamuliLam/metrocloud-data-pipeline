#!/usr/bin/env python3
"""
Script to run the PostgreSQL data sink service.
This consumes IoT data from Kafka and stores it in PostgreSQL database.
"""

import time
import signal
import sys
import os
import random

from src.utils.logger import log
from src.config.config import settings
from src.data_storage.postgresql_sink import PostgreSQLSink
from src.data_storage.database import db_manager
from src.utils.schema_registry import schema_registry


def wait_for_dependencies(max_retries=60, initial_backoff=2):
    """
    Wait for Kafka, Schema Registry, and PostgreSQL to become available.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
    
    Returns:
        True if all dependencies are available, False otherwise
    """
    backoff = initial_backoff
    
    for attempt in range(1, max_retries + 1):
        try:
            dependencies_ready = True
            
            # Check PostgreSQL
            log.info(f"Attempt {attempt}/{max_retries}: Checking PostgreSQL connection...")
            if not db_manager.test_connection():
                log.warning("PostgreSQL is not ready")
                dependencies_ready = False
            else:
                log.info("PostgreSQL is ready")
            
            # Check Kafka
            log.info(f"Attempt {attempt}/{max_retries}: Checking Kafka cluster...")
            from confluent_kafka import Consumer
            consumer_conf = settings.consumer.get_config(
                group_id=f"health-check-{attempt}",
                auto_offset_reset='earliest'
            )
            consumer = Consumer(consumer_conf)
            
            # List topics as a connectivity test
            device_metadata = consumer.list_topics(timeout=10)
            if device_metadata:
                log.info(f"Kafka is ready with {len(device_metadata.topics)} topics")
            else:
                log.warning("Kafka device metadata retrieval failed")
                dependencies_ready = False
            
            consumer.close()
            
            # Check Schema Registry
            log.info(f"Attempt {attempt}/{max_retries}: Checking Schema Registry...")
            try:
                subjects = schema_registry.get_subjects()
                log.info(f"Schema Registry is ready with subjects: {subjects}")
            except Exception as e:
                log.warning(f"Schema Registry check failed: {str(e)}")
                dependencies_ready = False
            
            if dependencies_ready:
                log.info("All dependencies are ready!")
                return True
            
        except Exception as e:
            log.warning(f"Dependency check failed: {str(e)}")
        
        if attempt < max_retries:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.1 * backoff)
            sleep_time = backoff + jitter
            
            log.info(f"Retrying in {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
            # Exponential backoff with cap
            backoff = min(backoff * 1.5, 30)
        else:
            log.error(f"Dependencies not ready after {max_retries} attempts")
            return False


def log_configuration():
    """
    Log the current configuration settings.
    """
    log.info("PostgreSQL Data Sink Configuration:")
    log.info(f"App name: {settings.app_name}")
    log.info(f"Environment: {settings.environment}")
    
    # Kafka settings
    log.info(f"Kafka bootstrap servers: {settings.kafka.bootstrap_servers}")
    log.info(f"Kafka topic: {settings.kafka.topic_name}")
    log.info(f"Consumer group ID: {settings.data_sink.consumer_group_id}")
    
    # PostgreSQL settings
    log.info(f"PostgreSQL host: {settings.postgresql.host}:{settings.postgresql.port}")
    log.info(f"Database name: {settings.postgresql.database}")
    log.info(f"Username: {settings.postgresql.username}")
    log.info(f"Main table: {settings.postgresql.main_table}")
    log.info(f"Archive table: {settings.postgresql.archive_table}")
    
    # Data sink settings
    log.info(f"Batch size: {settings.data_sink.batch_size}")
    log.info(f"Commit interval: {settings.data_sink.commit_interval} seconds")
    log.info(f"Max retries: {settings.data_sink.max_retries}")
    log.info(f"Retry backoff: {settings.data_sink.retry_backoff} seconds")
    
    # Data retention settings
    log.info(f"Archive after: {settings.postgresql.archive_after_days} days")
    log.info(f"Retention period: {settings.postgresql.retention_days} days")


def check_database_setup():
    """
    Check if database tables exist and are properly set up.
    """
    try:
        log.info("Checking database setup...")
        
        # Check if main table exists
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
            );
        """
        
        result = db_manager.execute_query(query, {'table_name': settings.postgresql.main_table})
        main_table_exists = result[0]['exists'] if result else False
        
        if not main_table_exists:
            log.error(f"Main table '{settings.postgresql.main_table}' does not exist!")
            log.error("Please run the database initialization script first.")
            return False
        
        # Check if archive table exists
        result = db_manager.execute_query(query, {'table_name': settings.postgresql.archive_table})
        archive_table_exists = result[0]['exists'] if result else False
        
        if not archive_table_exists:
            log.warning(f"Archive table '{settings.postgresql.archive_table}' does not exist!")
            log.warning("Archival functionality will not be available.")
        
        # Check table structure by counting columns
        structure_query = """
            SELECT count(*) as column_count
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
        """
        
        result = db_manager.execute_query(structure_query, {'table_name': settings.postgresql.main_table})
        column_count = result[0]['column_count'] if result else 0
        
        # We expect around 20+ columns based on our schema
        if column_count < 15:
            log.error(f"Main table appears to have incomplete structure (only {column_count} columns)")
            return False
        
        log.info(f"Database setup verified: {column_count} columns in main table")
        return True
        
    except Exception as e:
        log.error(f"Error checking database setup: {str(e)}")
        return False


def run_initial_health_checks():
    """
    Run initial health checks before starting the service.
    """
    log.info("Running initial health checks...")
    
    # Check database setup
    if not check_database_setup():
        return False
    
    # Test data insertion (dry run)
    try:
        test_data = {
            'device_id': 'health_check_device',
            'device_type': 'test_sensor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'value': 42.0,
            'unit': 'test_unit',
            'latitude': 60.1699,
            'longitude': 24.9384,
            'building': 'test_building',
            'floor': 1,
            'zone': 'test_zone',
            'room': 'test_room',
            'battery_level': 85.5,
            'signal_strength': -60.0,
            'firmware_version': '1.0.0',
            'is_anomaly': False,
            'status': 'ACTIVE',
            'maintenance_date': None,
            'device_metadata': {'test': True},
            'tags': ['health_check']
        }
        
        # Try inserting test data
        success = db_manager.insert_sensor_reading(test_data)
        if success:
            log.info("Database write test successful")
            
            # Clean up test data
            cleanup_query = "DELETE FROM sensor_readings WHERE device_id = 'health_check_device'"
            db_manager.execute_non_query(cleanup_query)
            log.info("Test data cleaned up")
        else:
            log.error("Database write test failed")
            return False
            
    except Exception as e:
        log.error(f"Database write test failed: {str(e)}")
        return False
    
    log.info("All health checks passed!")
    return True


def main():
    """
    Main function to run the PostgreSQL data sink service.
    """
    log.info("Starting PostgreSQL Data Sink Service")
    log_configuration()
    
    # Wait for dependencies
    log.info("Waiting for dependencies to be ready...")
    if not wait_for_dependencies():
        log.error("Dependencies are not available. Exiting.")
        sys.exit(1)
    
    # Run health checks
    if not run_initial_health_checks():
        log.error("Initial health checks failed. Exiting.")
        sys.exit(1)
    
    # Initialize and start the data sink
    sink = None
    try:
        log.info("Initializing PostgreSQL data sink...")
        sink = PostgreSQLSink()
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            log.info(f"Caught signal {sig}. Stopping data sink...")
            if sink:
                sink.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the sink
        log.info("Starting PostgreSQL data sink service...")
        log.info("Service will consume IoT data from Kafka and store in PostgreSQL")
        log.info("Press CTRL+C to stop the service")
        
        sink.start()
        
    except KeyboardInterrupt:
        log.info("Data sink interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in data sink: {str(e)}")
        raise
    finally:
        # Ensure cleanup happens
        if sink:
            sink.stop()
        
        # Close database connections
        db_manager.close()
        
        log.info("PostgreSQL data sink shutdown complete")


if __name__ == "__main__":
    main()