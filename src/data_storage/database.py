"""
TimescaleDB connection and operations for time-series IoT data.
"""

import json
import time
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import psycopg2
from psycopg2.extras import RealDictCursor

from src.utils.logger import log
from src.config.config import settings


class TimescaleDBManager:
    """
    Manages TimescaleDB database connections and operations optimized for time-series data.
    """
    
    def __init__(self):
        """
        Initialize the TimescaleDB manager.
        """
        self.engine: Optional[Engine] = None
        self._connection_retries = 0
        self._max_retries = 5
        self._retry_delay = 2
        
        # Initialize the database connection
        self._create_engine()
    
    def _create_engine(self):
        """
        Create SQLAlchemy engine with connection pooling for TimescaleDB.
        """
        try:
            database_url = settings.timescaledb.database_url
            
            # Create engine with connection pooling
            self.engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=settings.timescaledb.pool_size,
                max_overflow=settings.timescaledb.max_overflow,
                pool_timeout=settings.timescaledb.pool_timeout,
                pool_recycle=settings.timescaledb.pool_recycle,
                pool_pre_ping=True,  # Validate connections before use
                echo=False,  # Set to True for SQL debugging
                future=True
            )
            
            log.info(f"TimescaleDB engine created successfully")
            log.debug(f"Connection pool size: {settings.timescaledb.pool_size}")
            log.debug(f"Max overflow: {settings.timescaledb.max_overflow}")
            
        except Exception as e:
            log.error(f"Failed to create TimescaleDB engine: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test the TimescaleDB connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # Test basic connection
                result = conn.execute(text("SELECT 1")).fetchone()
                if result and result[0] == 1:
                    # Test TimescaleDB extension
                    ts_result = conn.execute(text("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")).fetchone()
                    if ts_result:
                        log.info("TimescaleDB connection and extension test successful")
                        return True
                    else:
                        log.error("TimescaleDB extension not found")
                        return False
                else:
                    log.error("TimescaleDB connection test failed: unexpected result")
                    return False
        except Exception as e:
            log.error(f"TimescaleDB connection test failed: {str(e)}")
            return False
    
    def wait_for_database(self, max_retries: int = 30, retry_delay: float = 2.0) -> bool:
        """
        Wait for TimescaleDB to become available.
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between attempts in seconds
            
        Returns:
            True if database is available, False otherwise
        """
        for attempt in range(1, max_retries + 1):
            try:
                if self.test_connection():
                    log.info(f"TimescaleDB is available after {attempt} attempt(s)")
                    return True
            except Exception as e:
                log.warning(f"TimescaleDB connection attempt {attempt}/{max_retries} failed: {str(e)}")
            
            if attempt < max_retries:
                log.info(f"Waiting {retry_delay} seconds before next attempt...")
                time.sleep(retry_delay)
        
        log.error(f"TimescaleDB is not available after {max_retries} attempts")
        return False
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.
        
        Yields:
            SQLAlchemy connection object
        """
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except OperationalError as e:
            log.error(f"TimescaleDB operational error: {str(e)}")
            # Try to recreate the engine
            if self._connection_retries < self._max_retries:
                self._connection_retries += 1
                log.info(f"Recreating TimescaleDB engine (attempt {self._connection_retries}/{self._max_retries})")
                time.sleep(self._retry_delay)
                self._create_engine()
                # Retry the connection
                try:
                    connection = self.engine.connect()
                    yield connection
                except Exception as retry_e:
                    log.error(f"Failed to reconnect after engine recreation: {str(retry_e)}")
                    raise
            else:
                log.error("Max reconnection attempts exceeded")
                raise
        except Exception as e:
            log.error(f"TimescaleDB connection error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
            # Reset retry counter on successful connection
            self._connection_retries = 0
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            parameters: Query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            with self.get_connection() as conn:
                if parameters:
                    result = conn.execute(text(query), parameters)
                else:
                    result = conn.execute(text(query))
                
                # Convert to list of dictionaries
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))
                
                return rows
                
        except Exception as e:
            log.error(f"Error executing query: {str(e)}")
            log.debug(f"Query: {query}")
            log.debug(f"Parameters: {parameters}")
            raise
    
    def execute_non_query(self, query: str, parameters: Dict[str, Any] = None) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query.
        
        Args:
            query: SQL query string
            parameters: Query parameters
            
        Returns:
            Number of affected rows
        """
        try:
            with self.get_connection() as conn:
                with conn.begin():  # Start transaction
                    if parameters:
                        result = conn.execute(text(query), parameters)
                    else:
                        result = conn.execute(text(query))
                    
                    return result.rowcount
                    
        except Exception as e:
            log.error(f"Error executing non-query: {str(e)}")
            log.debug(f"Query: {query}")
            log.debug(f"Parameters: {parameters}")
            raise
    
    def insert_sensor_reading(self, reading_data: Dict[str, Any]) -> bool:
        """
        Insert a single sensor reading into the TimescaleDB hypertable.
        
        Args:
            reading_data: Sensor reading data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare the INSERT query for TimescaleDB
            query = f"""
                INSERT INTO {settings.timescaledb.main_table} (
                    device_id, device_type, timestamp, value, unit,
                    latitude, longitude, building, floor, zone, room,
                    battery_level, signal_strength, firmware_version,
                    is_anomaly, status, maintenance_date, device_metadata, tags
                ) VALUES (
                    :device_id, :device_type, :timestamp, :value, :unit,
                    :latitude, :longitude, :building, :floor, :zone, :room,
                    :battery_level, :signal_strength, :firmware_version,
                    :is_anomaly, :status, :maintenance_date, :device_metadata, :tags
                )
            """
            
            # Prepare parameters
            location = reading_data.get('location', {})
            device_metadata = reading_data.get('device_metadata')
            
            parameters = {
                'device_id': reading_data.get('device_id'),
                'device_type': reading_data.get('device_type'),
                'timestamp': reading_data.get('timestamp'),
                'value': reading_data.get('value'),
                'unit': reading_data.get('unit'),
                'latitude': location.get('latitude'),
                'longitude': location.get('longitude'),
                'building': location.get('building'),
                'floor': location.get('floor'),
                'zone': location.get('zone'),
                'room': location.get('room'),
                'battery_level': reading_data.get('battery_level'),
                'signal_strength': reading_data.get('signal_strength'),
                'firmware_version': reading_data.get('firmware_version'),
                'is_anomaly': reading_data.get('is_anomaly', False),
                'status': reading_data.get('status', 'ACTIVE'),
                'maintenance_date': reading_data.get('maintenance_date'),
                'device_metadata': json.dumps(device_metadata) if device_metadata else None,
                'tags': reading_data.get('tags', [])
            }
            
            rows_affected = self.execute_non_query(query, parameters)
            return rows_affected > 0
            
        except Exception as e:
            log.error(f"Error inserting sensor reading: {str(e)}")
            log.debug(f"Reading data: {reading_data}")
            return False
    
    def insert_sensor_readings_batch(self, readings: List[Dict[str, Any]]) -> int:
        """
        Insert multiple sensor readings in a batch using TimescaleDB optimizations.
        
        Args:
            readings: List of sensor reading data
            
        Returns:
            Number of successfully inserted rows
        """
        if not readings:
            return 0
        
        try:
            # Use raw psycopg2 for better batch performance with TimescaleDB
            with psycopg2.connect(settings.timescaledb.database_url) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Prepare the INSERT query with ON CONFLICT handling
                    query = f"""
                        INSERT INTO {settings.timescaledb.main_table} (
                            device_id, device_type, timestamp, value, unit,
                            latitude, longitude, building, floor, zone, room,
                            battery_level, signal_strength, firmware_version,
                            is_anomaly, status, maintenance_date, device_metadata, tags
                        ) VALUES %s
                        ON CONFLICT DO NOTHING
                    """
                    
                    # Prepare values for batch insert
                    values = []
                    for reading in readings:
                        location = reading.get('location', {})
                        device_metadata = reading.get('device_metadata')
                        
                        value_tuple = (
                            reading.get('device_id'),
                            reading.get('device_type'),
                            reading.get('timestamp'),
                            reading.get('value'),
                            reading.get('unit'),
                            location.get('latitude'),
                            location.get('longitude'),
                            location.get('building'),
                            location.get('floor'),
                            location.get('zone'),
                            location.get('room'),
                            reading.get('battery_level'),
                            reading.get('signal_strength'),
                            reading.get('firmware_version'),
                            reading.get('is_anomaly', False),
                            reading.get('status', 'ACTIVE'),
                            reading.get('maintenance_date'),
                            psycopg2.extras.Json(device_metadata) if device_metadata else None,
                            reading.get('tags', [])
                        )
                        values.append(value_tuple)
                    
                    # Execute batch insert with larger page size for TimescaleDB
                    psycopg2.extras.execute_values(
                        cur, query, values, template=None, page_size=2000
                    )
                    
                    rows_inserted = cur.rowcount
                    conn.commit()
                    
                    log.info(f"Successfully inserted {rows_inserted} sensor readings into TimescaleDB")
                    return rows_inserted
                    
        except Exception as e:
            log.error(f"Error in TimescaleDB batch insert: {str(e)}")
            log.debug(f"Number of readings: {len(readings)}")
            return 0
    
    def get_recent_readings(self, device_id: str = None, limit: int = 100, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent sensor readings using TimescaleDB time-based queries.
        
        Args:
            device_id: Optional device ID filter
            limit: Maximum number of readings to return
            hours: Number of hours back to query
            
        Returns:
            List of recent sensor readings
        """
        try:
            if device_id:
                query = f"""
                    SELECT * FROM {settings.timescaledb.main_table} 
                    WHERE device_id = :device_id 
                    AND timestamp >= NOW() - INTERVAL '%s hours'
                    ORDER BY timestamp DESC 
                    LIMIT :limit
                """ % hours
                parameters = {'device_id': device_id, 'limit': limit}
            else:
                query = f"""
                    SELECT * FROM {settings.timescaledb.main_table}
                    WHERE timestamp >= NOW() - INTERVAL '%s hours'
                    ORDER BY timestamp DESC 
                    LIMIT :limit
                """ % hours
                parameters = {'limit': limit}
            
            return self.execute_query(query, parameters)
            
        except Exception as e:
            log.error(f"Error getting recent readings: {str(e)}")
            return []
    
    def get_device_stats(self, device_id: str = None) -> List[Dict[str, Any]]:
        """
        Get device statistics using TimescaleDB functions.
        
        Args:
            device_id: Optional device ID filter
            
        Returns:
            List of device statistics
        """
        try:
            query = "SELECT * FROM get_device_stats(:device_id)"
            parameters = {'device_id': device_id}
            
            return self.execute_query(query, parameters)
            
        except Exception as e:
            log.error(f"Error getting device stats: {str(e)}")
            return []
    
    def get_timeseries_data(self, device_id: str, start_time: str = None, end_time: str = None, 
                           bucket_interval: str = '1 hour') -> List[Dict[str, Any]]:
        """
        Get time-series data with time bucketing using TimescaleDB functions.
        
        Args:
            device_id: Device ID to query
            start_time: Start time (ISO format or interval like '7 days')
            end_time: End time (ISO format, defaults to now)
            bucket_interval: Time bucket interval (e.g., '1 hour', '15 minutes')
            
        Returns:
            List of time-bucketed data
        """
        try:
            # Default to last 7 days if no start_time specified
            if start_time is None:
                start_time = "NOW() - INTERVAL '7 days'"
            else:
                # If it looks like an interval, use it as such
                if 'days' in start_time or 'hours' in start_time or 'minutes' in start_time:
                    start_time = f"NOW() - INTERVAL '{start_time}'"
                else:
                    start_time = f"'{start_time}'"
            
            end_time = end_time or "NOW()"
            if end_time != "NOW()":
                end_time = f"'{end_time}'"
            
            query = f"""
                SELECT 
                    time_bucket('{bucket_interval}', timestamp) AS time_bucket,
                    COUNT(*) as reading_count,
                    AVG(value) as avg_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value
                FROM {settings.timescaledb.main_table}
                WHERE device_id = :device_id
                    AND timestamp >= {start_time}
                    AND timestamp <= {end_time}
                GROUP BY time_bucket
                ORDER BY time_bucket
            """
            
            parameters = {'device_id': device_id}
            return self.execute_query(query, parameters)
            
        except Exception as e:
            log.error(f"Error getting timeseries data: {str(e)}")
            return []
    
    def get_hourly_aggregates(self, device_id: str = None, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get hourly aggregated data using TimescaleDB continuous aggregates.
        
        Args:
            device_id: Optional device ID filter
            days_back: Number of days back to query
            
        Returns:
            List of hourly aggregated data
        """
        try:
            if device_id:
                query = """
                    SELECT * FROM sensor_readings_hourly
                    WHERE device_id = :device_id 
                    AND bucket >= NOW() - INTERVAL '%s days'
                    ORDER BY bucket DESC
                """ % days_back
                parameters = {'device_id': device_id}
            else:
                query = """
                    SELECT * FROM sensor_readings_hourly
                    WHERE bucket >= NOW() - INTERVAL '%s days'
                    ORDER BY bucket DESC
                """ % days_back
                parameters = {}
            
            return self.execute_query(query, parameters)
            
        except Exception as e:
            log.error(f"Error getting hourly aggregates: {str(e)}")
            return []
    
    def get_daily_aggregates(self, device_id: str = None, days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Get daily aggregated data using TimescaleDB continuous aggregates.
        
        Args:
            device_id: Optional device ID filter
            days_back: Number of days back to query
            
        Returns:
            List of daily aggregated data
        """
        try:
            if device_id:
                query = """
                    SELECT * FROM sensor_readings_daily
                    WHERE device_id = :device_id 
                    AND bucket >= NOW() - INTERVAL '%s days'
                    ORDER BY bucket DESC
                """ % days_back
                parameters = {'device_id': device_id}
            else:
                query = """
                    SELECT * FROM sensor_readings_daily
                    WHERE bucket >= NOW() - INTERVAL '%s days'
                    ORDER BY bucket DESC
                """ % days_back
                parameters = {}
            
            return self.execute_query(query, parameters)
            
        except Exception as e:
            log.error(f"Error getting daily aggregates: {str(e)}")
            return []
    
    def cleanup_old_data(self, archive_days: int = None, cleanup_days: int = None) -> Dict[str, int]:
        """
        Clean up old data using TimescaleDB retention policies.
        
        Args:
            archive_days: Days after which to archive data
            cleanup_days: Days after which to delete archived data
            
        Returns:
            Dictionary with archive and cleanup counts
        """
        try:
            archive_days = archive_days or settings.postgresql.archive_after_days
            cleanup_days = cleanup_days or settings.postgresql.retention_days
            
            # Archive old data
            archive_query = "SELECT archive_old_data(:days_old)"
            archive_result = self.execute_query(archive_query, {'days_old': archive_days})
            archived_count = archive_result[0]['archive_old_data'] if archive_result else 0
            
            # Cleanup very old archived data
            cleanup_query = "SELECT cleanup_archived_data(:days_old)"
            cleanup_result = self.execute_query(cleanup_query, {'days_old': cleanup_days})
            cleaned_count = cleanup_result[0]['cleanup_archived_data'] if cleanup_result else 0
            
            result = {
                'archived_rows': archived_count,
                'cleaned_rows': cleaned_count
            }
            
            if archived_count > 0 or cleaned_count > 0:
                log.info(f"TimescaleDB data cleanup completed: {result}")
            
            return result
            
        except Exception as e:
            log.error(f"Error during TimescaleDB data cleanup: {str(e)}")
            return {'archived_rows': 0, 'cleaned_rows': 0}
    
    def vacuum_and_analyze(self):
        """
        Run VACUUM and ANALYZE on TimescaleDB tables for maintenance.
        """
        try:
            main_table = settings.timescaledb.main_table
            archive_table = settings.timescaledb.archive_table
            
            log.info("Starting TimescaleDB vacuum and analyze operation...")
            
            # Vacuum and analyze main table
            vacuum_query = f"VACUUM ANALYZE {main_table}"
            self.execute_non_query(vacuum_query)
            log.info(f"Vacuumed and analyzed table: {main_table}")
            
            # Vacuum and analyze archive table if it exists
            try:
                vacuum_archive_query = f"VACUUM ANALYZE {archive_table}"
                self.execute_non_query(vacuum_archive_query)
                log.info(f"Vacuumed and analyzed table: {archive_table}")
            except:
                log.warning(f"Could not vacuum archive table: {archive_table}")
            
            log.info("TimescaleDB vacuum and analyze operation completed")
            
        except Exception as e:
            log.error(f"Error during vacuum and analyze operation: {str(e)}")
    
    def refresh_continuous_aggregates(self):
        """
        Manually refresh TimescaleDB continuous aggregates.
        """
        try:
            log.info("Refreshing TimescaleDB continuous aggregates...")
            
            # Refresh hourly aggregates
            self.execute_non_query("CALL refresh_continuous_aggregate('sensor_readings_hourly', NULL, NULL)")
            log.info("Refreshed hourly continuous aggregate")
            
            # Refresh daily aggregates  
            self.execute_non_query("CALL refresh_continuous_aggregate('sensor_readings_daily', NULL, NULL)")
            log.info("Refreshed daily continuous aggregate")
            
        except Exception as e:
            log.error(f"Error refreshing continuous aggregates: {str(e)}")
    
    def get_hypertable_info(self) -> Dict[str, Any]:
        """
        Get information about TimescaleDB hypertables (compatible with open-source).
        
        Returns:
            Dictionary with hypertable information.
        """
        try:
            query = """
                SELECT 
                    h.hypertable_schema AS schemaname,
                    h.hypertable_name AS tablename,
                    h.num_dimensions,
                    (
                        SELECT COUNT(*) 
                        FROM timescaledb_information.chunks c
                        WHERE c.hypertable_schema = h.hypertable_schema
                        AND c.hypertable_name = h.hypertable_name
                    ) AS num_chunks,
                    h.compression_enabled,
                    pg_total_relation_size(quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name)) AS total_bytes,
                    pg_relation_size(quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name)) AS table_bytes,
                    pg_indexes_size(quote_ident(h.hypertable_schema) || '.' || quote_ident(h.hypertable_name)) AS index_bytes
                FROM timescaledb_information.hypertables h
                WHERE h.hypertable_schema = 'public'
            """
            
            result = self.execute_query(query)
            
            info = {}
            for row in result:
                table_name = row['tablename']
                info[table_name] = {
                    'num_dimensions': row['num_dimensions'],
                    'num_chunks': row['num_chunks'],
                    'compression_enabled': row['compression_enabled'],
                    'table_bytes': row['table_bytes'],
                    'index_bytes': row['index_bytes'],
                    'total_bytes': row['total_bytes']
                }
            
            return info

        except Exception as e:
            log.error(f"Error getting hypertable info: {str(e)}")
            return {}
 
    def close(self):
        """
        Close the database engine and all connections.
        """
        if self.engine:
            self.engine.dispose()
            log.info("TimescaleDB connections closed")


# Singleton instance
db_manager = TimescaleDBManager()