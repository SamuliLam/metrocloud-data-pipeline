-- TimescaleDB initialization script for IoT Data Pipeline

-- Create the database (if running as superuser)
-- CREATE DATABASE iot_data;

-- Create the user and grant permissions
-- CREATE USER iot_user WITH PASSWORD 'iot_password';
-- GRANT ALL PRIVILEGES ON DATABASE iot_data TO iot_user;

-- Connect to the iot_data database
\c iot_data;

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable PostGIS extension (if needed for location data)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Create enum for device status
CREATE TYPE device_status AS ENUM ('ACTIVE', 'IDLE', 'MAINTENANCE', 'ERROR', 'UNKNOWN');

-- Create main sensor readings table (will become a hypertable)
CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL,
    device_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT NOT NULL,
    
    -- Location information
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    building TEXT,
    floor INTEGER,
    zone TEXT,
    room TEXT,
    
    -- Device information
    battery_level DOUBLE PRECISION,
    signal_strength DOUBLE PRECISION,
    firmware_version TEXT,
    is_anomaly BOOLEAN DEFAULT FALSE,
    status device_status DEFAULT 'ACTIVE',
    maintenance_date TIMESTAMPTZ,
    
    -- Metadata as JSONB for flexibility
    device_metadata JSONB,
    tags TEXT[],
    
    -- Audit fields
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT valid_battery_level CHECK (battery_level IS NULL OR (battery_level >= 0 AND battery_level <= 100)),
    CONSTRAINT valid_coordinates CHECK (
        (latitude IS NULL AND longitude IS NULL) OR 
        (latitude IS NOT NULL AND longitude IS NOT NULL AND 
         latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
    )
);

-- Convert the table to a hypertable (partitioned by time)
-- Chunk interval of 1 day for optimal performance with IoT data
SELECT create_hypertable('sensor_readings', 'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Enable compression (safe to re-run)
DO $$
BEGIN
    ALTER TABLE sensor_readings SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'device_id'
    );
EXCEPTION WHEN others THEN
    -- Do nothing if compression is already enabled
    NULL;
END
$$;

-- Create archive table with same structure (also as hypertable)
CREATE TABLE IF NOT EXISTS sensor_readings_archive (
    LIKE sensor_readings INCLUDING ALL
);

-- Convert archive table to hypertable with longer chunk intervals (7 days)
SELECT create_hypertable('sensor_readings_archive', 'timestamp', 
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression (safe to re-run)
DO $$
BEGIN
    ALTER TABLE sensor_readings_archive SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'device_id'
    );
EXCEPTION WHEN others THEN
    -- Do nothing if compression is already enabled
    NULL;
END
$$;

-- Create indexes for performance (TimescaleDB automatically creates time index)
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_id ON sensor_readings(device_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_type ON sensor_readings(device_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_location ON sensor_readings(latitude, longitude, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_anomaly ON sensor_readings(is_anomaly, timestamp DESC) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_sensor_readings_status ON sensor_readings(status, timestamp DESC);

-- Create GIN index for JSONB device_metadata
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_metadata ON sensor_readings USING GIN(device_metadata);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_tags ON sensor_readings USING GIN(tags);

-- Create archive table indexes
CREATE INDEX IF NOT EXISTS idx_sensor_readings_archive_device_id ON sensor_readings_archive(device_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_archive_device_type ON sensor_readings_archive(device_type, timestamp DESC);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_sensor_readings_updated_at 
    BEFORE UPDATE ON sensor_readings 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for recent readings (last 24 hours)
CREATE OR REPLACE VIEW recent_sensor_readings AS
SELECT *
FROM sensor_readings
WHERE timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Create a view for anomalous readings
CREATE OR REPLACE VIEW anomalous_sensor_readings AS
SELECT *
FROM sensor_readings
WHERE is_anomaly = TRUE
ORDER BY timestamp DESC;

-- Create a view for device summary with TimescaleDB optimizations
CREATE OR REPLACE VIEW device_summary AS
SELECT 
    device_id,
    device_type,
    COUNT(*) as total_readings,
    last(battery_level, timestamp) as latest_battery_level,
    last(status, timestamp) as current_status,
    first(timestamp, timestamp) as first_reading,
    last(timestamp, timestamp) as last_reading,
    avg(value) as avg_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count
FROM sensor_readings
GROUP BY device_id, device_type
ORDER BY last_reading DESC;

-- Create time-bucket aggregation views for different time windows
CREATE OR REPLACE VIEW hourly_sensor_aggregates AS
SELECT 
    time_bucket('1 hour', timestamp) AS bucket,
    device_id,
    device_type,
    COUNT(*) as reading_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    last(battery_level, timestamp) as latest_battery_level
FROM sensor_readings
GROUP BY bucket, device_id, device_type
ORDER BY bucket DESC;

-- Create daily aggregation view
CREATE OR REPLACE VIEW daily_sensor_aggregates AS
SELECT 
    time_bucket('1 day', timestamp) AS bucket,
    device_id,
    device_type,
    COUNT(*) as reading_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    first(battery_level, timestamp) as first_battery_level,
    last(battery_level, timestamp) as latest_battery_level
FROM sensor_readings
GROUP BY bucket, device_id, device_type
ORDER BY bucket DESC;

-- TimescaleDB compression policy (compress data older than 7 days)
SELECT add_compression_policy('sensor_readings', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('sensor_readings_archive', INTERVAL '1 day', if_not_exists => TRUE);

-- TimescaleDB retention policy (automatically drop data older than 90 days from main table)
SELECT add_retention_policy('sensor_readings', INTERVAL '90 days', if_not_exists => TRUE);

-- TimescaleDB retention policy for archive (keep for 1 year)
SELECT add_retention_policy('sensor_readings_archive', INTERVAL '365 days', if_not_exists => TRUE);

-- Create function for manual data archival (move data from main to archive table)
CREATE OR REPLACE FUNCTION archive_old_data(days_old INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    rows_moved INTEGER;
    cutoff_time TIMESTAMPTZ;
BEGIN
    cutoff_time := NOW() - (days_old || ' days')::INTERVAL;
    
    -- Move old data to archive using INSERT...SELECT with time constraints
    INSERT INTO sensor_readings_archive
    SELECT * FROM sensor_readings
    WHERE timestamp < cutoff_time;
    
    GET DIAGNOSTICS rows_moved = ROW_COUNT;
    
    -- Delete the moved data from main table
    DELETE FROM sensor_readings
    WHERE timestamp < cutoff_time;
    
    RETURN rows_moved;
END;
$$ LANGUAGE plpgsql;

-- Create function for data cleanup (manual deletion of very old archived data)
CREATE OR REPLACE FUNCTION cleanup_archived_data(days_old INTEGER DEFAULT 365)
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM sensor_readings_archive
    WHERE timestamp < NOW() - (days_old || ' days')::INTERVAL;
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get device statistics with TimescaleDB optimizations
CREATE OR REPLACE FUNCTION get_device_stats(device_id_param TEXT DEFAULT NULL)
RETURNS TABLE (
    device_id TEXT,
    device_type TEXT,
    total_readings BIGINT,
    first_reading TIMESTAMPTZ,
    last_reading TIMESTAMPTZ,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    anomaly_percentage DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        sr.device_id,
        sr.device_type,
        COUNT(*) as total_readings,
        first(sr.timestamp, sr.timestamp) as first_reading,
        last(sr.timestamp, sr.timestamp) as last_reading,
        AVG(sr.value) as avg_value,
        MIN(sr.value) as min_value,
        MAX(sr.value) as max_value,
        (COUNT(CASE WHEN sr.is_anomaly THEN 1 END) * 100.0 / COUNT(*)) as anomaly_percentage
    FROM sensor_readings sr
    WHERE (device_id_param IS NULL OR sr.device_id = device_id_param)
    GROUP BY sr.device_id, sr.device_type
    ORDER BY last_reading DESC;
END;
$$ LANGUAGE plpgsql;

-- Create function to get time-series statistics for a device
CREATE OR REPLACE FUNCTION get_device_timeseries_stats(
    device_id_param TEXT,
    start_time TIMESTAMPTZ DEFAULT NOW() - INTERVAL '7 days',
    end_time TIMESTAMPTZ DEFAULT NOW(),
    bucket_interval INTERVAL DEFAULT INTERVAL '1 hour'
)
RETURNS TABLE (
    time_bucket TIMESTAMPTZ,
    reading_count BIGINT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        time_bucket(bucket_interval, timestamp) AS time_bucket,
        COUNT(*) as reading_count,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value
    FROM sensor_readings
    WHERE device_id = device_id_param
        AND timestamp >= start_time
        AND timestamp <= end_time
    GROUP BY time_bucket
    ORDER BY time_bucket;
END;
$$ LANGUAGE plpgsql;

-- Create continuous aggregates for real-time analytics (TimescaleDB feature)
CREATE MATERIALIZED VIEW sensor_readings_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', timestamp) AS bucket,
    device_id,
    device_type,
    COUNT(*) as reading_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    last(battery_level, timestamp) as latest_battery_level
FROM sensor_readings
GROUP BY bucket, device_id, device_type
ORDER BY bucket DESC;

-- Add refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('sensor_readings_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Create daily continuous aggregate
CREATE MATERIALIZED VIEW sensor_readings_daily
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', timestamp) AS bucket,
    device_id,
    device_type,
    COUNT(*) as reading_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    first(battery_level, timestamp) as first_battery_level,
    last(battery_level, timestamp) as latest_battery_level
FROM sensor_readings
GROUP BY bucket, device_id, device_type
ORDER BY bucket DESC;

-- Add refresh policy for daily aggregate
SELECT add_continuous_aggregate_policy('sensor_readings_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Insert some initial test data (optional)
-- INSERT INTO sensor_readings (device_id, device_type, timestamp, value, unit, latitude, longitude, building, floor, zone, room)
-- VALUES 
--     ('test_device_001', 'temperature_sensor', NOW(), 22.5, '°C', 60.1699, 24.9384, 'building-1', 1, 'main', 'room-101'),
--     ('test_device_002', 'humidity_sensor', NOW(), 45.2, '%', 60.1699, 24.9384, 'building-1', 1, 'main', 'room-101');

-- Grant permissions to iot_user on all created objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO iot_user;
GRANT SELECT ON sensor_readings_hourly TO iot_user;
GRANT SELECT ON sensor_readings_daily TO iot_user;

-- Display completion message
SELECT 'TimescaleDB database initialization completed successfully!' as message;