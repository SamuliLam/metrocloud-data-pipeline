-- PostgreSQL initialization script for IoT Data Pipeline

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

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable PostGIS extension (if needed for location data)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Create enum for device status
CREATE TYPE device_status AS ENUM ('ACTIVE', 'IDLE', 'MAINTENANCE', 'ERROR', 'UNKNOWN');

-- Create main sensor readings table
CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    device_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    unit VARCHAR(50) NOT NULL,
    
    -- Location information
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    building VARCHAR(100),
    floor INTEGER,
    zone VARCHAR(100),
    room VARCHAR(100),
    
    -- Device information
    battery_level DOUBLE PRECISION,
    signal_strength DOUBLE PRECISION,
    firmware_version VARCHAR(50),
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

-- Create archive table with same structure
CREATE TABLE IF NOT EXISTS sensor_readings_archive (
    LIKE sensor_readings INCLUDING ALL
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_id ON sensor_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_type ON sensor_readings(device_type);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_location ON sensor_readings(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_anomaly ON sensor_readings(is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_sensor_readings_status ON sensor_readings(status);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_created_at ON sensor_readings(created_at);

-- Create GIN index for JSONB device_metadata
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_metadata ON sensor_readings USING GIN(device_metadata);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_tags ON sensor_readings USING GIN(tags);

-- Create archive table indexes
CREATE INDEX IF NOT EXISTS idx_sensor_readings_archive_device_id ON sensor_readings_archive(device_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_archive_timestamp ON sensor_readings_archive(timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_archive_created_at ON sensor_readings_archive(created_at);

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
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Create a view for anomalous readings
CREATE OR REPLACE VIEW anomalous_sensor_readings AS
SELECT *
FROM sensor_readings
WHERE is_anomaly = TRUE
ORDER BY timestamp DESC;

-- Create a view for device summary
CREATE OR REPLACE VIEW device_summary AS
SELECT 
    device_id,
    device_type,
    COUNT(*) as total_readings,
    MAX(timestamp) as last_reading,
    AVG(CASE WHEN value IS NOT NULL THEN value END) as avg_value,
    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
    MAX(battery_level) as latest_battery_level,
    MAX(status) as current_status
FROM sensor_readings
GROUP BY device_id, device_type
ORDER BY last_reading DESC;

-- Create function for data archival
CREATE OR REPLACE FUNCTION archive_old_data(days_old INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    rows_moved INTEGER;
BEGIN
    -- Move old data to archive
    WITH moved_rows AS (
        DELETE FROM sensor_readings
        WHERE created_at < CURRENT_TIMESTAMP - (days_old || ' days')::INTERVAL
        RETURNING *
    )
    INSERT INTO sensor_readings_archive
    SELECT * FROM moved_rows;
    
    GET DIAGNOSTICS rows_moved = ROW_COUNT;
    
    RETURN rows_moved;
END;
$$ LANGUAGE plpgsql;

-- Create function for data cleanup (delete very old archived data)
CREATE OR REPLACE FUNCTION cleanup_archived_data(days_old INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM sensor_readings_archive
    WHERE created_at < CURRENT_TIMESTAMP - (days_old || ' days')::INTERVAL;
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get device statistics
CREATE OR REPLACE FUNCTION get_device_stats(device_id_param VARCHAR DEFAULT NULL)
RETURNS TABLE (
    device_id VARCHAR,
    device_type VARCHAR,
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
        MIN(sr.timestamp) as first_reading,
        MAX(sr.timestamp) as last_reading,
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

-- Insert some initial test data (optional)
-- INSERT INTO sensor_readings (device_id, device_type, timestamp, value, unit, latitude, longitude, building, floor, zone, room)
-- VALUES 
--     ('test_device_001', 'temperature_sensor', CURRENT_TIMESTAMP, 22.5, '°C', 60.1699, 24.9384, 'building-1', 1, 'main', 'room-101'),
--     ('test_device_002', 'humidity_sensor', CURRENT_TIMESTAMP, 45.2, '%', 60.1699, 24.9384, 'building-1', 1, 'main', 'room-101');

-- Grant permissions to iot_user on all created objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO iot_user;

-- Display completion message
SELECT 'Database initialization completed successfully!' as message;