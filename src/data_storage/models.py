"""
Data models for sensor readings and related entities.
"""

import enum
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, JSON, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class DeviceStatus(enum.Enum):
    """
    Enumeration for device status values.
    """
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    MAINTENANCE = "MAINTENANCE"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class SensorReading(Base):
    """
    SQLAlchemy model for sensor readings table.
    """
    __tablename__ = 'sensor_readings'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Device information
    device_id = Column(String(255), nullable=False, index=True)
    device_type = Column(String(100), nullable=False, index=True)
    
    # Sensor reading data
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    value = Column(Float)
    unit = Column(String(50), nullable=False)
    
    # Location information
    latitude = Column(Float)
    longitude = Column(Float)
    building = Column(String(100))
    floor = Column(Integer)
    zone = Column(String(100))
    room = Column(String(100))
    
    # Device metadata
    battery_level = Column(Float)
    signal_strength = Column(Float)
    firmware_version = Column(String(50))
    is_anomaly = Column(Boolean, default=False, index=True)
    status = Column(SQLEnum(DeviceStatus), default=DeviceStatus.ACTIVE, index=True)
    maintenance_date = Column(DateTime(timezone=True))
    
    # Flexible data storage
    device_metadata = Column(JSONB)
    tags = Column(ARRAY(Text))
    
    # Audit fields
    created_at = Column(DateTime(timezone=True), default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<SensorReading(device_id='{self.device_id}', device_type='{self.device_type}', timestamp='{self.timestamp}', value={self.value})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the model to a dictionary.
        
        Returns:
            Dictionary representation of the sensor reading
        """
        return {
            'id': self.id,
            'device_id': self.device_id,
            'device_type': self.device_type,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'value': self.value,
            'unit': self.unit,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude,
                'building': self.building,
                'floor': self.floor,
                'zone': self.zone,
                'room': self.room
            },
            'battery_level': self.battery_level,
            'signal_strength': self.signal_strength,
            'firmware_version': self.firmware_version,
            'is_anomaly': self.is_anomaly,
            'status': self.status.value if self.status else None,
            'maintenance_date': self.maintenance_date.isoformat() if self.maintenance_date else None,
            'device_metadata': self.device_metadata,
            'tags': self.tags,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class SensorReadingArchive(Base):
    """
    SQLAlchemy model for archived sensor readings.
    """
    __tablename__ = 'sensor_readings_archive'
    
    # Same structure as SensorReading
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(String(255), nullable=False, index=True)
    device_type = Column(String(100), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    value = Column(Float)
    unit = Column(String(50), nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)
    building = Column(String(100))
    floor = Column(Integer)
    zone = Column(String(100))
    room = Column(String(100))
    battery_level = Column(Float)
    signal_strength = Column(Float)
    firmware_version = Column(String(50))
    is_anomaly = Column(Boolean, default=False)
    status = Column(SQLEnum(DeviceStatus), default=DeviceStatus.ACTIVE)
    maintenance_date = Column(DateTime(timezone=True))
    device_metadata = Column(JSONB)
    tags = Column(ARRAY(Text))
    created_at = Column(DateTime(timezone=True), default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), default=func.now())


@dataclass
class SensorReadingDTO:
    """
    Data Transfer Object for sensor readings.
    Used for data validation and transformation.
    """
    device_id: str
    device_type: str
    timestamp: datetime
    value: Optional[float]
    unit: str
    
    # Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    building: Optional[str] = None
    floor: Optional[int] = None
    zone: Optional[str] = None
    room: Optional[str] = None
    
    # Device metadata
    battery_level: Optional[float] = None
    signal_strength: Optional[float] = None
    firmware_version: Optional[str] = None
    is_anomaly: bool = False
    status: DeviceStatus = DeviceStatus.ACTIVE
    maintenance_date: Optional[datetime] = None
    
    # Flexible fields
    device_metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """
        Validate data after initialization.
        """
        # Validate battery level
        if self.battery_level is not None:
            if not (0 <= self.battery_level <= 100):
                raise ValueError(f"Battery level must be between 0 and 100, got {self.battery_level}")
        
        # Validate coordinates
        if (self.latitude is None) != (self.longitude is None):
            raise ValueError("Both latitude and longitude must be provided together or both must be None")
        
        if self.latitude is not None:
            if not (-90 <= self.latitude <= 90):
                raise ValueError(f"Latitude must be between -90 and 90, got {self.latitude}")
        
        if self.longitude is not None:
            if not (-180 <= self.longitude <= 180):
                raise ValueError(f"Longitude must be between -180 and 180, got {self.longitude}")
        
        # Ensure status is DeviceStatus enum
        if isinstance(self.status, str):
            try:
                self.status = DeviceStatus(self.status)
            except ValueError:
                raise ValueError(f"Invalid device status: {self.status}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert DTO to dictionary for database insertion.
        
        Returns:
            Dictionary representation suitable for database operations
        """
        return {
            'device_id': self.device_id,
            'device_type': self.device_type,
            'timestamp': self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            'value': self.value,
            'unit': self.unit,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'building': self.building,
            'floor': self.floor,
            'zone': self.zone,
            'room': self.room,
            'battery_level': self.battery_level,
            'signal_strength': self.signal_strength,
            'firmware_version': self.firmware_version,
            'is_anomaly': self.is_anomaly,
            'status': self.status.value if isinstance(self.status, DeviceStatus) else self.status,
            'maintenance_date': self.maintenance_date.isoformat() if isinstance(self.maintenance_date, datetime) else self.maintenance_date,
            'device_metadata': self.device_metadata,  # Keep as dict, will be converted to JSON in database layer
            'tags': self.tags
        }
    
    @classmethod
    def from_kafka_message(cls, message: Dict[str, Any]) -> 'SensorReadingDTO':
        """
        Create SensorReadingDTO from Kafka message.
        
        Args:
            message: Kafka message data
            
        Returns:
            SensorReadingDTO instance
        """
        # Extract location data
        location = message.get('location', {})
        
        # Parse timestamp
        timestamp_str = message.get('timestamp')
        if isinstance(timestamp_str, str):
            try:
                # Try parsing ISO format
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except ValueError:
                # Fallback to current time
                # timestamp = datetime.utcnow() -- deprecated
                timestamp = datetime.now(datetime.timezone.utc)
        elif isinstance(timestamp_str, datetime):
            timestamp = timestamp_str
        else:
            timestamp = datetime.utcnow()
        
        # Parse maintenance date
        maintenance_date_str = message.get('maintenance_date')
        maintenance_date = None
        if maintenance_date_str:
            try:
                maintenance_date = datetime.fromisoformat(maintenance_date_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass
        
        return cls(
            device_id=message.get('device_id', ''),
            device_type=message.get('device_type', ''),
            timestamp=timestamp,
            value=message.get('value'),
            unit=message.get('unit', ''),
            latitude=location.get('latitude'),
            longitude=location.get('longitude'),
            building=location.get('building'),
            floor=location.get('floor'),
            zone=location.get('zone'),
            room=location.get('room'),
            battery_level=message.get('battery_level'),
            signal_strength=message.get('signal_strength'),
            firmware_version=message.get('firmware_version'),
            is_anomaly=message.get('is_anomaly', False),
            status=message.get('status', 'ACTIVE'),
            maintenance_date=maintenance_date,
            device_metadata=message.get('device_metadata', {}),
            tags=message.get('tags', [])
        )


@dataclass
class DeviceStatistics:
    """
    Data class for device statistics.
    """
    device_id: str
    device_type: str
    total_readings: int
    first_reading: datetime
    last_reading: datetime
    avg_value: Optional[float]
    min_value: Optional[float]
    max_value: Optional[float]
    anomaly_percentage: float
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dictionary representation
        """
        return {
            'device_id': self.device_id,
            'device_type': self.device_type,
            'total_readings': self.total_readings,
            'first_reading': self.first_reading.isoformat() if self.first_reading else None,
            'last_reading': self.last_reading.isoformat() if self.last_reading else None,
            'avg_value': self.avg_value,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'anomaly_percentage': self.anomaly_percentage
        }