import json
import os
import requests
from typing import Dict, Any, Optional, List
import avro.schema
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer 
from confluent_kafka.serialization import SerializationContext, MessageField

from src.utils.logger import log
from src.config.config import settings

class SchemaRegistry:
    """
    Helper class for interacting with the Schema Registry.
    
    This class provides methods for schema management, serialization,
    and deserialization using Avro and the Schema Registry.
    """
    
    def __init__(self, schema_registry_url: str = None):
        """
        Initialize the Schema Registry client.
        
        Args:
            schema_registry_url: URL of the Schema Registry service
        """
        self.schema_registry_url = schema_registry_url or settings.schema_registry.url
        self.sr_config = {'url': self.schema_registry_url}
        self.schema_registry = SchemaRegistryClient(self.sr_config)
        
        log.info(f"Schema Registry client initialized with URL: {self.schema_registry_url}")
        
        # Load schemas
        self._load_schemas()
    
    def _load_schemas(self):
        """
        Load Avro schemas from files.
        """
        try:
            # Load IoT sensor reading schema
            self.sensor_schema_path = settings.schema_registry.sensor_schema_path
            
            if not os.path.exists(self.sensor_schema_path):
                log.error(f"Schema file not found: {self.sensor_schema_path}")
                raise FileNotFoundError(f"Schema file not found: {self.sensor_schema_path}")
            
            with open(self.sensor_schema_path, 'r') as f:
                self.sensor_schema_str = f.read()
                self.sensor_schema = avro.schema.parse(self.sensor_schema_str)
                
            log.info(f"Loaded sensor schema from: {self.sensor_schema_path}")
            
            # Initialize sensor serializer and deserializer
            self._init_serializers()
            
        except Exception as e:
            log.error(f"Error loading schemas: {str(e)}")
            raise
    
    def _init_serializers(self):
        """
        Initialize Avro serializers and deserializers.
        """
        try:
            # Sensor reading serializer
            self.sensor_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry,
                schema_str=self.sensor_schema_str,
                to_dict=self._sensor_to_dict
            )
            
            # Sensor reading deserializer
            self.sensor_deserializer = AvroDeserializer(
                schema_registry_client=self.schema_registry,
                schema_str=self.sensor_schema_str,
                from_dict=self._dict_to_sensor
            )
            
            log.info("Initialized Avro serializers and deserializers")
        
        except Exception as e:
            log.error(f"Error initializing serializers: {str(e)}")
            raise
    
    def _sensor_to_dict(self, sensor_reading: Dict[str, Any], ctx) -> Dict[str, Any]:
        """
        Convert a sensor reading dictionary to a format compatible with Avro serialization.
        
        Args:
            sensor_reading: Sensor reading data
            ctx: Serialization context
            
        Returns:
            Dictionary formatted for Avro serialization
        """
        # Make a copy to avoid modifying the original
        avro_reading = dict(sensor_reading)
        
        # Ensure all fields are present with appropriate types
        if 'metadata' not in avro_reading or avro_reading['metadata'] is None:
            avro_reading['metadata'] = {}
            
        # Convert any None values to appropriate defaults for required fields
        if avro_reading.get('signal_strength') is None:
            avro_reading['signal_strength'] = None
            
        if avro_reading.get('firmware_version') is None:
            avro_reading['firmware_version'] = None
            
        # Make sure location fields are properly set
        location = avro_reading.get('location', {})
        if 'zone' not in location or location['zone'] is None:
            location['zone'] = None
        
        avro_reading['location'] = location
            
        return avro_reading
    
    def _dict_to_sensor(self, avro_reading: Dict[str, Any], ctx) -> Dict[str, Any]:
        """
        Convert an Avro-deserialized dictionary to a sensor reading.
        
        Args:
            avro_reading: Avro-deserialized data
            ctx: Deserialization context
            
        Returns:
            Sensor reading dictionary
        """
        # This is a simple pass-through for now, but can be extended
        # to handle any special conversion needed when deserializing
        return avro_reading
    
    def register_schema(self, subject: str, schema_str: str) -> Optional[int]:
        """
        Register a schema with the Schema Registry.
        
        Args:
            subject: Subject name (typically topic-value)
            schema_str: Avro schema as a string
            
        Returns:
            Schema ID
        """
        try:
            # Check if auto-registration is enabled
            if not settings.schema_registry.auto_register_schemas:
                log.warning(f"Auto schema registration is disabled. Skipping registration for {subject}")
                return None
                
            # Check if schema already exists
            try:
                # Check latest version of schema
                latest_schema = self.schema_registry.get_latest_version(subject)
                log.info(f"Schema already exists for subject {subject} with ID: {latest_schema.schema_id}")
                return latest_schema.schema_id
            except Exception as e:
                log.debug(f"No existing schema found for subject {subject}. Proceeding to register new schema.")
            
            # Properly instantiate Schema object
            schema = Schema(schema_str, schema_type="AVRO")

            schema_id = self.schema_registry.register_schema(subject, schema)            

            log.info(f"Schema registered with ID: {schema_id}")
            return schema_id

        except Exception as e:
            log.error(f"Error registering schema: {str(e)}")
            raise
    
    def get_schema(self, subject: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """
        Get a schema from the Schema Registry.
        
        Args:
            subject: Subject name
            version: Schema version (default: "latest")
            
        Returns:
            Schema information or None if not found
        """
        try:
            if version == "latest":
                metadata = self.schema_registry.get_latest_version(subject)
                return {
                    "subject": subject, 
                    "version": metadata.version, 
                    "id": metadata.schema_id,
                    "schema": metadata.schema.schema_str
                }
            else:
                return self.schema_registry.get_version(subject, version)
        except Exception as e:
            log.error(f"Error getting schema: {str(e)}")
            return None
    
    def get_subjects(self) -> List[str]:
        """
        Get all subjects from the Schema Registry.
        
        Returns:
            List of subject names
        """
        try:
            return self.schema_registry.get_subjects()
        except Exception as e:
            log.error(f"Error getting subjects: {str(e)}")
            return []
    
    def delete_schema(self, subject: str, version: str = None) -> bool:
        """
        Delete a schema from the Schema Registry.
        
        Args:
            subject: Subject name
            version: Schema version (default: None, which deletes all versions)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if version is None:
                self.schema_registry.delete_subject(subject)
                log.info(f"Deleted all versions of subject: {subject}")
            else:
                self.schema_registry.delete_version(subject, version)
                log.info(f"Deleted version {version} of subject: {subject}")
            return True
        except Exception as e:
            log.error(f"Error deleting schema: {str(e)}")
            return False
    
    def check_compatibility(self, subject: str, schema_str: str) -> bool:
        """
        Check if a schema is compatible with the latest version.
        
        Args:
            subject: Subject name
            schema_str: Avro schema as a string
            
        Returns:
            True if compatible, False otherwise
        """
        try:
            return self.schema_registry.test_compatibility(subject, avro.schema.parse(schema_str))
        except Exception as e:
            log.error(f"Error checking compatibility: {str(e)}")
            return False
    
    def serialize_sensor_reading(self, reading: Dict[str, Any], topic: str) -> bytes:
        """
        Serialize a sensor reading using Avro and Schema Registry.
        
        Args:
            reading: Sensor reading data
            topic: Kafka topic name
            
        Returns:
            Serialized data as bytes
        """
        try:
            # Register schema if needed
            subject = f"{topic}-value"
            self.register_schema(subject, self.sensor_schema_str)
            
            # Create serialization context
            ctx = SerializationContext(topic, MessageField.VALUE)
            
            # Serialize the reading
            return self.sensor_serializer(reading, ctx)
            
        except Exception as e:
            log.error(f"Error serializing sensor reading: {str(e)}")
            raise
    
    def deserialize_sensor_reading(self, data: bytes, topic: str) -> Dict[str, Any]:
        """
        Deserialize Avro-encoded sensor reading data.
        
        Args:
            data: Serialized data as bytes
            topic: Kafka topic name
            
        Returns:
            Deserialized sensor reading
        """
        try:
            # Create deserialization context
            ctx = SerializationContext(topic, MessageField.VALUE)
            
            # Deserialize the data
            return self.sensor_deserializer(data, ctx)
            
        except Exception as e:
            log.error(f"Error deserializing sensor reading: {str(e)}")
            raise

# Singleton instance
schema_registry = SchemaRegistry()