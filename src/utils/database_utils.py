"""
Database utility functions for management and maintenance tasks.
"""

import os
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from src.utils.logger import log
from src.config.config import settings
from src.data_storage.database import db_manager


class DatabaseUtils:
    """
    Utility class for database management operations.
    """
    
    @staticmethod
    def create_tables():
        """
        Create database tables if they don't exist.
        """
        try:
            # Read and execute the init.sql file
            init_sql_path = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'init.sql')
            
            if not os.path.exists(init_sql_path):
                log.error(f"Database initialization script not found: {init_sql_path}")
                return False
            
            with open(init_sql_path, 'r') as f:
                sql_content = f.read()
            
            # Split on statement boundaries and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement and not statement.startswith('--'):
                    try:
                        db_manager.execute_non_query(statement)
                    except Exception as e:
                        # Skip statements that might fail (like CREATE EXTENSION if already exists)
                        if 'already exists' not in str(e).lower():
                            log.warning(f"Statement execution warning: {str(e)}")
            
            log.info("Database tables created successfully")
            return True
            
        except Exception as e:
            log.error(f"Error creating database tables: {str(e)}")
            return False
    
    @staticmethod
    def check_table_structure() -> Dict[str, Any]:
        """
        Check the structure of database tables.
        
        Returns:
            Dictionary with table structure information
        """
        try:
            result = {}
            
            # Check main table
            main_table = settings.timescaledb.main_table
            query = """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = :table_name
                ORDER BY ordinal_position
            """
            
            columns = db_manager.execute_query(query, {'table_name': main_table})
            result[main_table] = {
                'exists': len(columns) > 0,
                'column_count': len(columns),
                'columns': columns
            }
            
            # Check archive table
            archive_table = settings.timescaledb.archive_table
            archive_columns = db_manager.execute_query(query, {'table_name': archive_table})
            result[archive_table] = {
                'exists': len(archive_columns) > 0,
                'column_count': len(archive_columns),
                'columns': archive_columns
            }
            
            # Check indexes
            index_query = """
                SELECT 
                    indexname,
                    tablename,
                    indexdef
                FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND tablename IN (:main_table, :archive_table)
                ORDER BY tablename, indexname
            """
            
            indexes = db_manager.execute_query(index_query, {
                'main_table': main_table,
                'archive_table': archive_table
            })
            result['indexes'] = indexes
            
            return result
            
        except Exception as e:
            log.error(f"Error checking table structure: {str(e)}")
            return {}
    
    @staticmethod
    def get_table_stats() -> Dict[str, Any]:
        """
        Get statistics about database tables.
        
        Returns:
            Dictionary with table statistics
        """
        try:
            result = {}
            
            # Main table stats
            main_table = settings.timescaledb.main_table
            main_stats_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT device_id) as unique_devices,
                    COUNT(DISTINCT device_type) as unique_device_types,
                    MIN(timestamp) as earliest_reading,
                    MAX(timestamp) as latest_reading,
                    COUNT(CASE WHEN is_anomaly THEN 1 END) as anomaly_count,
                    AVG(battery_level) as avg_battery_level,
                    pg_size_pretty(pg_total_relation_size('{main_table}')) as table_size
                FROM {main_table}
            """
            
            main_stats = db_manager.execute_query(main_stats_query)
            result[main_table] = main_stats[0] if main_stats else {}
            
            # Archive table stats
            archive_table = settings.timescaledb.archive_table
            try:
                archive_stats_query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        MIN(timestamp) as earliest_reading,
                        MAX(timestamp) as latest_reading,
                        pg_size_pretty(pg_total_relation_size('{archive_table}')) as table_size
                    FROM {archive_table}
                """
                
                archive_stats = db_manager.execute_query(archive_stats_query)
                result[archive_table] = archive_stats[0] if archive_stats else {}
            except:
                result[archive_table] = {'error': 'Table does not exist or is not accessible'}
            
            # Recent data distribution (last 24 hours)
            recent_query = f"""
                SELECT 
                    device_type,
                    COUNT(*) as reading_count,
                    AVG(value) as avg_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value
                FROM {main_table}
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY device_type
                ORDER BY reading_count DESC
            """
            
            recent_stats = db_manager.execute_query(recent_query)
            result['recent_distribution'] = recent_stats
            
            return result
            
        except Exception as e:
            log.error(f"Error getting table statistics: {str(e)}")
            return {}
    
    @staticmethod
    def cleanup_old_data(archive_days: int = None, delete_days: int = None) -> Dict[str, int]:
        """
        Clean up old data by archiving and deleting.
        
        Args:
            archive_days: Days after which to archive data
            delete_days: Days after which to delete archived data
            
        Returns:
            Dictionary with cleanup results
        """
        try:
            archive_days = archive_days or settings.timescaledb.archive_after_days
            delete_days = delete_days or settings.timescaledb.retention_days
            
            log.info(f"Starting data cleanup: archive after {archive_days} days, delete after {delete_days} days")
            
            result = db_manager.cleanup_old_data(archive_days, delete_days)
            
            if result['archived_rows'] > 0:
                log.info(f"Archived {result['archived_rows']} old records")
            
            if result['cleaned_rows'] > 0:
                log.info(f"Deleted {result['cleaned_rows']} very old archived records")
            
            return result
            
        except Exception as e:
            log.error(f"Error during data cleanup: {str(e)}")
            return {'archived_rows': 0, 'cleaned_rows': 0}
    
    @staticmethod
    def vacuum_tables():
        """
        Vacuum database tables to reclaim space and update statistics.
        """
        try:
            main_table = settings.timescaledb.main_table
            archive_table = settings.timescaledb.archive_table
            
            log.info("Starting database vacuum operation...")
            
            # Vacuum main table
            vacuum_query = f"VACUUM ANALYZE {main_table}"
            db_manager.execute_non_query(vacuum_query)
            log.info(f"Vacuumed table: {main_table}")
            
            # Vacuum archive table if it exists
            try:
                vacuum_archive_query = f"VACUUM ANALYZE {archive_table}"
                db_manager.execute_non_query(vacuum_archive_query)
                log.info(f"Vacuumed table: {archive_table}")
            except:
                log.warning(f"Could not vacuum archive table: {archive_table}")
            
            log.info("Database vacuum operation completed")
            
        except Exception as e:
            log.error(f"Error during vacuum operation: {str(e)}")
    
    @staticmethod
    def export_data(
        device_id: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Export data with optional filters.
        
        Args:
            device_id: Optional device ID filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum number of records to return
            
        Returns:
            List of sensor readings
        """
        try:
            main_table = settings.timescaledb.main_table
            
            # Build query with filters
            where_conditions = []
            parameters = {'limit': limit}
            
            if device_id:
                where_conditions.append("device_id = :device_id")
                parameters['device_id'] = device_id
            
            if start_date:
                where_conditions.append("timestamp >= :start_date")
                parameters['start_date'] = start_date
            
            if end_date:
                where_conditions.append("timestamp <= :end_date")
                parameters['end_date'] = end_date
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            query = f"""
                SELECT *
                FROM {main_table}
                {where_clause}
                ORDER BY timestamp DESC
                LIMIT :limit
            """
            
            results = db_manager.execute_query(query, parameters)
            
            log.info(f"Exported {len(results)} records")
            return results
            
        except Exception as e:
            log.error(f"Error exporting data: {str(e)}")
            return []
    
    @staticmethod
    def get_device_summary() -> List[Dict[str, Any]]:
        """
        Get a summary of all devices.
        
        Returns:
            List of device summaries
        """
        try:
            query = "SELECT * FROM device_summary ORDER BY last_reading DESC"
            results = db_manager.execute_query(query)
            
            log.info(f"Retrieved summary for {len(results)} devices")
            return results
            
        except Exception as e:
            log.error(f"Error getting device summary: {str(e)}")
            return []
    
    @staticmethod
    def check_data_integrity() -> Dict[str, Any]:
        """
        Check data integrity and identify potential issues.
        
        Returns:
            Dictionary with integrity check results
        """
        try:
            main_table = settings.timescaledb.main_table
            issues = []
            
            # Check for null device IDs
            null_device_query = f"SELECT COUNT(*) as count FROM {main_table} WHERE device_id IS NULL OR device_id = ''"
            result = db_manager.execute_query(null_device_query)
            null_devices = result[0]['count'] if result else 0
            
            if null_devices > 0:
                issues.append(f"Found {null_devices} records with null/empty device_id")
            
            # Check for future timestamps
            future_query = f"SELECT COUNT(*) as count FROM {main_table} WHERE timestamp > NOW()"
            result = db_manager.execute_query(future_query)
            future_timestamps = result[0]['count'] if result else 0
            
            if future_timestamps > 0:
                issues.append(f"Found {future_timestamps} records with future timestamps")
            
            # Check for invalid battery levels
            invalid_battery_query = f"""
                SELECT COUNT(*) as count 
                FROM {main_table} 
                WHERE battery_level IS NOT NULL AND (battery_level < 0 OR battery_level > 100)
            """
            result = db_manager.execute_query(invalid_battery_query)
            invalid_battery = result[0]['count'] if result else 0
            
            if invalid_battery > 0:
                issues.append(f"Found {invalid_battery} records with invalid battery levels")
            
            # Check for invalid coordinates
            invalid_coords_query = f"""
                SELECT COUNT(*) as count 
                FROM {main_table} 
                WHERE (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
                   OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))
            """
            result = db_manager.execute_query(invalid_coords_query)
            invalid_coords = result[0]['count'] if result else 0
            
            if invalid_coords > 0:
                issues.append(f"Found {invalid_coords} records with invalid coordinates")
            
            # Check for duplicate readings (same device, same timestamp)
            duplicate_query = f"""
                SELECT COUNT(*) as count
                FROM (
                    SELECT device_id, timestamp, COUNT(*)
                    FROM {main_table}
                    GROUP BY device_id, timestamp
                    HAVING COUNT(*) > 1
                ) duplicates
            """
            result = db_manager.execute_query(duplicate_query)
            duplicates = result[0]['count'] if result else 0
            
            if duplicates > 0:
                issues.append(f"Found {duplicates} sets of duplicate readings")
            
            return {
                'healthy': len(issues) == 0,
                'issues': issues,
                'checks_performed': [
                    'null_device_ids',
                    'future_timestamps',
                    'invalid_battery_levels',
                    'invalid_coordinates',
                    'duplicate_readings'
                ]
            }
            
        except Exception as e:
            log.error(f"Error checking data integrity: {str(e)}")
            return {
                'healthy': False,
                'issues': [f"Error during integrity check: {str(e)}"],
                'checks_performed': []
            }


def run_maintenance():
    """
    Run routine database maintenance tasks.
    """
    log.info("Starting database maintenance...")
    
    utils = DatabaseUtils()
    
    # Clean up old data
    cleanup_result = utils.cleanup_old_data()
    
    # Vacuum tables
    utils.vacuum_tables()
    
    # Check integrity
    integrity_result = utils.check_data_integrity()
    
    if not integrity_result['healthy']:
        log.warning("Data integrity issues found:")
        for issue in integrity_result['issues']:
            log.warning(f"  - {issue}")
    
    # Log table statistics
    stats = utils.get_table_stats()
    if stats:
        main_table = settings.timescaledb.main_table
        if main_table in stats:
            main_stats = stats[main_table]
            log.info(f"Table statistics - Rows: {main_stats.get('total_rows', 'N/A')}, "
                   f"Devices: {main_stats.get('unique_devices', 'N/A')}, "
                   f"Size: {main_stats.get('table_size', 'N/A')}")
    
    log.info("Database maintenance completed")


if __name__ == "__main__":
    # Run maintenance when script is executed directly
    run_maintenance()