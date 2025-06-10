# #!/usr/bin/env python3
# """
# Database management script for IoT Data Pipeline.

# This script provides various database management operations:
# - Initialize database tables
# - Check database health
# - Export data
# - Run maintenance tasks
# - View statistics
# """

# import argparse
# import sys
# import json
# from datetime import datetime, timedelta
# from typing import Any, Dict

# from src.utils.logger import log
# from src.config.config import settings
# from src.data_storage.database import db_manager
# from src.utils.database_utils import DatabaseUtils, run_maintenance


# def init_database():
#     """
#     Initialize the database tables and structure.
#     """
#     print("Initializing database...")
    
#     if not db_manager.wait_for_database():
#         print("ERROR: Database is not available")
#         return False
    
#     utils = DatabaseUtils()
#     success = utils.create_tables()
    
#     if success:
#         print("Database initialized successfully")
        
#         # Check table structure
#         structure = utils.check_table_structure()
#         main_table = settings.postgresql.main_table
#         archive_table = settings.postgresql.archive_table
        
#         if structure.get(main_table, {}).get('exists'):
#             columns = structure[main_table]['column_count']
#             print(f"Main table '{main_table}' created with {columns} columns")
        
#         if structure.get(archive_table, {}).get('exists'):
#             columns = structure[archive_table]['column_count']
#             print(f"Archive table '{archive_table}' created with {columns} columns")
        
#         return True
#     else:
#         print("ERROR: Failed to initialize database")
#         return False


# def check_health():
#     """
#     Check database health and connectivity.
#     """
#     print("Checking database health...")
    
#     # Test connectivity
#     if not db_manager.test_connection():
#         print("ERROR: Cannot connect to database")
#         return False
    
#     print("✓ Database connection: OK")
    
#     # Check table structure
#     utils = DatabaseUtils()
#     structure = utils.check_table_structure()
    
#     main_table = settings.postgresql.main_table
#     archive_table = settings.postgresql.archive_table
    
#     if structure.get(main_table, {}).get('exists'):
#         print(f"✓ Main table '{main_table}': OK")
#     else:
#         print(f"✗ Main table '{main_table}': MISSING")
    
#     if structure.get(archive_table, {}).get('exists'):
#         print(f"✓ Archive table '{archive_table}': OK")
#     else:
#         print(f"✗ Archive table '{archive_table}': MISSING")
    
#     # Check data integrity
#     integrity = utils.check_data_integrity()
#     if integrity['healthy']:
#         print("✓ Data integrity: OK")
#     else:
#         print("✗ Data integrity: ISSUES FOUND")
#         for issue in integrity['issues']:
#             print(f"  - {issue}")
    
#     # Get basic statistics
#     stats = utils.get_table_stats()
#     if stats and main_table in stats:
#         main_stats = stats[main_table]
#         print(f"\nDatabase Statistics:")
#         print(f"  Total readings: {main_stats.get('total_rows', 'N/A')}")
#         print(f"  Unique devices: {main_stats.get('unique_devices', 'N/A')}")
#         print(f"  Device types: {main_stats.get('unique_device_types', 'N/A')}")
#         print(f"  Table size: {main_stats.get('table_size', 'N/A')}")
        
#         if main_stats.get('earliest_reading'):
#             print(f"  Earliest reading: {main_stats['earliest_reading']}")
#         if main_stats.get('latest_reading'):
#             print(f"  Latest reading: {main_stats['latest_reading']}")
        
#         anomaly_count = main_stats.get('anomaly_count', 0)
#         total_rows = main_stats.get('total_rows', 0)
#         if total_rows > 0:
#             anomaly_rate = (anomaly_count / total_rows) * 100
#             print(f"  Anomaly rate: {anomaly_rate:.2f}% ({anomaly_count} anomalies)")
    
#     return True


# def show_statistics(detailed=False):
#     """
#     Show database statistics.
    
#     Args:
#         detailed: Whether to show detailed statistics
#     """
#     print("Database Statistics")
#     print("=" * 50)
    
#     utils = DatabaseUtils()
#     stats = utils.get_table_stats()
    
#     if not stats:
#         print("No statistics available")
#         return
    
#     main_table = settings.postgresql.main_table
#     archive_table = settings.postgresql.archive_table
    
#     # Main table stats
#     if main_table in stats:
#         main_stats = stats[main_table]
#         print(f"\n{main_table.upper()}:")
#         print(f"  Total readings: {main_stats.get('total_rows', 'N/A'):,}")
#         print(f"  Unique devices: {main_stats.get('unique_devices', 'N/A'):,}")
#         print(f"  Device types: {main_stats.get('unique_device_types', 'N/A'):,}")
#         print(f"  Table size: {main_stats.get('table_size', 'N/A')}")
        
#         if main_stats.get('earliest_reading'):
#             print(f"  Data range: {main_stats['earliest_reading']} to {main_stats['latest_reading']}")
        
#         anomaly_count = main_stats.get('anomaly_count', 0)
#         print(f"  Anomalies: {anomaly_count:,}")
        
#         avg_battery = main_stats.get('avg_battery_level')
#         if avg_battery:
#             print(f"  Avg battery level: {avg_battery:.1f}%")
    
#     # Archive table stats
#     if archive_table in stats and not stats[archive_table].get('error'):
#         archive_stats = stats[archive_table]
#         print(f"\n{archive_table.upper()}:")
#         print(f"  Archived readings: {archive_stats.get('total_rows', 'N/A'):,}")
#         print(f"  Archive size: {archive_stats.get('table_size', 'N/A')}")
        
#         if archive_stats.get('earliest_reading'):
#             print(f"  Archive range: {archive_stats['earliest_reading']} to {archive_stats['latest_reading']}")
    
#     # Recent data distribution
#     if 'recent_distribution' in stats and stats['recent_distribution']:
#         print(f"\nRECENT DATA (Last 24 hours):")
#         for dist in stats['recent_distribution']:
#             device_type = dist['device_type']
#             count = dist['reading_count']
#             avg_val = dist.get('avg_value')
#             min_val = dist.get('min_value')
#             max_val = dist.get('max_value')
            
#             if avg_val is not None:
#                 print(f"  {device_type}: {count:,} readings (avg: {avg_val:.2f}, range: {min_val:.2f}-{max_val:.2f})")
#             else:
#                 print(f"  {device_type}: {count:,} readings")
    
#     if detailed:
#         # Device summary
#         device_summary = utils.get_device_summary()
#         if device_summary:
#             print(f"\nDEVICE SUMMARY:")
#             print(f"{'Device ID':<30} {'Type':<20} {'Readings':<10} {'Last Reading':<20} {'Battery':<8}")
#             print("-" * 90)
            
#             for device in device_summary[:10]:  # Show top 10 devices
#                 device_id = device['device_id'][:28]
#                 device_type = device['device_type'][:18]
#                 readings = device['total_readings']
#                 last_reading = device['last_reading']
#                 battery = device.get('latest_battery_level')
#                 battery_str = f"{battery:.1f}%" if battery else "N/A"
                
#                 print(f"{device_id:<30} {device_type:<20} {readings:<10} {last_reading:<20} {battery_str:<8}")
            
#             if len(device_summary) > 10:
#                 print(f"... and {len(device_summary) - 10} more devices")


# def export_data(device_id=None, days=None, limit=1000, output_file=None):
#     """
#     Export data to JSON file.
    
#     Args:
#         device_id: Optional device ID filter
#         days: Number of days back to export
#         limit: Maximum number of records
#         output_file: Output file path
#     """
#     print("Exporting data...")
    
#     utils = DatabaseUtils()
    
#     # Calculate date range
#     end_date = datetime.now()
#     start_date = None
#     if days:
#         start_date = end_date - timedelta(days=days)
    
#     # Export data
#     data = utils.export_data(
#         device_id=device_id,
#         start_date=start_date,
#         end_date=end_date,
#         limit=limit
#     )
    
#     if not data:
#         print("No data found matching the criteria")
#         return
    
#     # Convert datetime objects to strings for JSON serialization
#     for record in data:
#         for key, value in record.items():
#             if isinstance(value, datetime):
#                 record[key] = value.isoformat()
    
#     # Output to file or stdout
#     if output_file:
#         with open(output_file, 'w') as f:
#             json.dump(data, f, indent=2, default=str)
#         print(f"Exported {len(data)} records to {output_file}")
#     else:
#         print(json.dumps(data, indent=2, default=str))


# def run_cleanup(archive_days=None, delete_days=None):
#     """
#     Run data cleanup operations.
    
#     Args:
#         archive_days: Days after which to archive data
#         delete_days: Days after which to delete archived data
#     """
#     print("Running data cleanup...")
    
#     utils = DatabaseUtils()
#     result = utils.cleanup_old_data(archive_days, delete_days)
    
#     print(f"Cleanup completed:")
#     print(f"  Archived: {result['archived_rows']} records")
#     print(f"  Deleted: {result['cleaned_rows']} archived records")


# def run_vacuum():
#     """
#     Run database vacuum operation.
#     """
#     print("Running database vacuum...")
    
#     utils = DatabaseUtils()
#     utils.vacuum_tables()
    
#     print("Vacuum operation completed")


# def main():
#     """
#     Main function to handle command line arguments.
#     """
#     parser = argparse.ArgumentParser(description='IoT Database Management Tool')
    
#     subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
#     # Initialize command
#     init_parser = subparsers.add_parser('init', help='Initialize database tables')
    
#     # Health check command
#     health_parser = subparsers.add_parser('health', help='Check database health')
    
#     # Statistics command
#     stats_parser = subparsers.add_parser('stats', help='Show database statistics')
#     stats_parser.add_argument('--detailed', action='store_true', help='Show detailed statistics')
    
#     # Export command
#     export_parser = subparsers.add_parser('export', help='Export data to JSON')
#     export_parser.add_argument('--device-id', help='Filter by device ID')
#     export_parser.add_argument('--days', type=int, help='Number of days back to export')
#     export_parser.add_argument('--limit', type=int, default=1000, help='Maximum number of records')
#     export_parser.add_argument('--output', help='Output file path (default: stdout)')
    
#     # Cleanup command
#     cleanup_parser = subparsers.add_parser('cleanup', help='Run data cleanup')
#     cleanup_parser.add_argument('--archive-days', type=int, help='Days after which to archive data')
#     cleanup_parser.add_argument('--delete-days', type=int, help='Days after which to delete archived data')
    
#     # Vacuum command
#     vacuum_parser = subparsers.add_parser('vacuum', help='Run database vacuum')
    
#     # Maintenance command
#     maintenance_parser = subparsers.add_parser('maintenance', help='Run routine maintenance')
    
#     args = parser.parse_args()
    
#     if not args.command:
#         parser.print_help()
#         return
    
#     # Log configuration
#     print(f"Database: {settings.postgresql.database_url.split('@')[1]}")  # Hide password
#     print()
    
#     try:
#         if args.command == 'init':
#             success = init_database()
#             sys.exit(0 if success else 1)
        
#         elif args.command == 'health':
#             success = check_health()
#             sys.exit(0 if success else 1)
        
#         elif args.command == 'stats':
#             show_statistics(detailed=args.detailed)
        
#         elif args.command == 'export':
#             export_data(
#                 device_id=args.device_id,
#                 days=args.days,
#                 limit=args.limit,
#                 output_file=args.output
#             )
        
#         elif args.command == 'cleanup':
#             run_cleanup(
#                 archive_days=args.archive_days,
#                 delete_days=args.delete_days
#             )
        
#         elif args.command == 'vacuum':
#             run_vacuum()
        
#         elif args.command == 'maintenance':
#             run_maintenance()
    
#     except KeyboardInterrupt:
#         print("\nOperation cancelled by user")
#         sys.exit(1)
    
#     except Exception as e:
#         print(f"ERROR: {str(e)}")
#         log.error(f"Database management error: {str(e)}")
#         sys.exit(1)


# if __name__ == "__main__":
#     main()