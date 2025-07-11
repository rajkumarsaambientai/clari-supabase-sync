#!/usr/bin/env python3
"""
Automated Clari to Supabase Sync
Runs daily to pull new calls from Clari and sync them to Supabase
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from clari_data_importer import ClariDataImporter

# Load environment variables
load_dotenv()

# Setup logging for Render
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Render captures stdout/stderr
    ]
)
logger = logging.getLogger(__name__)

class AutomatedClariSync:
    def __init__(self):
        """Initialize the automated sync system"""
        self.importer = ClariDataImporter()
        self.sync_stats = {
            'new_calls_found': 0,
            'calls_imported': 0,
            'participants_imported': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
    
    def get_existing_call_ids(self):
        """Get all call IDs that already exist in Supabase"""
        try:
            result = self.importer.supabase.table('calls').select('call_id').execute()
            existing_ids = {row['call_id'] for row in result.data}
            logger.info(f"Found {len(existing_ids)} existing calls in Supabase")
            return existing_ids
        except Exception as e:
            logger.error(f"Error fetching existing call IDs: {e}")
            return set()
    
    def fetch_recent_call_ids_from_clari(self, days_back=7):
        """Fetch recent call IDs from Clari API"""
        try:
            # This would need to be implemented based on Clari's API
            # For now, we'll use a placeholder that you can customize
            logger.info(f"Fetching call IDs from last {days_back} days from Clari...")
            
            # Placeholder - you'll need to implement this based on Clari's API
            # This should return a list of call IDs from recent calls
            recent_call_ids = self._fetch_call_ids_from_clari_api(days_back)
            
            logger.info(f"Found {len(recent_call_ids)} recent calls from Clari")
            return recent_call_ids
            
        except Exception as e:
            logger.error(f"Error fetching recent call IDs from Clari: {e}")
            return []
    
    def _fetch_call_ids_from_clari_api(self, days_back):
        """
        Fetch call IDs from Clari API
        You'll need to implement this based on Clari's API documentation
        """
        # Placeholder implementation - replace with actual Clari API call
        import requests
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # This is a placeholder - you'll need to implement the actual API call
        # based on Clari's API documentation
        url = f"{self.importer.clari_headers['BASE_URL']}/calls"
        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'limit': 1000  # Adjust based on your needs
        }
        
        try:
            response = requests.get(url, headers=self.importer.clari_headers, params=params)
            if response.status_code == 200:
                data = response.json()
                # Extract call IDs from response - adjust based on actual API response structure
                call_ids = [call['id'] for call in data.get('calls', [])]
                return call_ids
            else:
                logger.error(f"Clari API error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error calling Clari API: {e}")
            return []
    
    def sync_new_calls(self, days_back=7):
        """Main sync function - finds and imports new calls"""
        logger.info("Starting automated Clari sync...")
        self.sync_stats['start_time'] = datetime.now()
        
        try:
            # Get existing call IDs from Supabase
            existing_call_ids = self.get_existing_call_ids()
            
            # Get recent call IDs from Clari
            recent_call_ids = self.fetch_recent_call_ids_from_clari(days_back)
            
            # Find new calls (not already in Supabase)
            new_call_ids = [call_id for call_id in recent_call_ids if call_id not in existing_call_ids]
            
            self.sync_stats['new_calls_found'] = len(new_call_ids)
            logger.info(f"Found {len(new_call_ids)} new calls to import")
            
            if not new_call_ids:
                logger.info("No new calls to import")
                return
            
            # Import new calls
            successful, failed = self.importer.import_call_data(new_call_ids)
            
            self.sync_stats['calls_imported'] = successful
            self.sync_stats['errors'] = failed
            
            logger.info(f"Sync completed: {successful} calls imported, {failed} failed")
            
        except Exception as e:
            logger.error(f"Error during sync: {e}")
            self.sync_stats['errors'] += 1
        
        finally:
            self.sync_stats['end_time'] = datetime.now()
            self._log_sync_summary()
    
    def _log_sync_summary(self):
        """Log a summary of the sync operation"""
        duration = self.sync_stats['end_time'] - self.sync_stats['start_time']
        
        logger.info("=" * 50)
        logger.info("SYNC SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Duration: {duration}")
        logger.info(f"New calls found: {self.sync_stats['new_calls_found']}")
        logger.info(f"Calls imported: {self.sync_stats['calls_imported']}")
        logger.info(f"Errors: {self.sync_stats['errors']}")
        logger.info("=" * 50)
    
    def run_daily_sync(self):
        """Run the daily sync (defaults to last 7 days)"""
        self.sync_new_calls(days_back=7)
    
    def run_sample_sync(self, days=7):
        """Run a sample sync for testing with detailed results"""
        logger.info(f"Starting sample sync for last {days} days...")
        
        try:
            # Get existing call IDs from Supabase
            existing_call_ids = self.get_existing_call_ids()
            
            # Get recent call IDs from Clari
            recent_call_ids = self.fetch_recent_call_ids_from_clari(days)
            
            # Find new calls (not already in Supabase)
            new_call_ids = [call_id for call_id in recent_call_ids if call_id not in existing_call_ids]
            
            logger.info(f"Sample sync: Found {len(recent_call_ids)} total calls, {len(new_call_ids)} new calls to import")
            
            result = {
                'total_calls': len(recent_call_ids),
                'existing_calls': len(existing_call_ids),
                'new_calls': len(new_call_ids),
                'imported_calls': 0,
                'failed_calls': 0
            }
            
            if new_call_ids:
                # Import new calls
                successful, failed = self.importer.import_call_data(new_call_ids)
                result['imported_calls'] = successful
                result['failed_calls'] = failed
                
                logger.info(f"Sample sync completed: {successful} imported, {failed} failed")
            else:
                logger.info("Sample sync: No new calls to import")
            
            return result
            
        except Exception as e:
            logger.error(f"Error during sample sync: {e}")
            return {
                'error': str(e),
                'total_calls': 0,
                'imported_calls': 0,
                'failed_calls': 0
            }

def main():
    """Main function to run the automated sync"""
    
    # Check if required environment variables are set
    required_vars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please set these in your .env file")
        sys.exit(1)
    
    # Create and run the sync
    sync = AutomatedClariSync()
    sync.run_daily_sync()

if __name__ == "__main__":
    main() 