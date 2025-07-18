import requests
import json
import csv
import os
import time
from datetime import datetime, timedelta
from supabase import create_client, Client
import logging
from participant_mapper import ParticipantMapper

# Configuration
CLARI_API_KEY = os.environ.get('CLARI_API_KEY', "1aAo6cu02x3eVFeHqK51O8GbW2CkXA8y4sMOUx1d")
CLARI_API_PASSWORD = "732f1e7a-5f7f-435e-a701-558467bd70cc"
CLARI_BASE_URL = "https://rest-api.copilot.clari.com"

# Supabase configuration from environment variables
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clari_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClariDataImporter:
    def __init__(self, mapping_file_path: str = None):
        self.clari_headers = {
            "X-Api-Key": CLARI_API_KEY,
            "X-Api-Password": CLARI_API_PASSWORD,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Initialize Supabase client
        if SUPABASE_URL and SUPABASE_KEY:
            self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        else:
            self.supabase = None
        
        # Initialize participant mapper
        self.participant_mapper = ParticipantMapper(mapping_file_path)
        self.current_call_id = None
        
    def fetch_call_details(self, call_id, max_retries=3):
        """Fetch detailed call information from Clari API"""
        url = f"{CLARI_BASE_URL}/call-details?id={call_id}"
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=self.clari_headers)
                if response.status_code == 200:
                    data = response.json()
                    # The call data is nested under 'call' key, just like in your working sample
                    call_data = data.get('call', {})
                    logger.info(f"Call {call_id}: Successfully fetched call data with {len(call_data)} keys")
                    return call_data
                elif response.status_code in (429, 500, 502, 503, 504):
                    logger.warning(f"Call {call_id}: API status {response.status_code}, attempt {attempt}/{max_retries}")
                    time.sleep(30)
                else:
                    logger.error(f"Call {call_id}: API status {response.status_code}")
                    return None
            except Exception as e:
                logger.error(f"Call {call_id}: {e}")
                time.sleep(30)
        
        logger.error(f"Call {call_id}: Failed after {max_retries} attempts")
        return None
    
    def transform_clari_data(self, call_id, call_data):
        """Transform Clari data to match our database schema - using the working sample approach"""
        # Handle cases where CRM info might be empty or missing
        crm_info = call_data.get('crm_info', {})
        summary = call_data.get('summary', {})
        
        # Extract data using the same approach as your working sample script
        opportunity_ids = crm_info.get('deal_id', '')
        account_ids = crm_info.get('account_id', '')
        contact_ids = ','.join(crm_info.get('contact_ids', [])) if isinstance(crm_info.get('contact_ids', []), list) else crm_info.get('contact_ids', '')
        deal_stage_live = call_data.get('deal_stage_live', '')
        
        # Extract summary data like your working script
        full_summary = summary.get('full_summary', '')
        key_takeaways = summary.get('key_takeaways', '')
        if isinstance(key_takeaways, list):
            key_takeaways_str = '\n'.join(f'- {item}' for item in key_takeaways)
        else:
            key_takeaways_str = key_takeaways
            
        topics_discussed = summary.get('topics_discussed', [])
        topics_json = json.dumps([
            {'name': t.get('name', ''), 'summary': t.get('summary', '')}
            for t in topics_discussed
        ])
        
        key_action_items = summary.get('key_action_items', [])
        action_items_json = json.dumps([
            {'action_item': a.get('action_item', ''), 'owner': a.get('owner_name', '')}
            for a in key_action_items
        ])
        
        # Build the call record with the fields that were working in your sample
        call_record = {
            'call_id': call_id,
            
            # Basic CRM data (with fallbacks for missing data)
            'contact_title': crm_info.get('contact_title', ''),
            'customer_prospect_name': crm_info.get('account_name', '') or 'Unknown Account',
            'account_type': self._determine_account_type(crm_info),
            'account_industry': crm_info.get('account_industry', ''),
            'account_annual_revenue': self._parse_revenue(crm_info.get('account_annual_revenue')),
            'account_id': account_ids or f'unknown_{call_id}',
            
            # Opportunity data (from your working sample)
            'opp_id_sfdc': opportunity_ids,
            'deal_stage_before': call_data.get('deal_stage_before', ''),
            'deal_stage_after': call_data.get('deal_stage_after', ''),
            'deal_stage_current': deal_stage_live,
            'opportunity_amount': self._parse_amount(crm_info.get('deal_amount')),
            'opportunity_age': self._calculate_opportunity_age(crm_info.get('deal_created_date')),
            'close_date': self._parse_date(crm_info.get('deal_close_date')),
            'created_date': self._parse_date(crm_info.get('deal_created_date')),
            'first_meeting_source': crm_info.get('first_meeting_source', ''),
            'marketing_source': crm_info.get('marketing_source', ''),
            'call_datetime_gmt': self._parse_datetime(call_data.get('call_datetime')),
            'opportunity_primary_campaign_source': crm_info.get('primary_campaign_source', ''),
            'opportunity_type': crm_info.get('deal_type', ''),
            'opportunity_contracted_arr': self._parse_amount(crm_info.get('contracted_arr')),
            
            # Call metrics (basic ones that should exist)
            'duration_seconds': call_data.get('duration_seconds', 0),
            'talk_listen_ratio': call_data.get('talk_listen_ratio', 0.0),
            'longest_monologue': call_data.get('longest_monologue', 0),
            'interactivity_score': call_data.get('interactivity_score', 0.0),
            'engaging_question_count': call_data.get('engaging_question_count', 0),
            
            # Narrative fields (from your working sample)
            'full_summary': full_summary,
            'key_takeaways': key_takeaways_str,
            'topics_discussed': topics_json,
            'key_action_items': action_items_json,
            'transcript': call_data.get('transcript', ''),
            
            # System metadata
            'source_system': 'clari'
        }
        
        return call_record
    
    def extract_participant_data(self, call_id, call_data):
        """Extract participant information from call data"""
        participants = []
        transcript = call_data.get('transcript', [])
        crm_info = call_data.get('crm_info', {})
        
        # Get contact IDs from CRM info
        contact_ids = crm_info.get('contact_ids', [])
        if isinstance(contact_ids, str):
            contact_ids = [contact_ids] if contact_ids else []
        
        # Extract unique person IDs from transcript
        person_ids = set()
        for utterance in transcript:
            if isinstance(utterance, dict) and 'personId' in utterance:
                person_ids.add(str(utterance['personId']))
        
        # Create participant records matching your existing table structure
        for person_id in person_ids:
            participant = {
                'call_id': call_id,
                'participant_name': self._get_participant_name(person_id, call_id),
                'participant_role': self._determine_participant_role(person_id, crm_info),
                'participant_type': self._determine_participant_type(person_id, crm_info),
                'company': crm_info.get('account_name', ''),
                'email': self._get_participant_email(person_id, call_id)
                # Note: created_at will be set automatically by the database
            }
            participants.append(participant)
        
        return participants
    
    def _get_participant_name(self, person_id, call_id):
        """Get participant name from mapping or generate one"""
        return self.participant_mapper.get_participant_name(call_id, person_id)
    
    def _get_participant_email(self, person_id, call_id):
        """Get participant email if available"""
        return self.participant_mapper.get_participant_email(call_id, person_id)
    
    def _determine_participant_type(self, person_id, crm_info):
        """Determine if participant is internal or external"""
        account_name = crm_info.get('account_name', '')
        call_id = self.current_call_id or 'unknown'
        return self.participant_mapper.determine_participant_type(call_id, person_id, account_name)
    
    def _determine_participant_role(self, person_id, crm_info):
        """Determine participant role in the call"""
        account_name = crm_info.get('account_name', '')
        call_id = self.current_call_id or 'unknown'
        return self.participant_mapper.determine_participant_role(call_id, person_id, account_name)
    
    def _determine_account_type(self, crm_info):
        """Determine account type based on available data"""
        if crm_info.get('account_type'):
            return crm_info.get('account_type')
        elif crm_info.get('deal_stage') in ['Closed Won', 'Closed Lost']:
            return 'Customer'
        elif not crm_info:  # No CRM data available
            return 'Unknown'
        else:
            return 'Prospect'
    
    def _parse_revenue(self, revenue_str):
        """Parse revenue string to integer"""
        if not revenue_str:
            return None
        try:
            # Remove currency symbols and commas
            cleaned = str(revenue_str).replace('$', '').replace(',', '').replace(' ', '')
            return int(float(cleaned))
        except:
            return None
    
    def _parse_amount(self, amount_str):
        """Parse amount string to decimal"""
        if not amount_str:
            return None
        try:
            cleaned = str(amount_str).replace('$', '').replace(',', '').replace(' ', '')
            return float(cleaned)
        except:
            return None
    
    def _parse_date(self, date_str):
        """Parse date string to date object"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def _parse_datetime(self, datetime_str):
        """Parse datetime string to timestamp"""
        if not datetime_str:
            return None
        try:
            return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        except:
            return None
    
    def _calculate_opportunity_age(self, created_date_str):
        """Calculate opportunity age in days"""
        if not created_date_str:
            return None
        try:
            created_date = datetime.strptime(created_date_str, '%Y-%m-%d')
            return (datetime.now() - created_date).days
        except:
            return None
    
    def _count_topic_mentions(self, topics, keywords):
        """Count mentions of specific topics"""
        if not topics:
            return 0
        count = 0
        import re
        pattern = re.compile(keywords, re.IGNORECASE)
        for topic in topics:
            if isinstance(topic, dict):
                topic_text = f"{topic.get('name', '')} {topic.get('summary', '')}"
            else:
                topic_text = str(topic)
            if pattern.search(topic_text):
                count += 1
        return count
    
    def _format_takeaways(self, takeaways):
        """Format key takeaways as string"""
        if not takeaways:
            return ''
        if isinstance(takeaways, list):
            return '\n'.join(f'- {item}' for item in takeaways)
        return str(takeaways)
    
    def import_call_data(self, call_ids):
        """Import call data for given call IDs"""
        logger.info(f"Starting import of {len(call_ids)} calls")
        
        successful_imports = 0
        failed_imports = 0
        
        for idx, call_id in enumerate(call_ids):
            try:
                logger.info(f"Processing call {idx+1}/{len(call_ids)}: {call_id}")
                
                # Set current call ID for participant mapping
                self.current_call_id = call_id
                
                # Fetch call details from Clari
                call_data = self.fetch_call_details(call_id)
                if not call_data:
                    logger.warning(f"Skipping call {call_id} (no data)")
                    failed_imports += 1
                    continue
                
                # Transform data to match our schema
                call_record = self.transform_clari_data(call_id, call_data)
                
                # Extract participant data
                participants = self.extract_participant_data(call_id, call_data)
                
                # Check if call already exists
                existing = self.supabase.table('calls').select('id').eq('call_id', call_id).execute()
                if existing.data:
                    logger.info(f"Call {call_id} already exists, updating...")
                    # Update existing record
                    result = self.supabase.table('calls').update(call_record).eq('call_id', call_id).execute()
                    
                    # Update participants (delete old ones and insert new ones)
                    if participants:
                        self.supabase.table('call_participants').delete().eq('call_id', call_id).execute()
                        if participants:
                            self.supabase.table('call_participants').insert(participants).execute()
                            logger.info(f"Updated {len(participants)} participants for call {call_id}")
                else:
                    logger.info(f"Inserting new call {call_id}")
                    # Insert new record
                    result = self.supabase.table('calls').insert(call_record).execute()
                    
                    # Insert participants
                    if participants:
                        self.supabase.table('call_participants').insert(participants).execute()
                        logger.info(f"Inserted {len(participants)} participants for call {call_id}")
                
                if result.data:
                    successful_imports += 1
                    logger.info(f"Successfully processed call {call_id}")
                else:
                    failed_imports += 1
                    logger.error(f"Failed to save call {call_id}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing call {call_id}: {e}")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Error details: {str(e)}")
                failed_imports += 1
        
        logger.info(f"Import completed. Successful: {successful_imports}, Failed: {failed_imports}")
        return successful_imports, failed_imports
    
    def import_from_csv(self, csv_file_path):
        """Import call IDs from a CSV file"""
        call_ids = []
        try:
            with open(csv_file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'call_id' in row and row['call_id']:
                        call_ids.append(row['call_id'])
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return 0, 0
        
        return self.import_call_data(call_ids)

    def create_clari_calls_table(self):
        """Create the clari_calls table with all available fields"""
        try:
            # Create comprehensive table for all Clari data
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS clari_calls (
                id SERIAL PRIMARY KEY,
                call_id VARCHAR(255) UNIQUE NOT NULL,
                source_id VARCHAR(255),
                title TEXT,
                status VARCHAR(100),
                type VARCHAR(100),
                disposition VARCHAR(100),
                time TIMESTAMP,
                last_modified_time TIMESTAMP,
                icaluid VARCHAR(255),
                calendar_id VARCHAR(255),
                audio_url TEXT,
                video_url TEXT,
                call_review_page_url TEXT,
                
                -- Deal/CRM Information
                deal_name TEXT,
                deal_value DECIMAL(15,2),
                deal_close_date DATE,
                deal_stage_before_call VARCHAR(255),
                deal_stage_live VARCHAR(255),
                account_name TEXT,
                contact_names JSONB,
                
                -- CRM Integration
                crm_source VARCHAR(100),
                deal_id VARCHAR(255),
                account_id VARCHAR(255),
                contact_ids JSONB,
                
                -- Participants
                users JSONB,
                external_participants JSONB,
                joined_participants JSONB,
                
                -- Call Metrics
                call_duration INTEGER,
                total_speak_duration DECIMAL(10,2),
                longest_monologue_duration DECIMAL(10,2),
                longest_monologue_start_time DECIMAL(10,2),
                talk_listen_ratio DECIMAL(5,4),
                num_questions_asked INTEGER,
                num_questions_asked_by_reps INTEGER,
                engaging_questions INTEGER,
                
                -- AI Categories and Trackers
                categories JSONB,
                
                -- Summary Data
                full_summary TEXT,
                key_takeaways TEXT,
                topics_discussed JSONB,
                key_action_items JSONB,
                
                -- Transcript
                transcript JSONB,
                
                -- Raw Data
                raw_data JSONB,
                
                -- Metadata
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """
            
            # Note: Table creation should be done via Supabase dashboard or migrations
            # For now, we'll assume the table exists and handle the error gracefully
            logger.info("clari_calls table creation skipped - use Supabase dashboard to create table")
            logger.info("clari_calls table created successfully")
            
        except Exception as e:
            logger.error(f"Error creating clari_calls table: {e}")
            raise

    def transform_clari_data_comprehensive(self, call_id, call_data):
        """Transform Clari data to comprehensive format for clari_calls table"""
        try:
            # Extract call metadata
            call_info = call_data.get('call', {})
            
            # Helper function to parse numeric values
            def parse_numeric(value):
                if value is None:
                    return None
                try:
                    return float(value) if '.' in str(value) else int(value)
                except:
                    return None
            
            # Helper function to parse date
            def parse_date(date_str):
                if not date_str:
                    return None
                try:
                    return date_str  # Keep as string for now, let Supabase handle conversion
                except:
                    return None
            
            # Basic call information
            transformed_data = {
                'call_id': call_id,
                'source_id': call_info.get('source_id'),
                'title': call_info.get('title'),
                'status': call_info.get('status'),
                'type': call_info.get('type'),
                'disposition': call_info.get('disposition'),
                'time': parse_date(call_info.get('time')),
                'last_modified_time': parse_date(call_info.get('last_modified_time')),
                'icaluid': call_info.get('icaluid'),
                'calendar_id': call_info.get('calendar_id'),
                'audio_url': call_info.get('audio_url'),
                'video_url': call_info.get('video_url'),
                'call_review_page_url': call_info.get('call_review_page_url'),
                
                # Deal/CRM Information
                'deal_name': call_info.get('deal_name'),
                'deal_value': parse_numeric(call_info.get('deal_value')),
                'deal_close_date': parse_date(call_info.get('deal_close_date')),
                'deal_stage_before_call': call_info.get('deal_stage_before_call'),
                'deal_stage_live': call_info.get('deal_stage_live'),
                'account_name': call_info.get('account_name'),
                'contact_names': call_info.get('contact_names'),
                
                # CRM Integration
                'crm_source': call_info.get('crm_info', {}).get('source_crm'),
                'deal_id': call_info.get('crm_info', {}).get('deal_id'),
                'account_id': call_info.get('crm_info', {}).get('account_id'),
                'contact_ids': call_info.get('crm_info', {}).get('contact_ids'),
                
                # Participants
                'users': call_info.get('users'),
                'external_participants': call_info.get('externalParticipants'),
                'joined_participants': call_info.get('joinedParticipants'),
                
                # Call Metrics
                'call_duration': parse_numeric(call_info.get('metrics', {}).get('call_duration')),
                'total_speak_duration': parse_numeric(call_info.get('metrics', {}).get('total_speak_duration')),
                'longest_monologue_duration': parse_numeric(call_info.get('metrics', {}).get('longest_monologue_duration')),
                'longest_monologue_start_time': parse_numeric(call_info.get('metrics', {}).get('longest_monologue_start_time')),
                'talk_listen_ratio': parse_numeric(call_info.get('metrics', {}).get('talk_listen_ratio')),
                'num_questions_asked': parse_numeric(call_info.get('metrics', {}).get('num_questions_asked')),
                'num_questions_asked_by_reps': parse_numeric(call_info.get('metrics', {}).get('num_questions_asked_by_reps')),
                'engaging_questions': parse_numeric(call_info.get('metrics', {}).get('engaging_questions')),
                
                # AI Categories and Trackers
                'categories': call_info.get('metrics', {}).get('categories'),
                
                # Summary Data
                'full_summary': call_info.get('summary', {}).get('full_summary'),
                'key_takeaways': call_info.get('summary', {}).get('key_takeaways'),
                'topics_discussed': call_info.get('summary', {}).get('topics_discussed'),
                'key_action_items': call_info.get('summary', {}).get('key_action_items'),
                
                # Transcript
                'transcript': call_info.get('transcript'),
                
                # Raw Data (for backup/debugging)
                'raw_data': call_data
            }
            
            # Remove None values and complex JSON fields to avoid issues with Supabase
            # For now, let's focus on the basic fields first
            basic_fields = {
                'call_id', 'source_id', 'title', 'status', 'type', 'disposition',
                'time', 'last_modified_time', 'icaluid', 'calendar_id', 'audio_url', 'video_url',
                'call_review_page_url', 'deal_name', 'deal_value', 'deal_close_date',
                'deal_stage_before_call', 'deal_stage_live', 'account_name',
                'crm_source', 'deal_id', 'account_id',
                'call_duration', 'total_speak_duration', 'longest_monologue_duration',
                'longest_monologue_start_time', 'talk_listen_ratio', 'num_questions_asked',
                'num_questions_asked_by_reps', 'engaging_questions',
                'full_summary', 'key_takeaways'
            }
            
            # Only include basic fields for now
            transformed_data = {k: v for k, v in transformed_data.items() 
                             if k in basic_fields and v is not None}
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming comprehensive Clari data: {e}")
            raise

    def import_call_to_clari_calls(self, call_id):
        """Import a single call to the clari_calls table"""
        try:
            # Fetch call details
            call_data = self.fetch_call_details(call_id)
            if not call_data:
                logger.warning(f"No data returned for call {call_id}")
                return False
            
            # Transform data
            transformed_data = self.transform_clari_data_comprehensive(call_id, call_data)
            
            # Debug: Log what we're trying to insert
            logger.info(f"Transformed data for {call_id}:")
            logger.info(f"  - title: {transformed_data.get('title')}")
            logger.info(f"  - deal_name: {transformed_data.get('deal_name')}")
            logger.info(f"  - full_summary: {transformed_data.get('full_summary', '')[:100]}...")
            logger.info(f"  - call_duration: {transformed_data.get('call_duration')}")
            logger.info(f"  - total fields: {len(transformed_data)}")
            
            # Insert into clari_calls table
            result = self.supabase.table('clari_calls').upsert(transformed_data).execute()
            
            logger.info(f"Successfully imported call {call_id} to clari_calls table")
            return True
            
        except Exception as e:
            logger.error(f"Error importing call {call_id} to clari_calls: {e}")
            return False

    def import_calls_to_clari_calls(self, call_ids):
        """Import multiple calls to the clari_calls table"""
        try:
            # Ensure table exists
            self.create_clari_calls_table()
            
            successful_imports = 0
            failed_imports = 0
            
            for call_id in call_ids:
                if self.import_call_to_clari_calls(call_id):
                    successful_imports += 1
                else:
                    failed_imports += 1
            
            logger.info(f"Import complete: {successful_imports} successful, {failed_imports} failed")
            return successful_imports, failed_imports
            
        except Exception as e:
            logger.error(f"Error in bulk import to clari_calls: {e}")
            raise

def main():
    """Main function to run the importer"""
    importer = ClariDataImporter()
    
    # Example call IDs (replace with your actual call IDs)
    call_ids = [
        "9b0f3ee3-8b91-4ab6-8177-7f07af53ddbf",
        "a5b23f40-d2b5-4ea3-9e17-76ce0b08dc8f",
        # Add more call IDs here
    ]
    
    # Import the data
    successful, failed = importer.import_call_data(call_ids)
    
    print(f"Import completed!")
    print(f"Successful imports: {successful}")
    print(f"Failed imports: {failed}")

if __name__ == "__main__":
    main() 