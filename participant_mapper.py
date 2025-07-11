#!/usr/bin/env python3
"""
Participant mapping utility for Clari data import
Handles mapping between person IDs and participant names/emails
"""

import csv
import json
import os
from typing import Dict, Optional

class ParticipantMapper:
    def __init__(self, mapping_file_path: Optional[str] = None):
        """
        Initialize the participant mapper
        
        Args:
            mapping_file_path: Path to the CSV file with person ID mappings
        """
        self.mapping_file_path = mapping_file_path
        self.person_mapping = {}
        self.load_mapping()
    
    def load_mapping(self):
        """Load participant mapping from CSV file"""
        if not self.mapping_file_path or not os.path.exists(self.mapping_file_path):
            print(f"Warning: Mapping file {self.mapping_file_path} not found. Using default mapping.")
            return
        
        try:
            with open(self.mapping_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    call_id = row.get('call_id', '')
                    person_id = str(row.get('personId', ''))
                    name_or_email = row.get('name_or_email', '')
                    
                    if call_id and person_id and name_or_email:
                        key = (call_id, person_id)
                        self.person_mapping[key] = name_or_email
                        
            print(f"Loaded {len(self.person_mapping)} participant mappings from {self.mapping_file_path}")
            
        except Exception as e:
            print(f"Error loading mapping file: {e}")
    
    def get_participant_name(self, call_id: str, person_id: str) -> str:
        """
        Get participant name from mapping
        
        Args:
            call_id: The call ID
            person_id: The person ID from Clari
            
        Returns:
            Participant name or a default name
        """
        key = (call_id, person_id)
        if key in self.person_mapping:
            return self.person_mapping[key]
        
        # Fallback to person ID suffix
        return f"Participant {person_id[-4:] if len(person_id) >= 4 else person_id}"
    
    def get_participant_email(self, call_id: str, person_id: str) -> Optional[str]:
        """
        Extract email from participant name if it looks like an email
        
        Args:
            call_id: The call ID
            person_id: The person ID from Clari
            
        Returns:
            Email address if found, None otherwise
        """
        name = self.get_participant_name(call_id, person_id)
        
        # Check if the name looks like an email
        if '@' in name and '.' in name.split('@')[1]:
            return name
        
        return None
    
    def determine_participant_type(self, call_id: str, person_id: str, account_name: str = '') -> str:
        """
        Determine if participant is internal or external
        
        Args:
            call_id: The call ID
            person_id: The person ID from Clari
            account_name: The account name for context
            
        Returns:
            'internal' or 'external'
        """
        name = self.get_participant_name(call_id, person_id)
        
        # Simple logic - you might want to customize this
        internal_domains = [
            'yourcompany.com',  # Replace with your domain
            'internal',
            'employee'
        ]
        
        # Check if name contains internal indicators
        name_lower = name.lower()
        if any(domain in name_lower for domain in internal_domains):
            return 'internal'
        
        return 'external'
    
    def determine_participant_role(self, call_id: str, person_id: str, account_name: str = '') -> str:
        """
        Determine participant role in the call
        
        Args:
            call_id: The call ID
            person_id: The person ID from Clari
            account_name: The account name for context
            
        Returns:
            Role description
        """
        name = self.get_participant_name(call_id, person_id)
        name_lower = name.lower()
        
        # Simple role detection based on name patterns
        if any(title in name_lower for title in ['ceo', 'president', 'director', 'manager']):
            return 'decision_maker'
        elif any(title in name_lower for title in ['engineer', 'developer', 'technical']):
            return 'technical_contact'
        elif any(title in name_lower for title in ['user', 'end user']):
            return 'user'
        elif any(title in name_lower for title in ['influencer', 'stakeholder']):
            return 'influencer'
        
        return 'unknown'
    
    def create_mapping_template(self, output_file: str = 'participant_mapping_template.csv'):
        """
        Create a template CSV file for participant mapping
        
        Args:
            output_file: Path to save the template
        """
        template_data = [
            {
                'call_id': 'example-call-id-1',
                'personId': 'person-123',
                'name_or_email': 'john.doe@company.com',
                'role': 'decision_maker',
                'company': 'Example Corp'
            },
            {
                'call_id': 'example-call-id-1', 
                'personId': 'person-456',
                'name_or_email': 'jane.smith@company.com',
                'role': 'technical_contact',
                'company': 'Example Corp'
            }
        ]
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['call_id', 'personId', 'name_or_email', 'role', 'company'])
                writer.writeheader()
                for row in template_data:
                    writer.writerow(row)
            
            print(f"Created mapping template: {output_file}")
            print("Edit this file with your actual participant data and use it with the importer.")
            
        except Exception as e:
            print(f"Error creating template: {e}")

def main():
    """Example usage of the participant mapper"""
    
    # Create a mapper instance
    mapper = ParticipantMapper('personid_mapping3.csv')  # Use your existing mapping file
    
    # Example usage
    call_id = "9b0f3ee3-8b91-4ab6-8177-7f07af53ddbf"
    person_id = "person-123"
    
    name = mapper.get_participant_name(call_id, person_id)
    email = mapper.get_participant_email(call_id, person_id)
    participant_type = mapper.determine_participant_type(call_id, person_id)
    role = mapper.determine_participant_role(call_id, person_id)
    
    print(f"Participant: {name}")
    print(f"Email: {email}")
    print(f"Type: {participant_type}")
    print(f"Role: {role}")
    
    # Create a template if needed
    # mapper.create_mapping_template()

if __name__ == "__main__":
    main() 