#!/usr/bin/env python3
"""
Simple security utilities - just the essentials
"""

import re
import logging

logger = logging.getLogger(__name__)

def clean_input(text):
    """Basic input cleaning - removes dangerous characters"""
    if not text:
        return ""
    
    # Remove script tags and quotes
    cleaned = re.sub(r'[<>"\']', '', str(text))
    return cleaned.strip()[:500]  # Limit length

def log_safely(message, data=None):
    """Log without exposing sensitive info"""
    if data:
        # Remove sensitive fields from logs
        safe_data = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in ['key', 'password', 'token']):
                safe_data[key] = '[HIDDEN]'
            else:
                safe_data[key] = value
        logger.info(f"{message}: {safe_data}")
    else:
        logger.info(message)

def validate_call_id(call_id):
    """Basic validation for call IDs"""
    if not call_id or len(call_id) < 10:
        return False
    return True 