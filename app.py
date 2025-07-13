#!/usr/bin/env python3
"""
Flask app for Render deployment
Provides web interface and handles automated sync
"""

from flask import Flask, jsonify, request
from flask_talisman import Talisman
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import os
import logging
from datetime import datetime
from automated_clari_sync import AutomatedClariSync

# Setup Flask app
app = Flask(__name__)

# Security headers
Talisman(app, 
    content_security_policy={
        'default-src': "'self'",
        'script-src': "'self'",
        'style-src': "'self'"
    }
)

# Rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize sync service (lazy loading)
sync_service = None

def get_sync_service():
    """Get or create sync service instance"""
    global sync_service
    if sync_service is None:
        try:
            # Debug environment variables
            supabase_url = os.environ.get('SUPABASE_URL')
            supabase_key = os.environ.get('SUPABASE_KEY')
            clari_key = os.environ.get('CLARI_API_KEY')
            
            logger.info(f"SUPABASE_URL: {'Set' if supabase_url else 'Not set'}")
            logger.info(f"SUPABASE_KEY: {'Set' if supabase_key else 'Not set'}")
            logger.info(f"CLARI_API_KEY: {'Set' if clari_key else 'Not set'}")
            
            # Debug the actual values (masked for security)
            if supabase_url:
                logger.info(f"SUPABASE_URL value: {supabase_url[:20]}...")
            if supabase_key:
                logger.info(f"SUPABASE_KEY value: {supabase_key[:20]}...")
            
            sync_service = AutomatedClariSync()
        except Exception as e:
            logger.error(f"Failed to initialize sync service: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            raise
    return sync_service

@app.route('/')
def home():
    """Home page with sync status"""
    return """
    <h1>Clari to Supabase Sync Service</h1>
    <p>This service automatically syncs Clari call data to your Supabase database.</p>
    <p><a href="/sync">Run Manual Sync</a></p>
    <p><a href="/status">Check Status</a></p>
    """

@app.route('/sync')
@limiter.limit("10 per hour")  # Rate limit manual syncs
def manual_sync():
    """Trigger manual sync"""
    try:
        logger.info("Manual sync triggered")
        service = get_sync_service()
        service.run_daily_sync()
        return jsonify({
            "status": "success",
            "message": "Sync completed successfully",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Manual sync failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/sync-sample')
@limiter.limit("20 per hour")  # Increased rate limit for testing
def sample_sync():
    """Trigger sample sync for last 7 days"""
    try:
        logger.info("Sample sync triggered (last 7 days)")
        service = get_sync_service()
        result = service.run_sample_sync(days=7)
        return jsonify({
            "status": "success",
            "message": f"Sample sync completed. Found {result.get('total_calls', 0)} calls, imported {result.get('imported_calls', 0)} new calls",
            "details": result,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Sample sync failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/sync-debug')
@limiter.limit("10 per hour")  # Rate limit debug syncs
def debug_sync():
    """Debug sync with just 1-2 calls to see import errors"""
    try:
        logger.info("Debug sync triggered (1-2 calls)")
        service = get_sync_service()
        
        # Get just 1-2 call IDs for testing
        recent_call_ids = service.fetch_recent_call_ids_from_clari(days_back=7)
        test_call_ids = recent_call_ids[:2] if recent_call_ids else []
        
        if not test_call_ids:
            return jsonify({
                "status": "error",
                "message": "No calls found to test",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        logger.info(f"Debug sync: Testing with {len(test_call_ids)} calls: {test_call_ids}")
        
        # Try to import just these calls
        successful, failed = service.importer.import_call_data(test_call_ids)
        
        return jsonify({
            "status": "success",
            "message": f"Debug sync completed. Tested {len(test_call_ids)} calls, imported {successful}, failed {failed}",
            "test_call_ids": test_call_ids,
            "successful": successful,
            "failed": failed,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Debug sync failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/debug-raw-data')
@limiter.limit("5 per hour")  # Rate limit raw data debug
def debug_raw_data():
    """Debug endpoint to show raw Clari data structure"""
    try:
        logger.info("Raw data debug triggered")
        service = get_sync_service()
        
        # Get just 1 call ID for testing
        recent_call_ids = service.fetch_recent_call_ids_from_clari(days_back=7)
        test_call_id = recent_call_ids[0] if recent_call_ids else None
        
        if not test_call_id:
            return jsonify({
                "status": "error",
                "message": "No calls found to test",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        # Fetch raw call data
        call_data = service.importer.fetch_call_details(test_call_id)
        
        if not call_data:
            return jsonify({
                "status": "error",
                "message": f"No data returned for call {test_call_id}",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        # Transform the data to see what we're working with
        transformed_data = service.importer.transform_clari_data(test_call_id, call_data)
        
        return jsonify({
            "status": "success",
            "call_id": test_call_id,
            "raw_data_keys": list(call_data.keys()) if call_data else [],
            "crm_info_keys": list(call_data.get('crm_info', {}).keys()) if call_data else [],
            "transformed_data": transformed_data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Raw data debug failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/debug-api-call')
@limiter.limit("5 per hour")  # Rate limit API call debug
def debug_api_call():
    """Debug endpoint to test the Clari API call directly"""
    try:
        logger.info("API call debug triggered")
        service = get_sync_service()
        
        # Test the API call directly
        recent_call_ids = service.fetch_recent_call_ids_from_clari(days_back=7)
        
        return jsonify({
            "status": "success",
            "call_ids_found": len(recent_call_ids),
            "call_ids": recent_call_ids[:5],  # Show first 5
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"API call debug failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/debug-specific-call')
@limiter.limit("5 per hour")  # Rate limit specific call debug
def debug_specific_call():
    """Debug endpoint to test specific call IDs from the working sample"""
    try:
        logger.info("Specific call debug triggered")
        service = get_sync_service()
        
        # Test with multiple call IDs from the working sample
        test_call_ids = [
            "9b0f3ee3-8b91-4ab6-8177-7f07af53ddbf",
            "a5b23f40-d2b5-4ea3-9e17-76ce0b08dc8f", 
            "542c15d1-2427-4017-ab72-5aa9d23617ce"
        ]
        
        results = []
        for call_id in test_call_ids:
            # Fetch raw call data
            call_data = service.importer.fetch_call_details(call_id)
            
            if not call_data:
                results.append({
                    "call_id": call_id,
                    "status": "error",
                    "message": f"No data returned for call {call_id}"
                })
                continue
            
            # Transform the data to see what we're working with
            transformed_data = service.importer.transform_clari_data(call_id, call_data)
            
            results.append({
                "call_id": call_id,
                "status": "success",
                "raw_data_keys": list(call_data.keys()) if call_data else [],
                "crm_info_keys": list(call_data.get('crm_info', {}).keys()) if call_data else [],
                "summary_keys": list(call_data.get('summary', {}).keys()) if call_data else [],
                "has_full_summary": bool(call_data.get('summary', {}).get('full_summary')),
                "has_key_takeaways": bool(call_data.get('summary', {}).get('key_takeaways')),
                "has_topics_discussed": bool(call_data.get('summary', {}).get('topics_discussed')),
                "has_key_action_items": bool(call_data.get('summary', {}).get('key_action_items')),
                "account_id": call_data.get('crm_info', {}).get('account_id'),
                "deal_id": call_data.get('crm_info', {}).get('deal_id'),
                "deal_stage_live": call_data.get('deal_stage_live')
            })
        
        return jsonify({
            "status": "success",
            "results": results,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Specific call debug failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/status')
def status():
    """Check service status"""
    try:
        # Test Supabase connection
        service = get_sync_service()
        existing_calls = service.get_existing_call_ids()
        return jsonify({
            "status": "healthy",
            "supabase_connected": True,
            "existing_calls_count": len(existing_calls),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    # Run the app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port) 