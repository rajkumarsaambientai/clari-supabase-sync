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

# Initialize sync service
sync_service = AutomatedClariSync()

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
        sync_service.run_daily_sync()
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

@app.route('/status')
def status():
    """Check service status"""
    try:
        # Test Supabase connection
        existing_calls = sync_service.get_existing_call_ids()
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