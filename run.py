#!/usr/bin/env python3
"""
Launch script for the web crawler application.
Run this from the project root directory.
"""

import sys
import os

# Add code directory to Python path
code_dir = os.path.join(os.path.dirname(__file__), 'code')
sys.path.insert(0, code_dir)

# Set up the NLWeb submodule path for imports
import setup_submodule_path  # This automatically sets up the submodule path

# Import and run the Flask app
from app import app, ensure_directories_exist, start_crawler

if __name__ == '__main__':
    # Ensure directories exist
    ensure_directories_exist()
    
    # Start the crawler thread
    start_crawler()
    
    # Suppress Flask startup output
    import sys as sys_module
    cli = sys_module.modules['flask.cli']
    cli.show_server_banner = lambda *x: None
    
    # Run the Flask app
    app.run(host='0.0.0.0', debug=False, threaded=True, use_reloader=False)