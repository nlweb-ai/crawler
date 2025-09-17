from flask import Flask, render_template, request, jsonify
import os
import json
import requests
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
from datetime import datetime
import threading
import asyncio
from crawler import Crawler
from concurrent.futures import ThreadPoolExecutor
import queue
import logging
import gzip
import re

app = Flask(__name__, template_folder='../templates')

# Disable Flask request logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Set up error logger for sitemap fetching
error_logger = logging.getLogger('sitemap_errors')
error_logger.setLevel(logging.ERROR)
# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)
error_handler = logging.FileHandler(os.path.join('logs', 'error.log'))
error_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

# Global crawler instance
crawler_instance = None
crawler_thread = None

# Background task executor for processing sites
site_processor = ThreadPoolExecutor(max_workers=2)
processing_status = {}  # Track processing status for each site

def get_site_name(url):
    """Extract site name from URL for file naming."""
    parsed = urlparse(url)
    return parsed.netloc.replace('.', '_')

def get_robots_txt(website_url):
    """Retrieve robots.txt and extract sitemap URLs."""
    robots_url = urljoin(website_url, '/robots.txt')
    sitemaps = []
    
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code == 200:
            for line in response.text.split('\n'):
                if line.strip().lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    sitemaps.append(sitemap_url)
        else:
            error_logger.error(f"SITEMAP | robots.txt HTTP {response.status_code} | {robots_url}")
    except Exception as e:
        # Suppressed: print(f"Error fetching robots.txt: {e}")
        error_logger.error(f"SITEMAP | robots.txt fetch failed | {website_url} | {str(e)}")
        pass
    
    return sitemaps

def parse_sitemap(sitemap_url, url_filter=None):
    """Parse sitemap and extract URLs, handling sitemap index files and gzipped sitemaps."""
    urls = []
    sub_sitemaps = []
    
    try:
        response = requests.get(sitemap_url, timeout=10)
        if response.status_code == 200:
            # Check if it's a gzipped file
            if sitemap_url.endswith('.gz'):
                # Decompress gzipped content
                content = gzip.decompress(response.content)
            else:
                content = response.content
            
            root = ET.fromstring(content)
            
            # Check if it's a sitemap index
            if root.tag.endswith('sitemapindex'):
                for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
                    loc = sitemap.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                    if loc is not None:
                        sub_sitemaps.append(loc.text)
            else:
                # Regular sitemap
                for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                    loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                    if loc is not None:
                        url_text = loc.text
                        if url_filter is None or url_filter in url_text:
                            urls.append(url_text)
        else:
            error_logger.error(f"SITEMAP | sitemap HTTP {response.status_code} | {sitemap_url}")
    except Exception as e:
        # Suppressed: print(f"Error parsing sitemap {sitemap_url}: {e}")
        error_logger.error(f"SITEMAP | sitemap parse failed | {sitemap_url} | {str(e)}")
        pass
    
    return urls, sub_sitemaps

def update_urls_file(site_name, urls):
    """Update the URLs file for a site."""
    urls_dir = os.path.join('data', 'urls')
    if not os.path.exists(urls_dir):
        os.makedirs(urls_dir)
    
    file_path = os.path.join(urls_dir, f"{site_name}.txt")
    
    # Read existing URLs
    existing_urls = set()
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_urls = set(line.strip() for line in f if line.strip())
    
    # Add new URLs
    existing_urls.update(urls)
    
    # Write back all URLs
    with open(file_path, 'w') as f:
        for url in sorted(existing_urls):
            f.write(url + '\n')
    
    return len(existing_urls)

def get_json_type_counts(site_name):
    """Get counts of JSON objects by @type for a site from status file."""
    # First try to get from status file (preferred)
    status = get_site_status(site_name)
    if 'json_stats' in status:
        return status['json_stats']
    
    # Fallback: calculate from JSON file if not in status
    json_file = os.path.join('data', 'json', f"{site_name}.json")
    type_counts = {}
    total_objects = 0
    
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r') as f:
                objects = json.load(f)
                for obj in objects:
                    # Handle new flattened format
                    if '@type' in obj:
                        # Direct object with @type at top level
                        total_objects += 1
                        type_name = obj['@type']
                        # Handle @type as list
                        if isinstance(type_name, list):
                            for t in type_name:
                                type_counts[t] = type_counts.get(t, 0) + 1
                        else:
                            type_counts[type_name] = type_counts.get(type_name, 0) + 1
                    elif 'items' in obj and isinstance(obj['items'], list):
                        # Array of items
                        for item in obj['items']:
                            if isinstance(item, dict) and '@type' in item:
                                total_objects += 1
                                type_name = item['@type']
                                # Handle @type as list
                                if isinstance(type_name, list):
                                    for t in type_name:
                                        type_counts[t] = type_counts.get(t, 0) + 1
                                else:
                                    type_counts[type_name] = type_counts.get(type_name, 0) + 1
                    elif 'data' in obj:
                        # Old format - backwards compatibility
                        data = obj['data']
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and '@type' in item:
                                    total_objects += 1
                                    type_name = item['@type']
                                    # Handle @type as list
                                    if isinstance(type_name, list):
                                        for t in type_name:
                                            type_counts[t] = type_counts.get(t, 0) + 1
                                    else:
                                        type_counts[type_name] = type_counts.get(type_name, 0) + 1
                        elif isinstance(data, dict):
                            if '@graph' in data:
                                for graph_item in data['@graph']:
                                    if isinstance(graph_item, dict) and '@type' in graph_item:
                                        total_objects += 1
                                        type_name = graph_item['@type']
                                        # Handle @type as list
                                        if isinstance(type_name, list):
                                            for t in type_name:
                                                type_counts[t] = type_counts.get(t, 0) + 1
                                        else:
                                            type_counts[type_name] = type_counts.get(type_name, 0) + 1
                            elif '@type' in data:
                                total_objects += 1
                                type_name = data['@type']
                                # Handle @type as list
                                if isinstance(type_name, list):
                                    for t in type_name:
                                        type_counts[t] = type_counts.get(t, 0) + 1
                                else:
                                    type_counts[type_name] = type_counts.get(type_name, 0) + 1
        except Exception as e:
            # Suppressed: print(f"Error processing JSON for {site_name}: {e}")
            pass
    
    return {
        'total_objects': total_objects,
        'type_counts': type_counts
    }

def get_site_status(site_name):
    """Get the status of a site crawl."""
    status_file = os.path.join('data', 'status', f"{site_name}.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                content = f.read()
                if not content.strip():
                    # Empty file - return default status
                    return {
                        'total_urls': 0,
                        'crawled_urls': 0,
                        'paused': False,
                        'sitemap_processed': True,  # Default to true for compatibility
                        'last_updated': datetime.now().isoformat()
                    }
                return json.loads(content)
        except json.JSONDecodeError as e:
            # Suppressed: print(f"Error reading status file {status_file}: {e}")
            # Return default status if file is corrupted
            pass
        return {
            'total_urls': 0,
            'crawled_urls': 0,
            'paused': False,
            'sitemap_processed': True,  # Default to true for compatibility
            'last_updated': datetime.now().isoformat()
        }
    
    # Default status
    return {
        'total_urls': 0,
        'crawled_urls': 0,
        'paused': False,
        'last_updated': datetime.now().isoformat()
    }

def update_site_status(site_name, status_data):
    """Update the status file for a site."""
    status_dir = os.path.join('data', 'status')
    if not os.path.exists(status_dir):
        os.makedirs(status_dir)
    
    status_file = os.path.join(status_dir, f"{site_name}.json")
    status_data['last_updated'] = datetime.now().isoformat()
    
    with open(status_file, 'w') as f:
        json.dump(status_data, f, indent=2)

def process_site_background(input_url, url_filter=None):
    """Process a site in the background thread."""
    site_name = get_site_name(input_url)
    
    try:
        # Update processing status
        processing_status[site_name] = {'status': 'processing', 'message': 'Fetching sitemaps...'}
        
        all_urls = []
        
        # Check if it's a sitemap URL
        if 'sitemap' in input_url.lower() or input_url.endswith('.xml'):
            # Process as sitemap
            sitemaps_to_process = [input_url]
        else:
            # Process as website - get sitemaps from robots.txt
            processing_status[site_name]['message'] = 'Checking robots.txt...'
            sitemaps_to_process = get_robots_txt(input_url)
            if not sitemaps_to_process:
                # Try default sitemap location
                default_sitemap = urljoin(input_url, '/sitemap.xml')
                sitemaps_to_process = [default_sitemap]
        
        # Process all sitemaps
        processed_sitemaps = set()
        while sitemaps_to_process:
            sitemap_url = sitemaps_to_process.pop(0)
            if sitemap_url in processed_sitemaps:
                continue
            
            processed_sitemaps.add(sitemap_url)
            processing_status[site_name]['message'] = f'Processing sitemap: {sitemap_url}'
            
            urls, sub_sitemaps = parse_sitemap(sitemap_url, url_filter)
            
            if urls:
                all_urls.extend(urls)
                # Update URLs file after each sitemap
                total_urls = update_urls_file(site_name, urls)
                
                # Update status
                status = get_site_status(site_name)
                status['total_urls'] = total_urls
                status['processing'] = True
                update_site_status(site_name, status)
                
                processing_status[site_name]['urls_found'] = len(all_urls)
            
            # Add sub-sitemaps to process
            sitemaps_to_process.extend(sub_sitemaps)
        
        # Final update - mark sitemap processing as complete
        status = get_site_status(site_name)
        status['processing'] = False
        status['sitemap_processed'] = True
        update_site_status(site_name, status)
        
        processing_status[site_name] = {
            'status': 'completed',
            'urls_found': len(all_urls),
            'total_urls': total_urls if 'total_urls' in locals() else len(all_urls)
        }
        
    except Exception as e:
        processing_status[site_name] = {
            'status': 'error',
            'message': str(e)
        }
        
        # Update status file to reflect error
        status = get_site_status(site_name)
        status['processing'] = False
        status['sitemap_processed'] = True  # Mark as processed even on error
        status['error'] = str(e)
        update_site_status(site_name, status)

@app.route('/')
def index():
    """Main page showing summary."""
    return render_template('summary.html')

@app.route('/add')
def add_site():
    """Page for adding new sites."""
    return render_template('add_site.html')

@app.route('/process', methods=['POST'])
def process():
    """Process website or sitemap URL."""
    data = request.json
    input_url = data.get('url', '').strip()
    url_filter = data.get('filter', '').strip() or None
    custom_site_name = data.get('site_name', '').strip()
    
    if not input_url:
        return jsonify({'error': 'URL is required'}), 400
    
    # Use custom site name if provided, otherwise generate from URL
    if custom_site_name:
        # Validate site name
        if not re.match(r'^[a-zA-Z0-9_]+$', custom_site_name):
            return jsonify({'error': 'Site name can only contain letters, numbers, and underscores'}), 400
        site_name = custom_site_name
    else:
        site_name = get_site_name(input_url)
    
    # Check if site already exists
    status_file = os.path.join('data', 'status', f"{site_name}.json")
    if os.path.exists(status_file):
        # Site already exists - just return success with existing info
        urls_file = os.path.join('data', 'urls', f"{site_name}.txt")
        existing_urls = 0
        if os.path.exists(urls_file):
            with open(urls_file, 'r') as f:
                existing_urls = sum(1 for line in f if line.strip())
        
        # Return success as if it was just added
        return jsonify({
            'site_name': site_name,
            'urls_found': 0,  # No new URLs found since it already exists
            'total_urls': existing_urls,
            'already_existed': True
        })
    
    # Create initial status file
    initial_status = {
        'total_urls': 0,
        'crawled_urls': 0,
        'paused': False,
        'processing': True,
        'sitemap_processed': False,
        'original_url': input_url  # Store original URL for restart
    }
    update_site_status(site_name, initial_status)
    
    # Submit to background processor
    site_processor.submit(process_site_background, input_url, url_filter)
    
    # Return immediately
    return jsonify({
        'site_name': site_name,
        'processing': True,
        'message': 'Site is being processed in the background'
    })

@app.route('/status/<site_name>')
def site_status(site_name):
    """Get status for a specific site."""
    status = get_site_status(site_name)
    
    # Get JSON type counts
    json_stats = get_json_type_counts(site_name)
    
    # Get last 5 JSON objects for this site
    json_objects = []
    json_file = os.path.join('data', 'json', f"{site_name}.json")
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r') as f:
                all_objects = json.load(f)
                # Get last 5 objects (most recent)
                last_objects = all_objects[-5:] if len(all_objects) > 5 else all_objects
                # Reverse to show newest first
                last_objects.reverse()
                
                # For display, we need to handle both old and new formats
                json_objects = []
                for obj in last_objects:
                    if 'data' in obj:
                        # Old format - keep as is
                        json_objects.append(obj)
                    else:
                        # New flattened format - create display object
                        display_obj = {
                            'url': obj.get('url', ''),
                            'timestamp': obj.get('timestamp', ''),
                            'data': {k: v for k, v in obj.items() if k not in ['url', 'timestamp']}
                        }
                        json_objects.append(display_obj)
        except Exception:
            pass  # Suppress error printing
    
    # Check if this is an API request or web page request
    if request.headers.get('Accept', '').startswith('application/json'):
        status['recent_json'] = json_objects
        status['json_stats'] = json_stats
        return jsonify(status)
    
    # For web requests, render the template
    return render_template('site_status.html', 
                         site_name=site_name, 
                         status=status, 
                         json_objects=json_objects,
                         json_stats=json_stats)

@app.route('/status')
def status_page():
    """Status page showing all sites."""
    return render_template('status.html')

@app.route('/summary')
def summary_page():
    """Summary page showing aggregate statistics."""
    return render_template('summary.html')

@app.route('/sites')
def list_sites():
    """List all sites being crawled."""
    sites = []
    status_dir = os.path.join('data', 'status')
    if os.path.exists(status_dir):
        for filename in os.listdir(status_dir):
            if filename.endswith('.json'):
                site_name = filename[:-5]
                status = get_site_status(site_name)
                # JSON stats are now in status file
                json_objects = status.get('json_stats', {}).get('total_objects', 0)
                sites.append({
                    'name': site_name,
                    'total_urls': status['total_urls'],
                    'crawled_urls': status['crawled_urls'],
                    'paused': status['paused'],
                    'errors': status.get('errors', {}),
                    'json_objects': json_objects
                })
    return jsonify(sites)

@app.route('/toggle_pause/<site_name>', methods=['POST'])
def toggle_pause(site_name):
    """Toggle pause/resume for a site crawl."""
    status = get_site_status(site_name)
    status['paused'] = not status['paused']
    update_site_status(site_name, status)
    return jsonify({'paused': status['paused']})

def delete_site_data(site_name):
    """Helper function to delete all data for a site."""
    import shutil
    
    # Notify crawler to stop processing this site
    global crawler_instance
    if crawler_instance:
        crawler_instance.delete_site(site_name)
    
    # Delete URLs file
    urls_file = os.path.join('data', 'urls', f"{site_name}.txt")
    if os.path.exists(urls_file):
        os.remove(urls_file)
    
    # Delete docs directory
    docs_dir = os.path.join('data', 'docs', site_name)
    if os.path.exists(docs_dir):
        shutil.rmtree(docs_dir)
    
    # Delete JSON file
    json_file = os.path.join('data', 'json', f"{site_name}.json")
    if os.path.exists(json_file):
        os.remove(json_file)
    
    # Delete embeddings file
    embeddings_file = os.path.join('data', 'embeddings', f"{site_name}.json")
    if os.path.exists(embeddings_file):
        os.remove(embeddings_file)
    
    # Delete keys file
    keys_file = os.path.join('data', 'keys', f"{site_name}.json")
    if os.path.exists(keys_file):
        os.remove(keys_file)
    
    # Delete status file
    status_file = os.path.join('data', 'status', f"{site_name}.json")
    if os.path.exists(status_file):
        os.remove(status_file)

@app.route('/delete_site/<site_name>', methods=['POST'])
def delete_site(site_name):
    """Delete a site and all its associated data."""
    try:
        delete_site_data(site_name)
        return jsonify({'success': True, 'message': f'Site {site_name} deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/restart_crawl/<site_name>', methods=['POST'])
def restart_crawl(site_name):
    """Restart crawl for a site from scratch by clearing all data and refetching sitemap."""
    try:
        # Get the original URL from status
        status = get_site_status(site_name)
        original_url = status.get('original_url')
        
        # If no original URL stored, try to reconstruct it
        if not original_url:
            original_url = f"https://{site_name.replace('_', '.')}"
        
        # Delete all existing data
        delete_site_data(site_name)
        
        # Create initial status file
        initial_status = {
            'total_urls': 0,
            'crawled_urls': 0,
            'paused': False,
            'processing': True,
            'sitemap_processed': False,
            'original_url': original_url  # Preserve original URL
        }
        update_site_status(site_name, initial_status)
        
        # Submit to background processor to refetch sitemap
        site_processor.submit(process_site_background, original_url)
        
        return jsonify({
            'success': True, 
            'message': f'Restarting crawl for {site_name}',
            'site_name': site_name
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/process_multiple', methods=['POST'])
def process_multiple():
    """Process multiple websites or sitemap URLs."""
    data = request.json
    urls = data.get('urls', [])
    
    if not urls:
        return jsonify({'error': 'No URLs provided'}), 400
    
    results = []
    
    for url in urls:
        url = url.strip()
        if not url:
            continue
            
        site_name = get_site_name(url)
        
        # Check if site already exists
        status_file = os.path.join('data', 'status', f"{site_name}.json")
        if os.path.exists(status_file):
            # Site already exists - just return existing info
            urls_file = os.path.join('data', 'urls', f"{site_name}.txt")
            existing_urls = 0
            if os.path.exists(urls_file):
                with open(urls_file, 'r') as f:
                    existing_urls = sum(1 for line in f if line.strip())
            
            results.append({
                'site_name': site_name,
                'already_existed': True,
                'total_urls': existing_urls
            })
            continue
        
        # Create initial status file
        initial_status = {
            'total_urls': 0,
            'crawled_urls': 0,
            'paused': False,
            'processing': True,
            'sitemap_processed': False,
            'original_url': url  # Store original URL for restart
        }
        update_site_status(site_name, initial_status)
        
        # Submit to background processor
        site_processor.submit(process_site_background, url)
        
        results.append({
            'site_name': site_name,
            'already_existed': False,
            'processing': True
        })
    
    return jsonify({
        'total_sites': len(results),
        'results': results,
        'message': 'Sites are being processed in the background'
    })

@app.route('/log')
def log_page():
    """Display the last 100 lines of the crawler log."""
    log_file = os.path.join('logs', 'crawler.log')
    log_lines = []
    
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                # Read all lines and get last 100
                lines = f.readlines()
                log_lines = lines[-100:]  # Last 100 lines
                # Reverse so newest is first
                log_lines.reverse()
        except Exception as e:
            log_lines = [f"Error reading log file: {e}"]
    else:
        log_lines = ["No log file found. The crawler may not have run yet."]
    
    return render_template('log.html', log_lines=log_lines)

@app.route('/error_log')
def error_log_page():
    """Display HTTP access errors with highlighted sitemap errors."""
    error_file = os.path.join('logs', 'error.log')
    error_entries = []
    
    if os.path.exists(error_file):
        try:
            with open(error_file, 'r') as f:
                lines = f.readlines()
                # Parse each line and categorize
                for line in lines[-200:]:  # Last 200 errors
                    line = line.strip()
                    if line:
                        parts = line.split(' | ', 3)
                        if len(parts) >= 3:
                            timestamp = parts[0]
                            level = parts[1]
                            message = ' | '.join(parts[2:])
                            
                            # Determine error type
                            error_type = 'general'
                            if 'SITEMAP' in message:
                                error_type = 'sitemap'
                            elif 'HTTP 429' in message:
                                error_type = 'rate_limit'
                            elif 'TIMEOUT' in message:
                                error_type = 'timeout'
                            elif 'HTTP 404' in message:
                                error_type = 'not_found'
                            
                            error_entries.append({
                                'timestamp': timestamp,
                                'level': level,
                                'message': message,
                                'type': error_type
                            })
                
                # Reverse to show newest first
                error_entries.reverse()
        except Exception as e:
            error_entries = [{
                'timestamp': 'N/A',
                'level': 'ERROR',
                'message': f"Error reading error log: {e}",
                'type': 'general'
            }]
    else:
        error_entries = [{
            'timestamp': 'N/A',
            'level': 'INFO',
            'message': 'No errors logged yet.',
            'type': 'general'
        }]
    
    return render_template('error_log.html', error_entries=error_entries)

@app.route('/crawler_status')
def crawler_status():
    """Get the status of the crawler thread."""
    global crawler_thread
    
    is_running = crawler_thread is not None and crawler_thread.is_alive()
    
    return jsonify({
        'running': is_running,
        'message': 'Crawler is running' if is_running else 'Crawler is not running'
    })

@app.route('/processing_status/<site_name>')
def get_processing_status(site_name):
    """Get the processing status for a site."""
    if site_name in processing_status:
        return jsonify(processing_status[site_name])
    
    # Check if site exists and is not processing
    status_file = os.path.join('data', 'status', f"{site_name}.json")
    if os.path.exists(status_file):
        status = get_site_status(site_name)
        if status.get('processing', False):
            return jsonify({'status': 'processing', 'message': 'Processing URLs...'})
        else:
            return jsonify({'status': 'completed'})
    
    return jsonify({'status': 'not_found'})

def run_crawler_thread():
    """Run the crawler in a separate thread."""
    global crawler_instance
    crawler_instance = Crawler()
    
    # Create new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(crawler_instance.run())
    except Exception as e:
        # Suppressed: print(f"Crawler error: {e}")
        pass
    finally:
        loop.close()

def start_crawler():
    """Start the crawler in a background thread."""
    global crawler_thread
    
    if crawler_thread is None or not crawler_thread.is_alive():
        # Suppressed: print("Starting crawler thread...")
        crawler_thread = threading.Thread(target=run_crawler_thread, daemon=True)
        crawler_thread.start()
        # Suppressed: print("Crawler thread started.")

def ensure_directories_exist():
    """Ensure all required directories exist."""
    # Create main data directory
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Create subdirectories inside data
    directories = ['urls', 'docs', 'json', 'status', 'keys', 'embeddings']
    for directory in directories:
        full_path = os.path.join('data', directory)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
    
    # Create logs directory
    if not os.path.exists('logs'):
        os.makedirs('logs')

if __name__ == '__main__':
    # Ensure directories exist
    ensure_directories_exist()
    
    # Start the crawler thread
    start_crawler()
    
    # Note: debug=True will cause the app to restart, which may create multiple crawler threads
    # In production, use debug=False or handle this appropriately
    # Suppress Flask startup output
    import sys
    import os as os_module
    cli = sys.modules['flask.cli']
    cli.show_server_banner = lambda *x: None
    
    app.run(debug=False, threaded=True, use_reloader=False)