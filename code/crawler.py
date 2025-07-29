import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import hashlib
from datetime import datetime
import threading
import time
import random
import logging
from collections import deque

class Crawler:
    def __init__(self):
        self.url_queue = asyncio.Queue()
        self.sites_urls = {}  # site_name: [urls]
        self.sites_status = {}  # site_name: status_dict
        self.last_crawled = {}  # domain: timestamp
        self.crawled_urls = {}  # site_name: set of crawled urls
        self.running = True
        self.session = None
        self.MAX_CONCURRENT = 10
        self.MIN_DELAY_SAME_SITE = 1.0  # seconds between requests to same site
        self.log_file = os.path.join('logs', 'crawler.log')
        self.error_log_file = os.path.join('logs', 'error.log')
        self.setup_logging()
        self.loop = None  # Will be set in run()
        self.pending_urls = []  # Store URLs until loop is ready
        self.domain_backoff = {}  # domain: backoff_until_timestamp
        self.site_errors = {}  # site_name: {error_code: count}
        self.deleted_sites = set()  # Track deleted sites
        self.site_queues = {}  # site_name: list of URLs
        self.last_site_index = 0  # For round-robin
        self.json_keys = {}  # site_name: set of JSON object URLs (keys)
        self.json_type_counts = {}  # site_name: {type: count}
        self.embeddings_queue = asyncio.Queue()  # Queue for embedding processing
        self.processed_embeddings = {}  # site_name: set of processed JSON keys
        # Chrome user agent
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def setup_logging(self):
        """Set up logging configuration."""
        # Ensure logs directory exists
        os.makedirs('logs', exist_ok=True)
        
        # Create a custom formatter
        formatter = logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        # File handler for general logs
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        
        # File handler for error logs
        error_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        error_handler = logging.FileHandler(self.error_log_file)
        error_handler.setFormatter(error_formatter)
        
        # Configure main logger
        self.logger = logging.getLogger('crawler')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        
        # Configure error logger
        self.error_logger = logging.getLogger('crawler_errors')
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.addHandler(error_handler)
        # Removed console handler to suppress terminal output
        
    def get_site_name(self, url):
        """Extract site name from URL."""
        parsed = urlparse(url)
        return parsed.netloc.replace('.', '_')
    
    def get_domain(self, url):
        """Extract domain from URL."""
        parsed = urlparse(url)
        return parsed.netloc
    
    def url_to_filename(self, url):
        """Convert URL to safe filename."""
        return hashlib.md5(url.encode()).hexdigest() + '.html'
    
    def load_crawled_urls(self, site_name):
        """Load set of already crawled URLs for a site."""
        if site_name not in self.crawled_urls:
            self.crawled_urls[site_name] = set()
            docs_dir = os.path.join('data', 'docs', site_name)
            if os.path.exists(docs_dir):
                for filename in os.listdir(docs_dir):
                    if filename.endswith('.html'):
                        self.crawled_urls[site_name].add(filename)
    
    def load_json_keys(self, site_name):
        """Load set of JSON object keys (URLs) for a site."""
        if site_name not in self.json_keys:
            self.json_keys[site_name] = set()
            keys_file = os.path.join('data', 'keys', f"{site_name}.txt")
            if os.path.exists(keys_file):
                with open(keys_file, 'r') as f:
                    for line in f:
                        key = line.strip()
                        if key:
                            self.json_keys[site_name].add(key)
    
    def save_json_key(self, site_name, key):
        """Save a JSON object key to the keys file."""
        if site_name not in self.json_keys:
            self.json_keys[site_name] = set()
        
        if key not in self.json_keys[site_name]:
            self.json_keys[site_name].add(key)
            keys_dir = os.path.join('data', 'keys')
            os.makedirs(keys_dir, exist_ok=True)
            keys_file = os.path.join(keys_dir, f"{site_name}.txt")
            with open(keys_file, 'a') as f:
                f.write(key + '\n')
    
    def update_json_type_count(self, site_name, type_name):
        """Update count for a JSON type."""
        if site_name not in self.json_type_counts:
            self.json_type_counts[site_name] = {}
        
        # Handle case where @type is a list
        if isinstance(type_name, list):
            # Count each type in the list
            for t in type_name:
                if t not in self.json_type_counts[site_name]:
                    self.json_type_counts[site_name][t] = 0
                self.json_type_counts[site_name][t] += 1
        else:
            # Single type
            if type_name not in self.json_type_counts[site_name]:
                self.json_type_counts[site_name][type_name] = 0
            self.json_type_counts[site_name][type_name] += 1
    
    def is_crawled(self, site_name, url):
        """Check if URL has already been crawled."""
        self.load_crawled_urls(site_name)
        filename = self.url_to_filename(url)
        return filename in self.crawled_urls[site_name]
    
    def url_monitor_thread(self):
        """Thread that monitors the urls directory for changes."""
        # Suppressed: print("URL monitor thread started")
        last_check = {}
        
        while self.running:
            try:
                # Clean up deleted sites from last_check
                for site in list(last_check.keys()):
                    if site in self.deleted_sites:
                        del last_check[site]
                
                urls_dir = os.path.join('data', 'urls')
                if os.path.exists(urls_dir):
                    for filename in os.listdir(urls_dir):
                        if filename.endswith('.txt'):
                            filepath = os.path.join('data', 'urls', filename)
                            site_name = filename[:-4]
                            
                            # Skip deleted sites
                            if site_name in self.deleted_sites:
                                continue
                            
                            # Check if file is new or modified
                            mtime = os.path.getmtime(filepath)
                            if site_name not in last_check or mtime > last_check[site_name]:
                                last_check[site_name] = mtime
                                
                                # Read URLs from file
                                with open(filepath, 'r') as f:
                                    urls = [line.strip() for line in f if line.strip()]
                                
                                self.sites_urls[site_name] = urls
                                # Suppressed: print(f"Loaded {len(urls)} URLs for site {site_name}")
                                
                                # Skip if site has been deleted
                                if site_name not in self.deleted_sites:
                                    # Check if sitemap processing is complete
                                    status = self.get_site_status(site_name)
                                    if not status.get('sitemap_processed', False):
                                        # Sitemap still being processed, skip for now
                                        continue
                                    
                                    # Load existing JSON keys for this site
                                    self.load_json_keys(site_name)
                                    
                                    # Load existing JSON type counts from status
                                    if 'json_stats' in status and 'type_counts' in status['json_stats']:
                                        self.json_type_counts[site_name] = status['json_stats']['type_counts'].copy()
                                    
                                    # Initialize site queue if needed
                                    if site_name not in self.site_queues:
                                        self.site_queues[site_name] = []
                                    
                                    # Add new URLs to site-specific queue
                                    new_urls = []
                                    for url in urls:
                                        if not self.is_crawled(site_name, url):
                                            new_urls.append(url)
                                    
                                    if new_urls:
                                        self.site_queues[site_name].extend(new_urls)
                                        # Suppressed: print(f"Added {len(new_urls)} new URLs to {site_name} queue")
                
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                # Suppressed: print(f"Error in URL monitor thread: {e}")
                time.sleep(5)
    
    def load_processed_embeddings(self, site_name):
        """Load set of already processed embeddings for a site."""
        if site_name not in self.processed_embeddings:
            self.processed_embeddings[site_name] = set()
            embeddings_file = os.path.join('data', 'embeddings', f"{site_name}.json")
            if os.path.exists(embeddings_file):
                try:
                    with open(embeddings_file, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            # Extract keys from embeddings data
                            self.processed_embeddings[site_name] = {item['key'] for item in data if 'key' in item}
                except Exception:
                    pass
    
    def embeddings_monitor_thread(self):
        """Thread that monitors JSON files and queues items for embedding processing."""
        last_check = {}
        
        while self.running:
            try:
                json_dir = os.path.join('data', 'json')
                if os.path.exists(json_dir):
                    for filename in os.listdir(json_dir):
                        if filename.endswith('.json'):
                            filepath = os.path.join(json_dir, filename)
                            site_name = filename[:-5]
                            
                            # Skip deleted sites
                            if site_name in self.deleted_sites:
                                continue
                            
                            # Check if file is new or modified
                            mtime = os.path.getmtime(filepath)
                            if site_name not in last_check or mtime > last_check[site_name]:
                                last_check[site_name] = mtime
                                
                                # Load processed embeddings for this site
                                self.load_processed_embeddings(site_name)
                                
                                # Read JSON file and find items needing embeddings
                                try:
                                    with open(filepath, 'r') as f:
                                        json_objects = json.load(f)
                                    
                                    # Find objects that haven't been processed
                                    unprocessed = []
                                    for obj in json_objects:
                                        if 'url' in obj and obj['url'] not in self.processed_embeddings[site_name]:
                                            unprocessed.append(obj)
                                    
                                    if unprocessed:
                                        # Queue them for processing in batches
                                        for i in range(0, len(unprocessed), 100):
                                            batch = unprocessed[i:i+100]
                                            # Add to queue (will be processed by async worker)
                                            if self.loop:
                                                asyncio.run_coroutine_threadsafe(
                                                    self.embeddings_queue.put((site_name, batch)),
                                                    self.loop
                                                )
                                except Exception as e:
                                    self.error_logger.error(f"Error processing JSON for embeddings | {site_name} | {str(e)}")
                
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                self.error_logger.error(f"Error in embeddings monitor thread | {str(e)}")
                time.sleep(30)
    
    def get_site_status(self, site_name):
        """Get current status for a site."""
        status_file = os.path.join('data', 'status', f"{site_name}.json")
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status = json.load(f)
                # Ensure sitemap_processed field exists
                if 'sitemap_processed' not in status:
                    status['sitemap_processed'] = True  # Default to true for compatibility
                return status
        return {'paused': False, 'total_urls': 0, 'crawled_urls': 0, 'sitemap_processed': True}
    
    async def get_next_url(self):
        """Get next URL using round-robin across sites."""
        # First, try to get from the main queue
        try:
            return await asyncio.wait_for(self.url_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        
        # If main queue is empty, use round-robin from site queues
        if not self.site_queues:
            return None
        
        # Get list of sites with URLs
        active_sites = [site for site, urls in self.site_queues.items() 
                        if urls and site not in self.deleted_sites]
        
        if not active_sites:
            return None
        
        # Round-robin through sites
        for _ in range(len(active_sites)):
            self.last_site_index = (self.last_site_index + 1) % len(active_sites)
            site_name = active_sites[self.last_site_index]
            
            if self.site_queues[site_name]:
                url = self.site_queues[site_name].pop(0)
                return (site_name, url)
        
        return None
    
    def delete_site(self, site_name):
        """Mark a site as deleted to stop crawling its URLs."""
        self.deleted_sites.add(site_name)
        # Remove from sites_urls if present
        if site_name in self.sites_urls:
            del self.sites_urls[site_name]
        # Remove from crawled_urls if present
        if site_name in self.crawled_urls:
            del self.crawled_urls[site_name]
        # Remove from site_errors if present
        if site_name in self.site_errors:
            del self.site_errors[site_name]
        # Remove from site_queues if present
        if site_name in self.site_queues:
            del self.site_queues[site_name]
        # Remove from json_keys if present
        if site_name in self.json_keys:
            del self.json_keys[site_name]
        # Remove from json_type_counts if present
        if site_name in self.json_type_counts:
            del self.json_type_counts[site_name]
        # Suppressed: print(f"Site {site_name} marked for deletion in crawler")
    
    def track_error(self, site_name, error_code):
        """Track error counts per site."""
        if site_name not in self.site_errors:
            self.site_errors[site_name] = {}
        
        error_str = str(error_code)
        if error_str not in self.site_errors[site_name]:
            self.site_errors[site_name][error_str] = 0
        
        self.site_errors[site_name][error_str] += 1
    
    def update_site_status(self, site_name, crawled_count=None):
        """Update site status."""
        status_file = os.path.join('data', 'status', f"{site_name}.json")
        status = self.get_site_status(site_name)
        
        if crawled_count is not None:
            status['crawled_urls'] = crawled_count
        
        # Add error counts
        if site_name in self.site_errors:
            status['errors'] = self.site_errors[site_name]
        
        # Add JSON type statistics
        if site_name in self.json_type_counts:
            type_counts = self.json_type_counts[site_name]
            total_objects = sum(type_counts.values())
            status['json_stats'] = {
                'total_objects': total_objects,
                'type_counts': type_counts
            }
        
        status['last_updated'] = datetime.now().isoformat()
        
        os.makedirs(os.path.join('data', 'status'), exist_ok=True)
        with open(status_file, 'w') as f:
            json.dump(status, f, indent=2)
    
    def extract_json_key(self, json_obj):
        """Extract the key (URL) from a JSON object."""
        if isinstance(json_obj, dict):
            # Check @id first as it's the standard JSON-LD identifier
            if '@id' in json_obj:
                return json_obj['@id']
            # Also check url attribute
            elif 'url' in json_obj:
                return json_obj['url']
        return None
    
    def extract_schema_org(self, html, url, site_name):
        """Extract schema.org JSON-LD from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        schema_data = []
        
        # Load existing keys for this site
        self.load_json_keys(site_name)
        
        # Find all JSON-LD script tags
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                
                # Check if this object (or objects in array/graph) already exists
                new_objects = []
                
                if isinstance(data, list):
                    # Array of objects
                    for item in data:
                        key = self.extract_json_key(item)
                        if key and key not in self.json_keys.get(site_name, set()):
                            new_objects.append(item)
                            self.save_json_key(site_name, key)
                        elif not key:
                            # No key, include it
                            new_objects.append(item)
                    
                    if new_objects:
                        # If multiple objects, we need to handle differently
                        if len(new_objects) > 1:
                            # For arrays, keep the original structure but flatten
                            flattened = {
                                'url': url,
                                'timestamp': datetime.now().isoformat(),
                                'items': new_objects
                            }
                            schema_data.append(flattened)
                            # Track type counts for each item
                            for obj in new_objects:
                                if '@type' in obj:
                                    self.update_json_type_count(site_name, obj['@type'])
                        else:
                            # Single object - flatten it
                            flattened = {
                                'url': url,
                                'timestamp': datetime.now().isoformat()
                            }
                            flattened.update(new_objects[0])
                            schema_data.append(flattened)
                            # Track type count
                            if '@type' in new_objects[0]:
                                self.update_json_type_count(site_name, new_objects[0]['@type'])
                        
                elif isinstance(data, dict):
                    if '@graph' in data:
                        # Handle @graph objects - save each item individually
                        for item in data['@graph']:
                            key = self.extract_json_key(item)
                            if key and key not in self.json_keys.get(site_name, set()):
                                self.save_json_key(site_name, key)
                                # Flatten the structure
                                flattened = {
                                    'url': url,
                                    'timestamp': datetime.now().isoformat()
                                }
                                flattened.update(item)
                                schema_data.append(flattened)
                                # Track type count
                                if '@type' in item:
                                    self.update_json_type_count(site_name, item['@type'])
                            elif not key:
                                # No key, include it
                                # Flatten the structure
                                flattened = {
                                    'url': url,
                                    'timestamp': datetime.now().isoformat()
                                }
                                flattened.update(item)
                                schema_data.append(flattened)
                                # Track type count
                                if '@type' in item:
                                    self.update_json_type_count(site_name, item['@type'])
                    else:
                        # Single object
                        key = self.extract_json_key(data)
                        if key and key not in self.json_keys.get(site_name, set()):
                            self.save_json_key(site_name, key)
                            # Flatten the structure
                            flattened = {
                                'url': url,
                                'timestamp': datetime.now().isoformat()
                            }
                            flattened.update(data)
                            schema_data.append(flattened)
                            # Track type count
                            if '@type' in data:
                                self.update_json_type_count(site_name, data['@type'])
                        elif not key:
                            # No key, include it
                            # Flatten the structure
                            flattened = {
                                'url': url,
                                'timestamp': datetime.now().isoformat()
                            }
                            flattened.update(data)
                            schema_data.append(flattened)
                            # Track type count
                            if '@type' in data:
                                self.update_json_type_count(site_name, data['@type'])
                            
            except json.JSONDecodeError:
                pass
        
        return schema_data
    
    def save_schema_org(self, site_name, schema_data):
        """Save schema.org data to JSON file."""
        if not schema_data:
            return
        
        json_dir = os.path.join('data', 'json')
        os.makedirs(json_dir, exist_ok=True)
        
        json_file = os.path.join(json_dir, f"{site_name}.json")
        
        # Load existing data
        existing_data = []
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        
        # Append new data
        existing_data.extend(schema_data)
        
        # Save back
        with open(json_file, 'w') as f:
            json.dump(existing_data, f, indent=2)
    
    def save_page(self, site_name, url, html):
        """Save crawled page to docs directory."""
        docs_dir = os.path.join('data', 'docs', site_name)
        os.makedirs(docs_dir, exist_ok=True)
        
        filename = self.url_to_filename(url)
        filepath = os.path.join(docs_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        # Update crawled URLs cache
        if site_name not in self.crawled_urls:
            self.crawled_urls[site_name] = set()
        self.crawled_urls[site_name].add(filename)
    
    async def can_crawl_domain(self, domain):
        """Check if enough time has passed since last crawl to this domain."""
        current_time = time.time()
        
        # Check if domain is in backoff period
        if domain in self.domain_backoff:
            if current_time < self.domain_backoff[domain]:
                # Still in backoff period
                return False
            else:
                # Backoff period expired
                del self.domain_backoff[domain]
        
        # Check regular rate limit
        if domain in self.last_crawled:
            elapsed = current_time - self.last_crawled[domain]
            if elapsed < self.MIN_DELAY_SAME_SITE:
                # Wait for the remaining time to ensure minimum delay
                wait_time = self.MIN_DELAY_SAME_SITE - elapsed
                await asyncio.sleep(wait_time)
        else:
            # First request to this domain, no need to wait
            pass
        
        # Update last crawled time AFTER the request completes
        # This will be done in fetch_url after successful request
        return True
    
    async def fetch_url(self, session, site_name, url):
        """Fetch a single URL."""
        try:
            # Check if site has been deleted
            if site_name in self.deleted_sites:
                # Don't process URLs from deleted sites
                return
            
            # Check if site is paused
            status = self.get_site_status(site_name)
            if status.get('paused', False):
                # Put URL back in site queue
                if site_name not in self.site_queues:
                    self.site_queues[site_name] = []
                self.site_queues[site_name].append(url)
                return
            
            # Check if already crawled
            if self.is_crawled(site_name, url):
                return
            
            # Rate limit per domain
            domain = self.get_domain(url)
            if not await self.can_crawl_domain(domain):
                # Domain is in backoff, put URL back in queue
                await self.url_queue.put((site_name, url))
                return
            
            # Fetch the page
            async with session.get(url, timeout=30, headers=self.headers) as response:
                content_length = response.headers.get('Content-Length', 'N/A')
                
                # Update last crawled time for this domain after request completes
                self.last_crawled[domain] = time.time()
                
                if response.status == 200:
                    html = await response.text()
                    
                    # Log successful fetch
                    self.logger.info(f"{url} | {response.status} | {len(html) if content_length == 'N/A' else content_length}")
                    
                    # Extract schema.org
                    schema_data = self.extract_schema_org(html, url, site_name)
                    if schema_data:
                        self.save_schema_org(site_name, schema_data)
                    
                    # Save page
                    self.save_page(site_name, url, html)
                    
                    # Update status
                    crawled_count = len(self.crawled_urls.get(site_name, set()))
                    self.update_site_status(site_name, crawled_count)
                    
                    # Suppressed: print(f"Crawled: {url}")
                else:
                    # Log failed fetch
                    self.logger.info(f"{url} | {response.status} | {content_length}")
                    # Log to error log
                    self.error_logger.error(f"HTTP {response.status} | {site_name} | {url}")
                    
                    # Track error
                    self.track_error(site_name, response.status)
                    self.update_site_status(site_name)
                    
                    # Handle 429 Too Many Requests
                    if response.status == 429:
                        # Apply backoff for this domain
                        backoff_time = random.uniform(3, 7)  # Random 3-7 seconds
                        self.domain_backoff[domain] = time.time() + backoff_time
                        # Suppressed: print(f"Rate limited on {domain}, backing off for {backoff_time:.1f} seconds")
                        
                        # Put URL back in site queue to retry later
                        if site_name not in self.site_queues:
                            self.site_queues[site_name] = []
                        self.site_queues[site_name].append(url)
                    
        except asyncio.TimeoutError:
            self.logger.info(f"{url} | TIMEOUT | 0")
            self.error_logger.error(f"TIMEOUT | {site_name} | {url}")
            self.track_error(site_name, 'TIMEOUT')
            self.update_site_status(site_name)
        except Exception as e:
            self.logger.info(f"{url} | ERROR | 0")
            self.error_logger.error(f"ERROR | {site_name} | {url} | {str(e)}")
            self.track_error(site_name, 'ERROR')
            self.update_site_status(site_name)
    
    async def worker(self, session, worker_id):
        """Worker that processes URLs from the queue."""
        while self.running:
            try:
                # Get next URL using round-robin
                result = await self.get_next_url()
                
                if result:
                    site_name, url = result
                    await self.fetch_url(session, site_name, url)
                else:
                    # No URLs available, wait a bit
                    await asyncio.sleep(1)
                
            except Exception as e:
                # Suppressed: print(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
    
    def prepare_text_for_embedding(self, json_obj):
        """Prepare text from JSON object for embedding."""
        # Extract key fields based on @type
        text_parts = []
        
        # Add type if present
        if '@type' in json_obj:
            obj_type = json_obj['@type']
            if isinstance(obj_type, list):
                text_parts.append(f"Type: {', '.join(obj_type)}")
            else:
                text_parts.append(f"Type: {obj_type}")
        
        # Add name/title
        if 'name' in json_obj:
            text_parts.append(f"Name: {json_obj['name']}")
        elif 'headline' in json_obj:
            text_parts.append(f"Headline: {json_obj['headline']}")
        
        # Add description
        if 'description' in json_obj:
            text_parts.append(f"Description: {json_obj['description']}")
        
        # For recipes, add ingredients
        if 'recipeIngredient' in json_obj and isinstance(json_obj['recipeIngredient'], list):
            ingredients = ', '.join(json_obj['recipeIngredient'][:10])  # First 10 ingredients
            text_parts.append(f"Ingredients: {ingredients}")
        
        # For articles, add article body (truncated)
        if 'articleBody' in json_obj:
            body = json_obj['articleBody'][:500]  # First 500 chars
            text_parts.append(f"Content: {body}")
        
        # Combine all parts
        return '\n'.join(text_parts)
    
    async def embeddings_worker(self):
        """Worker that processes embeddings queue."""
        while self.running:
            try:
                # Get batch from queue with timeout
                site_name, batch = await asyncio.wait_for(
                    self.embeddings_queue.get(), 
                    timeout=5.0
                )
                
                # Prepare texts for embedding
                texts = []
                keys = []
                for obj in batch:
                    text = self.prepare_text_for_embedding(obj)
                    if text and 'url' in obj:
                        texts.append(text)
                        keys.append(obj['url'])
                
                if texts:
                    try:
                        # Import get_embedding function (assuming it's available)
                        from embeddings_utils import get_embedding
                        
                        # Get embeddings for batch
                        embeddings = []
                        for text in texts:
                            embedding = await get_embedding(text)
                            embeddings.append(embedding)
                        
                        # Save embeddings to file
                        await self.save_embeddings(site_name, keys, embeddings, batch)
                        
                        # Update processed set
                        self.processed_embeddings[site_name].update(keys)
                        
                        self.logger.info(f"Processed {len(embeddings)} embeddings for {site_name}")
                        
                    except Exception as e:
                        self.error_logger.error(f"Error getting embeddings | {site_name} | {str(e)}")
                        
            except asyncio.TimeoutError:
                # No items in queue, continue
                pass
            except Exception as e:
                self.error_logger.error(f"Embeddings worker error | {str(e)}")
                await asyncio.sleep(1)
    
    async def save_embeddings(self, site_name, keys, embeddings, original_objects):
        """Save embeddings to file."""
        embeddings_dir = os.path.join('data', 'embeddings')
        if not os.path.exists(embeddings_dir):
            os.makedirs(embeddings_dir)
        
        embeddings_file = os.path.join(embeddings_dir, f"{site_name}.json")
        
        # Load existing embeddings
        existing_data = []
        if os.path.exists(embeddings_file):
            try:
                with open(embeddings_file, 'r') as f:
                    existing_data = json.load(f)
            except Exception:
                pass
        
        # Add new embeddings
        for i, (key, embedding) in enumerate(zip(keys, embeddings)):
            embedding_obj = {
                'key': key,
                'embedding': embedding,
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    '@type': original_objects[i].get('@type', 'Unknown'),
                    'name': original_objects[i].get('name', original_objects[i].get('headline', '')),
                    'url': original_objects[i].get('url', '')
                }
            }
            existing_data.append(embedding_obj)
        
        # Write back to file
        with open(embeddings_file, 'w') as f:
            json.dump(existing_data, f, indent=2)
    
    def requeue_urls(self):
        """Requeue URLs with domain diversity."""
        # Group URLs by domain
        domain_urls = {}
        temp_queue = []
        
        # Empty current queue
        while not self.url_queue.empty():
            try:
                item = self.url_queue.get_nowait()
                temp_queue.append(item)
            except asyncio.QueueEmpty:
                break
        
        # Group by domain
        for site_name, url in temp_queue:
            domain = self.get_domain(url)
            if domain not in domain_urls:
                domain_urls[domain] = []
            domain_urls[domain].append((site_name, url))
        
        # Interleave URLs from different domains
        while domain_urls:
            domains = list(domain_urls.keys())
            random.shuffle(domains)
            
            for domain in domains:
                if domain in domain_urls and domain_urls[domain]:
                    item = domain_urls[domain].pop(0)
                    if self.loop:
                        asyncio.run_coroutine_threadsafe(
                            self.url_queue.put(item),
                            self.loop
                        )
                    else:
                        self.pending_urls.append(item)
                    
                    if not domain_urls[domain]:
                        del domain_urls[domain]
    
    async def periodic_requeue(self):
        """Periodically requeue URLs to ensure domain diversity."""
        while self.running:
            await asyncio.sleep(30)  # Every 30 seconds
            self.requeue_urls()
    
    async def run(self):
        """Main crawler loop."""
        # Set the event loop
        self.loop = asyncio.get_event_loop()
        
        # Start URL monitor thread
        monitor_thread = threading.Thread(target=self.url_monitor_thread)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Start embeddings monitor thread
        embeddings_thread = threading.Thread(target=self.embeddings_monitor_thread)
        embeddings_thread.daemon = True
        embeddings_thread.start()
        
        # Process any pending URLs that were added before loop was ready
        # (No longer needed with site queues)
        
        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=self.MAX_CONCURRENT)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            
            # Start workers
            workers = []
            for i in range(self.MAX_CONCURRENT):
                worker = asyncio.create_task(self.worker(session, i))
                workers.append(worker)
            
            # Start embeddings worker
            embeddings_worker = asyncio.create_task(self.embeddings_worker())
            workers.append(embeddings_worker)
            
            # Wait for all workers
            await asyncio.gather(*workers, return_exceptions=True)
    
    def start(self):
        """Start the crawler."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_until_complete(self.run())
        except KeyboardInterrupt:
            # Suppressed: print("\nShutting down crawler...")
            self.running = False
        finally:
            self.loop.close()

if __name__ == '__main__':
    # This file is not meant to be run directly
    # The crawler is started from app.py
    print("Please run app.py instead of running crawler.py directly")