import requests
from urllib.parse import urljoin, urlparse
import os
import json
from datetime import datetime
import xml.etree.ElementTree as ET
import config  # Load environment variables
import db
from queue_interface_aad import get_queue_with_aad as get_queue

# Queue history log file
QUEUE_LOG_FILE = '/app/data/queue_history.jsonl'

def log_queue_operation(operation_type, job_data, success=True, error=None):
    """Log queue operations to a local JSONL file"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(QUEUE_LOG_FILE), exist_ok=True)

        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'operation': operation_type,
            'job': job_data,
            'success': success,
            'error': str(error) if error else None
        }

        with open(QUEUE_LOG_FILE, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"[MASTER] Error logging queue operation: {e}")

def parse_schema_map_xml(xml_content, base_url):
    """Parse schema_map.xml content and extract schema.org file URLs"""
    try:
        root = ET.fromstring(xml_content)

        # Handle namespace if present
        namespace = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        schema_urls = []

        # Try with namespace first
        urls = root.findall('sitemap:url', namespace)
        if not urls:
            # Try without namespace
            urls = root.findall('url')

        for url_elem in urls:
            # Check if this URL has structuredData/schema.org content type
            content_type = url_elem.get('contentType', '')
            if 'schema.org' in content_type.lower():
                # Get the location
                loc = url_elem.find('sitemap:loc', namespace)
                if loc is None:
                    loc = url_elem.find('loc')

                if loc is not None and loc.text:
                    # Make URL absolute if needed
                    url = urljoin(base_url, loc.text.strip())
                    schema_urls.append(url)

        return schema_urls
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return []

def get_schema_urls_from_robots(site_url):
    """
    Fetch robots.txt or schema_map.xml and extract schema file URLs.
    Returns triples: (site_url, schema_map_url, json_file_url)
    First tries robots.txt for schemaMap directives.
    If not found, tries schema_map.xml directly.
    """

    # First, try robots.txt
    robots_url = urljoin(site_url, '/robots.txt')
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code == 200:
            schema_map_urls = []
            for line in response.text.splitlines():
                if line.lower().startswith('schemamap:'):
                    url = line.split(':', 1)[1].strip()
                    schema_map_urls.append(urljoin(site_url, url))

            # If we found schemaMap directives, fetch and parse those XML files
            if schema_map_urls:
                all_schema_files = []
                for map_url in schema_map_urls:
                    try:
                        map_response = requests.get(map_url, timeout=10)
                        if map_response.status_code == 200:
                            json_urls = parse_schema_map_xml(map_response.text, site_url)
                            # Return triples of (site_url, schema_map_url, json_file_url)
                            all_schema_files.extend([(site_url, map_url, json_url) for json_url in json_urls])
                    except requests.RequestException as e:
                        print(f"Error fetching schema map from {map_url}: {e}")
                return all_schema_files
    except requests.RequestException:
        pass  # Try schema_map.xml next

    # If no robots.txt or no schemaMap directives, try schema_map.xml directly
    schema_map_url = urljoin(site_url + '/', 'schema_map.xml')
    try:
        response = requests.get(schema_map_url, timeout=10)
        if response.status_code == 200:
            json_urls = parse_schema_map_xml(response.text, site_url)
            # Return triples of (site_url, schema_map_url, json_file_url)
            return [(site_url, schema_map_url, json_url) for json_url in json_urls]
    except requests.RequestException:
        pass

    # As a last resort, if the site_url itself ends with schema_map.xml, fetch it
    if site_url.endswith('schema_map.xml'):
        try:
            response = requests.get(site_url, timeout=10)
            if response.status_code == 200:
                base = site_url.rsplit('/', 1)[0] + '/'
                json_urls = parse_schema_map_xml(response.text, base)
                # Return triples of (site_url, schema_map_url, json_file_url)
                return [(site_url, site_url, json_url) for json_url in json_urls]
        except requests.RequestException as e:
            print(f"Error fetching schema map from {site_url}: {e}")

    print(f"No schema files found for {site_url}")
    return []

def add_schema_map_to_site(site_url, user_id, schema_map_url):
    """
    Add a schema map to a site (Level 2 logic):
    1. Fetch and parse the schema_map XML
    2. Add all JSON files to database
    3. Queue all files for processing
    Returns: (files_added_count, files_queued_count)
    """
    conn = None
    try:
        conn = db.get_connection()

        # Check if site exists, if not create it
        cursor = conn.cursor()
        cursor.execute("SELECT site_url FROM sites WHERE site_url = %s AND user_id = %s", (site_url, user_id))
        if not cursor.fetchone():
            db.add_site(conn, site_url, user_id)

        # Fetch and parse the schema_map to get all JSON file URLs
        response = requests.get(schema_map_url, timeout=10)
        if response.status_code != 200:
            print(f"[MASTER] Failed to fetch schema_map {schema_map_url}: HTTP {response.status_code}")
            return (0, 0)

        json_file_urls = parse_schema_map_xml(response.text, site_url)

        if not json_file_urls:
            print(f"[MASTER] No schema files found in {schema_map_url}")
            return (0, 0)

        # Create triples: (site_url, schema_map_url, json_file_url)
        files_to_add = [(site_url, schema_map_url, json_url) for json_url in json_file_urls]

        # Add all files to the database
        added_files, removed_files = db.update_site_files(conn, site_url, user_id, files_to_add)

        # Queue jobs for NEW files only
        queue = get_queue()
        queued_count = 0

        for file_url in added_files:
            try:
                job = {
                    'type': 'process_file',
                    'user_id': user_id,  # Add user_id to job
                    'site': site_url,
                    'file_url': file_url,
                    'schema_map': schema_map_url,
                    'queued_at': datetime.utcnow().isoformat()
                }
                success = queue.send_message(job)
                if success:
                    log_queue_operation('queue_file', job, success=True)
                    queued_count += 1
                else:
                    log_queue_operation('queue_file', job, success=False, error="send_message returned False")
            except Exception as e:
                log_queue_operation('queue_file', job, success=False, error=e)

        # Queue jobs for REMOVED files
        for file_url in removed_files:
            try:
                job = {
                    'type': 'process_removed_file',
                    'user_id': user_id,  # Add user_id to job
                    'site': site_url,
                    'file_url': file_url,
                    'queued_at': datetime.utcnow().isoformat()
                }
                success = queue.send_message(job)
                if success:
                    log_queue_operation('queue_removed_file', job, success=True)
                else:
                    log_queue_operation('queue_removed_file', job, success=False, error="send_message returned False")
            except Exception as e:
                log_queue_operation('queue_removed_file', job, success=False, error=e)

        return (len(added_files), queued_count)

    except Exception as e:
        print(f"[MASTER] Error adding schema map {schema_map_url} to site {site_url}: {e}")
        return (0, 0)
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def process_site(site_url, user_id):
    """
    Process a site (Level 1 logic):
    1. Discover schema maps from robots.txt
    2. For each schema map, call add_schema_map_to_site (Level 2)
    """
    try:
        # Get schema map URLs from robots.txt
        # This returns triples, but we only need the unique schema_map URLs
        triples = get_schema_urls_from_robots(site_url)

        # Extract unique schema_map URLs
        schema_map_urls = list(set(schema_map for _, schema_map, _ in triples))

        if not schema_map_urls:
            print(f"[MASTER] No schema maps found for {site_url}")
            return False

        print(f"[MASTER] Found {len(schema_map_urls)} schema map(s) for {site_url}")

        # For each discovered schema map, use Level 2 logic to add it
        total_files = 0
        total_queued = 0
        for schema_map_url in schema_map_urls:
            print(f"[MASTER] Adding schema map: {schema_map_url}")
            files_added, files_queued = add_schema_map_to_site(site_url, user_id, schema_map_url)
            total_files += files_added
            total_queued += files_queued

        print(f"[MASTER] Processed {site_url}: {total_files} files added, {total_queued} queued")
        return True

    except Exception as e:
        print(f"[MASTER] Unexpected error processing {site_url}: {e}")
        import traceback
        traceback.print_exc()
        return False

# write_job function removed - now using queue interface

if __name__ == '__main__':
    # Simple command line interface
    import sys
    if len(sys.argv) != 2:
        print("Usage: python master.py <site_url>")
        sys.exit(1)
    process_site(sys.argv[1])