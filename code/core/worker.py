
import requests
import urllib.parse
import os
import time
import json
import sys
import threading
from datetime import datetime
from flask import Flask, jsonify
# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))
import config  # Load environment variables
import db
from vector_db import vector_db_add, vector_db_delete
from scheduler import update_site_last_processed

# Import appropriate queue interface based on QUEUE_TYPE
queue_type = os.getenv('QUEUE_TYPE', 'file')
if queue_type == 'storage':
    from queue_interface_storage import get_queue_with_aad as get_queue
else:
    from queue_interface_aad import get_queue_with_aad as get_queue

# Global worker status
worker_status = {
    'worker_id': os.getenv('HOSTNAME', 'unknown'),
    'started_at': datetime.utcnow().isoformat(),
    'current_job': None,
    'total_jobs_processed': 0,
    'total_jobs_failed': 0,
    'last_job_at': None,
    'last_job_status': None,
    'status': 'idle'
}

# Log files
VECTOR_DB_LOG_FILE = '/app/data/vector_db_additions.jsonl'
FETCH_LOG_FILE = '/app/data/fetch_log.jsonl'

def log_vector_db_addition(item_id, site_url, item_data):
    """Log items added to vector database"""
    try:
        os.makedirs(os.path.dirname(VECTOR_DB_LOG_FILE), exist_ok=True)
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'worker_id': worker_status['worker_id'],
            'id': item_id,
            'site': site_url,
            'data': item_data
        }
        with open(VECTOR_DB_LOG_FILE, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"[WORKER] Error logging vector DB addition: {e}")

def log_fetch(url, status_code, content_length, num_ids, error=None):
    """Log every URL fetch attempt with details"""
    try:
        os.makedirs(os.path.dirname(FETCH_LOG_FILE), exist_ok=True)
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'worker_id': worker_status['worker_id'],
            'url': url,
            'status_code': status_code,
            'content_length': content_length,
            'num_ids_extracted': num_ids,
            'error': error
        }
        with open(FETCH_LOG_FILE, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    except Exception as e:
        print(f"[WORKER] Error logging fetch: {e}")

def process_json_array(json_array):
    """
    Helper function to process an array of JSON objects and extract @id values.
    
    Args:
        json_array (list): List of JSON objects to process
        
    Returns:
        tuple: (list of @id values, list of JSON objects)
    """
    ids = []
    objects = []
    for item in json_array:
        if isinstance(item, dict) and '@id' in item:
            ids.append(item['@id'])
            objects.append(item)
    return ids, objects

def extract_schema_data_from_url(url):
    """
    Extracts schema data from a URL containing JSON content.

    Args:
        url (str): URL to fetch JSON data from

    Returns:
        tuple: (list of @id values, list of JSON objects)
    """
    try:
        # Fetch and parse JSON content
        print(f"[WORKER] Fetching {url}")
        response = requests.get(url, timeout=30)
        status_code = response.status_code
        content_length = len(response.content)

        response.raise_for_status()
        print(f"[WORKER] Fetched {url}: {status_code} status, {content_length} bytes")

        json_data = response.json()

        # Default case: no valid schema data found
        if type(json_data) is not dict and type(json_data) is not list:
            print(f"[WORKER] No valid schema data found in {url}")
            log_fetch(url, status_code, content_length, 0, error="No valid schema data found")
            return [], []
        
        json_data = [json_data] if not isinstance(json_data, list) else json_data

        ids, objects = process_json_array(json_data)
        for obj in json_data:
            # Check for @graph arrays within each object which do not have an @id
            if isinstance(obj, dict) and '@graph' in obj and '@id' not in obj and isinstance(obj['@graph'], list):
                graph_ids, graph_objects = process_json_array(obj['@graph'])
                ids.extend(graph_ids)
                objects.extend(graph_objects)
        log_fetch(url, status_code, content_length, len(ids))
        print(f"[WORKER] Extracted {len(ids)} IDs from array in {url}")
        return ids, objects





    except requests.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"[WORKER] Error fetching {url}: {error_msg}")
        log_fetch(url, getattr(e.response, 'status_code', None) if hasattr(e, 'response') and e.response else None, 0, 0, error=error_msg)
        return [], []
    except ValueError as e:
        error_msg = f"JSON parse error: {str(e)}"
        print(f"[WORKER] Error parsing JSON from {url}: {error_msg}")
        log_fetch(url, None, 0, 0, error=error_msg)
        return [], []
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[WORKER] Unexpected error processing {url}: {error_msg}")
        log_fetch(url, None, 0, 0, error=error_msg)
        return [], []
    



def process_job(conn, job):
    """Process a single job from the queue"""
    try:
        # Extract user_id from job
        user_id = job.get('user_id')
        if not user_id:
            print(f"[WORKER] WARNING: Job missing user_id, skipping: {job}")
            return False

        if job['type'] == 'process_file':
            print(f"[WORKER] ========== Starting process_file for {job['file_url']} ==========")
            print(f"[WORKER] Job details - site: {job.get('site')}, user_id: {user_id}")

            # Check if the file still exists in the files table for this user
            cursor = conn.cursor()
            cursor.execute("SELECT file_url FROM files WHERE file_url = %s AND user_id = %s", (job['file_url'], user_id))
            if not cursor.fetchone():
                print(f"[WORKER] File no longer exists in database, skipping: {job['file_url']}")
                return True  # Job completed successfully (file was deleted)

            print(f"[WORKER] File exists in database, proceeding with extraction")

            # Use existing extract_schema_data_from_url which returns (ids, objects)
            print(f"[WORKER] Calling extract_schema_data_from_url for {job['file_url']}")
            try:
                ids, objects = extract_schema_data_from_url(job['file_url'])
            except Exception as e:
                error_msg = f"Failed to extract schema data: {str(e)}"
                print(f"[WORKER ERROR] {error_msg}")
                db.log_processing_error(conn, job['file_url'], user_id, 'extraction_failed', error_msg, str(e.__class__.__name__))
                return False

            print(f"[WORKER] Extracted {len(ids)} IDs, {len(objects)} objects from {job['file_url']}")

            # Log if no IDs extracted
            if len(ids) == 0:
                error_msg = "No schema.org objects with @id found in file"
                print(f"[WORKER WARNING] {error_msg}")
                db.log_processing_error(conn, job['file_url'], user_id, 'no_ids_found', error_msg, f"Objects: {len(objects)}")
                # Continue processing - this might not be an error for some files

            if len(ids) > 0:
                print(f"[WORKER] Sample IDs: {list(ids)[:3]}")
            if len(objects) > 0:
                print(f"[WORKER] Sample object @type: {objects[0].get('@type', 'unknown')}")

            # Update database state with the extracted IDs
            print(f"[WORKER] Updating file_ids in database...")
            added_ids, removed_ids = db.update_file_ids(conn, job['file_url'], user_id, set(ids))

            print(f"[WORKER] DB update: {len(added_ids)} added, {len(removed_ids)} removed")
            if len(added_ids) > 0:
                print(f"[WORKER] Sample added IDs: {list(added_ids)[:3]}")

            # Collect items to batch add to vector DB
            items_to_add = []
            skipped_existing = 0
            skipped_breadcrumbs = 0
            for id in added_ids:
                ref_count = db.count_id_references(conn, id, user_id)
                if ref_count == 1:
                    # First occurrence of this ID - prepare for batch add to vector DB
                    obj = next((obj for obj in objects if obj['@id'] == id), None)
                    if obj:
                        # Skip BreadcrumbList items
                        obj_type = obj.get('@type', '')
                        if obj_type == 'BreadcrumbList' or (isinstance(obj_type, list) and 'BreadcrumbList' in obj_type):
                            skipped_breadcrumbs += 1
                            print(f"[WORKER] Skipping BreadcrumbList item: {id}")
                            continue
                        items_to_add.append((id, job['site'], obj))
                    else:
                        print(f"[WORKER] WARNING: Could not find object for ID {id}")
                else:
                    skipped_existing += 1

            if skipped_existing > 0:
                print(f"[WORKER] Skipped {skipped_existing} IDs that already exist in other files")
            if skipped_breadcrumbs > 0:
                print(f"[WORKER] Skipped {skipped_breadcrumbs} BreadcrumbList items")

            # Batch add to vector DB
            if items_to_add:
                print(f"[WORKER] Preparing to batch add {len(items_to_add)} items to vector DB")
                print(f"[WORKER] Sample items to add: {[(id, site) for id, site, _ in items_to_add[:3]]}")
                from vector_db import vector_db_batch_add
                print(f"[WORKER] Calling vector_db_batch_add...")
                try:
                    vector_db_batch_add(items_to_add)
                    print(f"[WORKER] Successfully completed vector_db_batch_add for {len(items_to_add)} items")
                    # Log the additions
                    for id, site, obj in items_to_add:
                        log_vector_db_addition(id, site, obj)
                except Exception as e:
                    error_msg = f"Failed to add items to vector DB: {str(e)}"
                    print(f"[WORKER ERROR] {error_msg}")
                    import traceback
                    error_details = traceback.format_exc()
                    db.log_processing_error(conn, job['file_url'], user_id, 'vector_db_add_failed', error_msg, error_details)
                    # Don't return False - we still updated the IDs table, so mark as processed
            else:
                print(f"[WORKER] No new items to add to vector DB (all IDs already exist)")

            # Collect IDs to batch delete from vector DB
            ids_to_delete = []
            for id in removed_ids:
                ref_count = db.count_id_references(conn, id, user_id)
                if ref_count == 0:
                    # ID no longer exists in any file - prepare for batch delete
                    ids_to_delete.append(id)

            # Batch delete from vector DB
            if ids_to_delete:
                print(f"[WORKER] Batch deleting {len(ids_to_delete)} items from vector DB")
                from vector_db import vector_db_batch_delete
                vector_db_batch_delete(ids_to_delete)

            # Update the site's last_processed timestamp (Note: may need user_id in future)
            print(f"[WORKER] Updating site last_processed timestamp for {job['site']}")
            update_site_last_processed(job['site'])

            # Clear any previous errors for this file since it processed successfully
            db.clear_file_errors(conn, job['file_url'], user_id)

            print(f"[WORKER] ========== Completed process_file for {job['file_url']} ==========")
            return True

        elif job['type'] == 'process_removed_file':
            print(f"[WORKER] Processing removal: {job['file_url']}")

            # Get IDs that were in this file for this user
            ids = db.get_file_ids(conn, job['file_url'], user_id)
            print(f"[WORKER] Found {len(ids)} IDs to check for removal")

            # Remove all ID mappings for this file (deletes from ids table)
            db.update_file_ids(conn, job['file_url'], user_id, set())

            # Check each ID to see if it's gone globally (for this user)
            removed_from_vector_db = 0
            for id in ids:
                if db.count_id_references(conn, id, user_id) == 0:
                    # ID no longer exists in any file - remove from vector DB
                    print(f"[WORKER] Removing from vector DB: {id}")
                    vector_db_delete(id)
                    removed_from_vector_db += 1

            print(f"[WORKER] Removed {removed_from_vector_db} items from vector DB")

            # Now delete the file from the files table for this user
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE file_url = %s AND user_id = %s", (job['file_url'], user_id))
            conn.commit()
            print(f"[WORKER] Deleted file from files table: {job['file_url']}")

            return True

    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def start_status_server():
    """Start Flask server for worker status in a separate thread"""
    app = Flask(__name__)

    @app.route('/status')
    def status():
        return jsonify(worker_status)

    @app.route('/health')
    def health():
        return jsonify({'status': 'healthy'})

    # Run Flask in a separate thread
    port = int(os.getenv('WORKER_STATUS_PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def worker_loop():
    """Main worker loop using queue interface"""
    global worker_status

    # Get queue implementation
    queue = get_queue()

    conn = None

    def get_db_connection():
        """Get a fresh database connection"""
        nonlocal conn
        try:
            if conn:
                try:
                    # Test if connection is still alive
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                except:
                    # Connection is dead, close it
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None

            if not conn:
                conn = db.get_connection()
                print("[WORKER] Database connection established")

            return conn
        except Exception as e:
            print(f"[WORKER] Error getting database connection: {e}")
            return None

    try:
        print("[WORKER] Started worker with queue type:", os.getenv('QUEUE_TYPE', 'file'))
        worker_status['status'] = 'running'

        # Track when we last logged queue status
        last_queue_status_time = 0
        queue_status_interval = 30  # Log every 30 seconds

        while True:
            try:
                # Receive message from queue
                worker_status['status'] = 'waiting'

                # Log queue status periodically
                current_time = time.time()
                if current_time - last_queue_status_time >= queue_status_interval:
                    if hasattr(queue, 'get_message_count'):
                        count = queue.get_message_count()
                        if count >= 0:
                            print(f"[QUEUE STATUS] Approximate messages in queue: {count}")
                    last_queue_status_time = current_time

                message = queue.receive_message(visibility_timeout=300)  # 5 minute timeout

                if not message:
                    time.sleep(5)
                    continue

                job = message.content
                worker_status['status'] = 'processing'
                worker_status['current_job'] = job
                print(f"[WORKER] Processing: {job.get('file_url', job.get('type', 'unknown'))}")

                # Get fresh connection for each job
                conn = get_db_connection()
                if not conn:
                    print(f"[WORKER] Cannot connect to database, returning job to queue")
                    if not queue.return_message(message):
                        print(f"[WORKER] Warning: Could not return message to queue")
                    worker_status['current_job'] = None
                    time.sleep(10)  # Wait before retrying
                    continue

                # Process job
                try:
                    success = process_job(conn, job)
                except Exception as e:
                    print(f"[WORKER] Error processing job: {e}")
                    print(f"[WORKER] Full traceback:")
                    import traceback
                    traceback.print_exc()
                    print(f"[WORKER] Job details: {json.dumps(job, indent=2)}")
                    # Check if it's a connection error
                    if "Communication link failure" in str(e) or "08S01" in str(e):
                        print(f"[WORKER] Database connection lost, will reconnect on next job")
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None
                    success = False

                # Update status
                worker_status['last_job_at'] = datetime.utcnow().isoformat()
                worker_status['last_job_status'] = 'success' if success else 'failed'
                worker_status['current_job'] = None

                if success:
                    worker_status['total_jobs_processed'] += 1
                    # Delete message from queue
                    if not queue.delete_message(message):
                        print(f"[WORKER] Warning: Could not delete message from queue")
                else:
                    worker_status['total_jobs_failed'] += 1
                    # Return message to queue for retry
                    if not queue.return_message(message):
                        print(f"[WORKER] Warning: Could not return message to queue")

            except Exception as e:
                print(f"[WORKER] Error in main loop iteration: {e}")
                worker_status['status'] = 'error'
                worker_status['current_job'] = None
                # Sleep before retrying to avoid tight error loops
                time.sleep(5)

    except KeyboardInterrupt:
        print("[WORKER] Shutdown requested")
        worker_status['status'] = 'stopped'
    except Exception as e:
        print(f"[WORKER] Fatal error in worker loop: {e}")
        worker_status['status'] = 'crashed'
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == '__main__':
    # Test database connectivity first
    print("[STARTUP] Testing database connection...")
    try:
        test_conn = db.get_connection()
        test_conn.close()
        print("[STARTUP] ✓ Database connection successful")
    except Exception as e:
        print(f"[STARTUP] ✗ Database connection failed: {str(e)}")
        sys.exit(1)

    # Test Queue connectivity
    queue_type = os.getenv('QUEUE_TYPE', 'file')
    if queue_type == 'servicebus':
        print("[STARTUP] Testing Service Bus connection...")
        try:
            from azure.servicebus import ServiceBusClient
            from azure.identity import DefaultAzureCredential

            conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
            namespace = os.getenv('AZURE_SERVICEBUS_NAMESPACE')
            queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'crawler-queue')

            if conn_str:
                client = ServiceBusClient.from_connection_string(conn_str)
                print("[STARTUP] Using Service Bus connection string")
            elif namespace:
                credential = DefaultAzureCredential()
                fully_qualified_namespace = namespace if '.servicebus.windows.net' in namespace else f"{namespace}.servicebus.windows.net"
                client = ServiceBusClient(fully_qualified_namespace, credential)
                print(f"[STARTUP] Using Azure AD auth for namespace: {fully_qualified_namespace}")
            else:
                print("[STARTUP] ✗ Service Bus not configured - no connection string or namespace found")
                sys.exit(1)

            # Test connection by peeking at queue
            with client.get_queue_receiver(queue_name, max_wait_time=5) as receiver:
                receiver.peek_messages(max_message_count=1)
            print(f"[STARTUP] ✓ Service Bus connection successful (queue: {queue_name})")
        except Exception as e:
            print(f"[STARTUP] ✗ Service Bus connection failed: {str(e)}")
            sys.exit(1)
    elif queue_type == 'storage':
        print("[STARTUP] Testing Storage Queue connection...")
        try:
            from azure.storage.queue import QueueServiceClient
            from azure.identity import DefaultAzureCredential

            storage_account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
            queue_name = os.getenv('AZURE_STORAGE_QUEUE_NAME', 'crawler-jobs')

            if not storage_account:
                print("[STARTUP] ✗ Storage Queue not configured - AZURE_STORAGE_ACCOUNT_NAME not set")
                sys.exit(1)

            account_url = f"https://{storage_account}.queue.core.windows.net"
            credential = DefaultAzureCredential()
            service_client = QueueServiceClient(account_url=account_url, credential=credential)
            queue_client = service_client.get_queue_client(queue_name)

            # Test connection by checking queue properties
            queue_client.get_queue_properties()
            print(f"[STARTUP] ✓ Storage Queue connection successful (queue: {queue_name})")

            # Ensure queue exists (create if needed)
            from queue_interface_storage import ensure_queue_exists
            ensure_queue_exists(storage_account, queue_name)
        except Exception as e:
            print(f"[STARTUP] ✗ Storage Queue connection failed: {str(e)}")
            sys.exit(1)

    # Start status server in background thread
    print("[STARTUP] Starting status server on port 8080...")
    status_thread = threading.Thread(target=start_status_server, daemon=True)
    status_thread.start()
    print("[STARTUP] ✓ Status server started")

    # Start worker loop
    worker_loop()