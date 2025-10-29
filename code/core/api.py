from flask import Flask, request, jsonify, send_from_directory, redirect, url_for
from flask_cors import CORS
from flask_login import login_user, logout_user
import db
from master import process_site
from queue_interface import get_queue
import asyncio
import os
import time
from datetime import datetime, timedelta
import json
import auth

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')
CORS(app)

# Initialize authentication
auth.init_auth(app)

# Global scheduler task
scheduler_task = None
scheduler_running = False
event_loop = None

# Track when master started
master_started_at = datetime.utcnow()

# ========== Authentication Routes ==========

@app.route('/login')
def login_page():
    """Show login page"""
    return send_from_directory('static', 'login.html')


@app.route('/faq')
def faq_page():
    """Show FAQ page"""
    return send_from_directory('static', 'faq.html')


@app.route('/auth/github')
def github_login():
    """Redirect to GitHub OAuth"""
    if not auth.github:
        return jsonify({'error': 'GitHub OAuth not configured'}), 500
    redirect_uri = url_for('github_callback', _external=True)
    return auth.github.authorize_redirect(redirect_uri)


@app.route('/auth/github/callback')
def github_callback():
    """Handle GitHub OAuth callback"""
    print("[AUTH] GitHub callback received")
    if not auth.github:
        print("[AUTH] GitHub OAuth not configured")
        return jsonify({'error': 'GitHub OAuth not configured'}), 500

    try:
        print("[AUTH] Authorizing GitHub access token...")
        token = auth.github.authorize_access_token()
        print("[AUTH] Getting GitHub user info...")
        resp = auth.github.get('user', token=token)
        user_info = resp.json()
        print(f"[AUTH] GitHub user info received: {user_info.get('login')}, id={user_info.get('id')}")

        # Get user email (may require additional API call)
        email = user_info.get('email')
        if not email:
            print("[AUTH] Email not in user info, fetching from /user/emails...")
            email_resp = auth.github.get('user/emails', token=token)
            emails = email_resp.json()
            # Get primary email
            for e in emails:
                if e.get('primary'):
                    email = e.get('email')
                    break
            if not email and emails:
                email = emails[0].get('email')
            print(f"[AUTH] Email fetched: {email}")

        # Create user_id from GitHub ID
        user_id = f"github:{user_info['id']}"
        name = user_info.get('name') or user_info.get('login')
        print(f"[AUTH] Creating user_id: {user_id}, name: {name}")

        # Get or create user
        user = auth.get_or_create_user(user_id, email, name, 'github')

        # Log user in
        login_user(user)
        print(f"[AUTH] User logged in successfully: {user_id}")

        # Redirect to main page
        return redirect('/')

    except Exception as e:
        print(f"[AUTH] GitHub OAuth error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Authentication failed'}), 500


@app.route('/auth/microsoft')
def microsoft_login():
    """Redirect to Microsoft OAuth"""
    if not auth.microsoft:
        return jsonify({'error': 'Microsoft OAuth not configured'}), 500
    redirect_uri = url_for('microsoft_callback', _external=True)
    return auth.microsoft.authorize_redirect(redirect_uri)


@app.route('/auth/microsoft/callback')
def microsoft_callback():
    """Handle Microsoft OAuth callback"""
    print("[AUTH] Microsoft callback received")
    if not auth.microsoft:
        print("[AUTH] Microsoft OAuth not configured")
        return jsonify({'error': 'Microsoft OAuth not configured'}), 500

    try:
        print("[AUTH] Authorizing Microsoft access token...")
        token = auth.microsoft.authorize_access_token()
        user_info = token.get('userinfo')

        if not user_info:
            print("[AUTH] Failed to get user info from token")
            return jsonify({'error': 'Failed to get user info'}), 500

        print(f"[AUTH] Microsoft user info received: oid={user_info.get('oid')}")

        # Create user_id from Microsoft OID
        user_id = f"microsoft:{user_info['oid']}"
        email = user_info.get('email') or user_info.get('preferred_username')
        name = user_info.get('name')
        print(f"[AUTH] Creating user_id: {user_id}, name: {name}, email: {email}")

        # Get or create user
        user = auth.get_or_create_user(user_id, email, name, 'microsoft')

        # Log user in
        login_user(user)
        print(f"[AUTH] User logged in successfully: {user_id}")

        # Redirect to main page
        return redirect('/')

    except Exception as e:
        print(f"[AUTH] Microsoft OAuth error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Authentication failed'}), 500


@app.route('/logout')
def logout():
    """Log out the current user"""
    logout_user()
    return redirect('/login')


@app.route('/api/me')
@auth.require_auth
def get_current_user_info():
    """Get current user information including API key"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        user_data = db.get_user_by_id(conn, user_id)
        if user_data:
            return jsonify({
                'user_id': user_data['user_id'],
                'email': user_data['email'],
                'name': user_data['name'],
                'provider': user_data['provider'],
                'api_key': user_data['api_key'],
                'created_at': user_data['created_at'].isoformat() if user_data['created_at'] else None,
                'last_login': user_data['last_login'].isoformat() if user_data['last_login'] else None
            })
        return jsonify({'error': 'User not found'}), 404
    finally:
        conn.close()


# Serve the frontend
@app.route('/')
@auth.require_auth
def index():
    return send_from_directory('static', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    # Allow access to login.html without authentication
    if path == 'login.html':
        return send_from_directory('static', path)
    return send_from_directory('static', path)

# API Routes

@app.route('/api/sites', methods=['GET'])
@auth.require_auth
def get_sites():
    """Get all sites with their status"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        sites = db.get_all_sites(conn, user_id)
        return jsonify(sites)
    finally:
        conn.close()

@app.route('/api/sites', methods=['POST'])
@auth.require_auth
def add_site():
    """Add a new site to monitor"""
    user_id = auth.get_current_user()
    try:
        data = request.json
        site_url = data.get('site_url')
        interval_hours = data.get('interval_hours', 24)

        if not site_url:
            return jsonify({'error': 'site_url is required'}), 400

        conn = db.get_connection()
        try:
            db.add_site(conn, site_url, user_id, interval_hours)
            # Process site immediately in background
            if event_loop:
                try:
                    asyncio.run_coroutine_threadsafe(process_site_async(site_url, user_id), event_loop)
                except Exception as e:
                    print(f"[API] Warning: Could not start async processing for {site_url}: {e}")
            return jsonify({'success': True, 'site_url': site_url})
        finally:
            conn.close()
    except Exception as e:
        print(f"[API] Error in add_site: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/sites/<path:site_url>', methods=['DELETE'])
@auth.require_auth
def delete_site(site_url):
    """Remove a site from monitoring by deleting all its schema_maps"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        cursor = conn.cursor()

        # Get all unique schema_maps for this site and user
        cursor.execute("""
            SELECT DISTINCT schema_map FROM files
            WHERE site_url = %s AND user_id = %s
        """, (site_url, user_id))
        schema_maps = [row[0] for row in cursor.fetchall()]

        # Delete each schema_map (queues removal jobs and deletes from DB)
        total_files_removed = 0
        for schema_map_url in schema_maps:
            files_removed = _delete_schema_map_internal(conn, site_url, user_id, schema_map_url)
            total_files_removed += files_removed

        # Finally delete the site itself
        cursor.execute("DELETE FROM sites WHERE site_url = %s AND user_id = %s", (site_url, user_id))
        conn.commit()

        return jsonify({
            'success': True,
            'schema_maps_removed': len(schema_maps),
            'files_queued_for_removal': total_files_removed
        })
    finally:
        conn.close()

def _delete_schema_map_internal(conn, site_url, user_id, schema_map_url):
    """Internal function to delete files for a schema_map and queue removal jobs"""
    from queue_interface_aad import get_queue_with_aad

    cursor = conn.cursor()

    # Get all files for this schema_map before deleting
    cursor.execute("""
        SELECT file_url FROM files
        WHERE site_url = %s AND user_id = %s AND schema_map = %s
    """, (site_url, user_id, schema_map_url))
    files = [row[0] for row in cursor.fetchall()]

    # Queue removal jobs for each file so workers can:
    # 1. Remove IDs from ids table
    # 2. Remove from vector DB
    # 3. Delete from files table
    queue = get_queue_with_aad()
    for file_url in files:
        job = {
            'type': 'process_removed_file',
            'user_id': user_id,  # Add user_id to job
            'site': site_url,
            'file_url': file_url
        }
        queue.send_message(job)

    # NOTE: Do NOT delete from files or ids tables here - workers will do that when they process the jobs
    # This ensures proper ordering: ids deleted first, then vector DB cleaned, then files table cleaned

    return len(files)

@app.route('/api/sites/<path:site_url>/schema-files', methods=['POST'])
@auth.require_auth
def add_schema_file(site_url):
    """Add a manual schema map to a specific site and extract all files from it"""
    from master import add_schema_map_to_site
    user_id = auth.get_current_user()

    data = request.json
    schema_map_url = data.get('schema_map_url')

    if not schema_map_url:
        return jsonify({'error': 'schema_map_url is required'}), 400

    try:
        # Use the Level 2 logic from master.py
        files_added, files_queued = add_schema_map_to_site(site_url, user_id, schema_map_url)

        if files_added == 0:
            return jsonify({'error': 'No schema files found or failed to fetch schema_map'}), 400

        return jsonify({
            'success': True,
            'site_url': site_url,
            'schema_map_url': schema_map_url,
            'files_added': files_added,
            'files_queued': files_queued
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sites/<path:site_url>/schema-files', methods=['DELETE'])
@auth.require_auth
def delete_schema_file(site_url):
    """Remove a schema map and all its files from a site"""
    user_id = auth.get_current_user()
    data = request.json
    schema_map_url = data.get('schema_map_url')

    if not schema_map_url:
        return jsonify({'error': 'schema_map_url is required'}), 400

    conn = db.get_connection()
    try:
        # Use the internal function to delete files and queue removal jobs
        files_removed = _delete_schema_map_internal(conn, site_url, user_id, schema_map_url)
        conn.commit()

        return jsonify({
            'success': True,
            'deleted_count': files_removed,
            'files_queued_for_removal': files_removed
        })
    finally:
        conn.close()

@app.route('/api/status', methods=['GET'])
@auth.require_auth
def get_status():
    """Get overall system status"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        sites_status = db.get_site_status(conn, user_id)
        # Return object with master info and sites array
        return jsonify({
            'master_started_at': master_started_at.isoformat(),
            'master_uptime_seconds': (datetime.utcnow() - master_started_at).total_seconds(),
            'sites': sites_status
        })
    finally:
        conn.close()

@app.route('/api/queue/status', methods=['GET'])
def get_queue_status():
    """Get queue processing status"""
    queue_type = os.getenv('QUEUE_TYPE', 'file')

    status = {
        'queue_type': queue_type,
        'pending_jobs': 0,
        'processing_jobs': 0,
        'failed_jobs': 0,
        'jobs': [],
        'error': None
    }

    try:
        if queue_type == 'file':
            # File-based queue status
            queue_dir = os.getenv('QUEUE_DIR', 'queue')
            status['queue_dir'] = queue_dir

            if os.path.exists(queue_dir):
                # Count pending jobs
                for filename in sorted(os.listdir(queue_dir), reverse=True):
                    if filename.startswith('job-') and filename.endswith('.json'):
                        status['pending_jobs'] += 1
                        # Read job details (limit to 20 most recent)
                        if len([j for j in status['jobs'] if j['status'] == 'pending']) < 20:
                            try:
                                with open(os.path.join(queue_dir, filename)) as f:
                                    job = json.load(f)
                                    status['jobs'].append({
                                        'id': filename,
                                        'status': 'pending',
                                        'type': job.get('type'),
                                        'site': job.get('site'),
                                        'file_url': job.get('file_url'),
                                        'queued_at': job.get('queued_at')
                                    })
                            except:
                                pass
                    elif filename.endswith('.processing'):
                        status['processing_jobs'] += 1
                        # Read job details
                        try:
                            filepath = os.path.join(queue_dir, filename)
                            mtime = os.path.getmtime(filepath)
                            age_seconds = int(time.time() - mtime)

                            with open(filepath) as f:
                                job = json.load(f)
                                status['jobs'].append({
                                    'id': filename,
                                    'status': 'processing',
                                    'type': job.get('type'),
                                    'site': job.get('site'),
                                    'file_url': job.get('file_url'),
                                    'queued_at': job.get('queued_at'),
                                    'processing_time': age_seconds
                                })
                        except:
                            pass

                # Count failed jobs
                error_dir = os.path.join(queue_dir, 'errors')
                if os.path.exists(error_dir):
                    for filename in os.listdir(error_dir):
                        if filename.startswith('job-') or filename.startswith('failed-'):
                            status['failed_jobs'] += 1

        elif queue_type == 'servicebus':
            # Azure Service Bus status
            try:
                from azure.servicebus import ServiceBusClient
                from azure.identity import DefaultAzureCredential

                conn_str = os.getenv('AZURE_SERVICEBUS_CONNECTION_STRING')
                namespace = os.getenv('AZURE_SERVICEBUS_NAMESPACE')
                queue_name = os.getenv('AZURE_SERVICE_BUS_QUEUE_NAME', 'crawler-queue')

                # Support both connection string and Azure AD authentication
                if conn_str:
                    client = ServiceBusClient.from_connection_string(conn_str)
                elif namespace:
                    # Use Azure AD authentication (Managed Identity or DefaultAzureCredential)
                    credential = DefaultAzureCredential()
                    fully_qualified_namespace = namespace if '.servicebus.windows.net' in namespace else f"{namespace}.servicebus.windows.net"
                    client = ServiceBusClient(fully_qualified_namespace, credential)
                else:
                    status['error'] = 'Azure Service Bus not configured (need connection string or namespace)'
                    return jsonify(status)

                with client.get_queue_receiver(queue_name, max_wait_time=1) as receiver:
                    # Peek at messages without consuming
                    messages = receiver.peek_messages(max_message_count=50)
                    status['pending_jobs'] = len(messages)

                    for msg in messages[:20]:  # Limit to 20 for display
                        try:
                            content = json.loads(str(msg))
                            status['jobs'].append({
                                'id': str(msg.message_id),
                                'status': 'pending',
                                'type': content.get('type'),
                                'site': content.get('site'),
                                'file_url': content.get('file_url'),
                                'queued_at': content.get('queued_at'),
                                'enqueued_time': str(msg.enqueued_time_utc) if msg.enqueued_time_utc else None
                            })
                        except:
                            pass
            except Exception as e:
                status['error'] = f'Error connecting to Service Bus: {str(e)}'

        elif queue_type == 'storage':
            # Azure Storage Queue status
            try:
                from azure.storage.queue import QueueServiceClient
                from azure.identity import DefaultAzureCredential

                storage_account = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
                queue_name = os.getenv('AZURE_STORAGE_QUEUE_NAME', 'crawler-jobs')

                if not storage_account:
                    status['error'] = 'Azure Storage Queue not configured (AZURE_STORAGE_ACCOUNT_NAME not set)'
                    return jsonify(status)

                # Use Azure AD authentication
                account_url = f"https://{storage_account}.queue.core.windows.net"
                credential = DefaultAzureCredential()
                service_client = QueueServiceClient(account_url=account_url, credential=credential)
                queue_client = service_client.get_queue_client(queue_name)

                properties = queue_client.get_queue_properties()
                status['pending_jobs'] = properties.get('approximate_message_count', 0)

                # Peek at messages
                messages = queue_client.peek_messages(max_messages=20)
                for msg in messages:
                    try:
                        content = json.loads(msg.content)
                        status['jobs'].append({
                            'id': msg.id,
                            'status': 'pending',
                            'type': content.get('type'),
                            'site': content.get('site'),
                            'file_url': content.get('file_url'),
                            'queued_at': content.get('queued_at'),
                            'inserted_on': str(msg.inserted_on) if msg.inserted_on else None
                        })
                    except:
                        pass
            except Exception as e:
                status['error'] = f'Error connecting to Storage Queue: {str(e)}'

    except Exception as e:
        status['error'] = f'Error getting queue status: {str(e)}'

    status['total_jobs'] = status['pending_jobs'] + status['processing_jobs'] + status['failed_jobs']

    # Sort jobs by status (processing first, then pending)
    status['jobs'].sort(key=lambda x: (x['status'] != 'processing', x.get('queued_at', '')), reverse=True)

    return jsonify(status)

@app.route('/api/process/<path:site_url>', methods=['POST'])
@auth.require_auth
def trigger_process(site_url):
    """Manually trigger processing for a site"""
    user_id = auth.get_current_user()
    try:
        # Process in background using asyncio
        if event_loop:
            try:
                asyncio.run_coroutine_threadsafe(process_site_async(site_url, user_id), event_loop)
            except Exception as e:
                print(f"[API] Warning: Could not trigger processing for {site_url}: {e}")
        else:
            print("[API] Warning: Event loop not initialized")
        return jsonify({'success': True, 'message': f'Processing started for {site_url}'})
    except Exception as e:
        print(f"[API] Error in trigger_process: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/status', methods=['GET'])
def get_scheduler_status():
    """Get scheduler status"""
    global scheduler_task, scheduler_running

    is_running = scheduler_running and scheduler_task and not scheduler_task.done()

    return jsonify({
        'running': is_running,
        'check_interval_seconds': 60
    })

@app.route('/api/scheduler/start', methods=['POST'])
def start_scheduler_endpoint():
    """Start the scheduler"""
    start_scheduler()
    return jsonify({'success': True, 'message': 'Scheduler started'})

@app.route('/api/scheduler/stop', methods=['POST'])
def stop_scheduler_endpoint():
    """Stop the scheduler"""
    stop_scheduler()
    return jsonify({'success': True, 'message': 'Scheduler stopped'})

@app.route('/api/sites/<path:site_url>/files', methods=['GET'])
@auth.require_auth
def get_site_files(site_url):
    """Get all files for a specific site"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT file_url, schema_map, last_read_time, number_of_items, is_manual, is_active
            FROM files
            WHERE site_url = %s AND user_id = %s AND is_active = 1
            ORDER BY file_url
        """, (site_url, user_id))

        files = [
            {
                'file_url': row[0],
                'schema_map': row[1],
                'last_read_time': row[2].isoformat() if row[2] else None,
                'number_of_items': row[3],
                'is_manual': bool(row[4]),
                'is_active': bool(row[5])
            }
            for row in cursor.fetchall()
        ]
        return jsonify(files)
    finally:
        conn.close()

@app.route('/api/files', methods=['GET'])
@auth.require_auth
def get_all_files():
    """Get all files from the database with their IDs"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.site_url, f.file_url, f.schema_map, f.is_active, f.is_manual,
                   f.number_of_items, f.last_read_time,
                   COUNT(DISTINCT i.id) as id_count
            FROM files f
            LEFT JOIN ids i ON f.file_url = i.file_url AND f.user_id = i.user_id
            WHERE f.user_id = %s
            GROUP BY f.site_url, f.file_url, f.schema_map, f.is_active, f.is_manual,
                     f.number_of_items, f.last_read_time
            ORDER BY f.site_url, f.file_url
        """, (user_id,))

        files = [
            {
                'site_url': row[0],
                'file_url': row[1],
                'schema_map': row[2],
                'is_active': bool(row[3]),
                'is_manual': bool(row[4]),
                'number_of_items': row[5],
                'last_read_time': row[6].isoformat() if row[6] else None,
                'id_count': row[7]
            }
            for row in cursor.fetchall()
        ]
        return jsonify(files)
    finally:
        conn.close()

@app.route('/api/files/<path:file_url>/ids', methods=['GET'])
@auth.require_auth
def get_file_ids(file_url):
    """Get all IDs for a specific file"""
    user_id = auth.get_current_user()
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id
            FROM ids
            WHERE file_url = %s AND user_id = %s
            ORDER BY id
        """, (file_url, user_id))

        ids = [row[0] for row in cursor.fetchall()]
        return jsonify({
            'file_url': file_url,
            'ids': ids,
            'count': len(ids)
        })
    finally:
        conn.close()

@app.route('/api/queue/history', methods=['GET'])
def get_queue_history():
    """Get queue history from log file"""
    import os
    QUEUE_LOG_FILE = '/app/data/queue_history.jsonl'

    try:
        if not os.path.exists(QUEUE_LOG_FILE):
            return jsonify([])

        # Read last 1000 lines
        history = []
        with open(QUEUE_LOG_FILE, 'r') as f:
            lines = f.readlines()
            for line in lines[-1000:]:
                try:
                    history.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        # Return in reverse chronological order (newest first)
        return jsonify(list(reversed(history)))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/fetch-log', methods=['GET'])
def get_fetch_log():
    """Get URL fetch log from workers"""
    import os
    FETCH_LOG_FILE = '/app/data/fetch_log.jsonl'

    try:
        if not os.path.exists(FETCH_LOG_FILE):
            return jsonify([])

        # Read last 1000 lines
        log_entries = []
        with open(FETCH_LOG_FILE, 'r') as f:
            lines = f.readlines()
            for line in lines[-1000:]:
                try:
                    log_entries.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        # Return in reverse chronological order (newest first)
        return jsonify(list(reversed(log_entries)))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/workers', methods=['GET'])
def get_workers():
    """Get all worker pods and their status"""
    try:
        import requests as req

        # Use Kubernetes API from within the cluster
        # Service account token and CA cert are automatically mounted
        k8s_host = os.getenv('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc')
        k8s_port = os.getenv('KUBERNETES_SERVICE_PORT', '443')
        namespace = 'crawler'

        # Read service account token
        with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as f:
            token = f.read()

        # API endpoint to list pods with label selector
        url = f'https://{k8s_host}:{k8s_port}/api/v1/namespaces/{namespace}/pods?labelSelector=app=crawler-worker'

        headers = {
            'Authorization': f'Bearer {token}'
        }

        # Get pods from Kubernetes API
        response = req.get(url, headers=headers, verify='/var/run/secrets/kubernetes.io/serviceaccount/ca.crt', timeout=10)

        if response.status_code != 200:
            return jsonify({'error': 'Failed to get worker pods from Kubernetes API', 'status': response.status_code}), 500

        pods_data = response.json()
        workers = []

        for pod in pods_data.get('items', []):
            pod_name = pod['metadata']['name']
            pod_ip = pod['status'].get('podIP', 'N/A')
            phase = pod['status'].get('phase', 'Unknown')

            worker_info = {
                'name': pod_name,
                'ip': pod_ip,
                'phase': phase,
                'status': None,
                'error': None
            }

            # Try to fetch status from worker if it's running
            if phase == 'Running' and pod_ip and pod_ip != 'N/A':
                try:
                    response = req.get(f'http://{pod_ip}:8080/status', timeout=2)
                    if response.status_code == 200:
                        worker_info['status'] = response.json()
                except Exception as e:
                    worker_info['error'] = str(e)

            workers.append(worker_info)

        return jsonify(workers)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

async def process_site_async(site_url, user_id):
    """Async wrapper for process_site function"""
    try:
        # Run process_site in a thread pool to avoid blocking the event loop
        await asyncio.get_event_loop().run_in_executor(None, process_site, site_url, user_id)
    except Exception as e:
        print(f"[API] Error processing site {site_url}: {e}")

async def scheduler_loop():
    """Background scheduler that periodically checks sites for reprocessing"""
    global scheduler_running

    print("[SCHEDULER] Started background scheduler")

    while scheduler_running:
        try:
            conn = db.get_connection()
            cursor = conn.cursor()

            # Get sites that need reprocessing (with user_id)
            # Check sites where last_processed + interval_hours < now OR never processed
            cursor.execute("""
                SELECT site_url, user_id, process_interval_hours, last_processed
                FROM sites
                WHERE is_active = 1
                  AND (
                    last_processed IS NULL
                    OR DATEADD(hour, process_interval_hours, last_processed) <= GETUTCDATE()
                  )
            """)

            sites_to_process = cursor.fetchall()

            if sites_to_process:
                print(f"[SCHEDULER] Found {len(sites_to_process)} sites to process")

                # Create tasks for all sites to process concurrently
                tasks = []
                for site_url, user_id, interval_hours, last_processed in sites_to_process:
                    if last_processed:
                        time_since = datetime.utcnow() - last_processed
                        print(f"[SCHEDULER] Processing {site_url} for user {user_id} (last processed {time_since} ago)")
                    else:
                        print(f"[SCHEDULER] Processing {site_url} for user {user_id} (never processed before)")

                    # Add to task list for concurrent processing
                    tasks.append(process_site_async(site_url, user_id))

                # Process all sites concurrently
                if tasks:
                    # Use return_exceptions=True to prevent one site error from killing all
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            print(f"[SCHEDULER] Error processing site {sites_to_process[i][0]}: {result}")

            conn.close()

        except Exception as e:
            print(f"[SCHEDULER] Error in scheduler loop: {e}")
            # Try to close connection if it exists
            try:
                if 'conn' in locals() and conn:
                    conn.close()
            except:
                pass

        # Sleep for 60 seconds between checks
        await asyncio.sleep(60)

    print("[SCHEDULER] Stopped")

def start_scheduler():
    """Start the background scheduler task"""
    global scheduler_task, scheduler_running, event_loop

    if scheduler_task and not scheduler_task.done():
        print("[SCHEDULER] Already running")
        return

    scheduler_running = True
    if event_loop:
        scheduler_task = asyncio.run_coroutine_threadsafe(scheduler_loop(), event_loop)
        print("[SCHEDULER] Starting background scheduler task")

def stop_scheduler():
    """Stop the background scheduler task"""
    global scheduler_running, scheduler_task
    scheduler_running = False
    print("[SCHEDULER] Stopping scheduler...")
    if scheduler_task:
        scheduler_task.cancel()

def run_event_loop():
    """Run the asyncio event loop in a separate thread"""
    global event_loop
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    event_loop.run_forever()

if __name__ == '__main__':
    # Ensure database tables exist
    print("[STARTUP] Testing database connection...")
    conn = db.get_connection()
    db.create_tables(conn)
    conn.close()
    print("[STARTUP] ✓ Database connection successful")

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

    # Start asyncio event loop in background
    import threading
    loop_thread = threading.Thread(target=run_event_loop, daemon=True)
    loop_thread.start()

    # Wait a moment for the event loop to start
    time.sleep(0.5)

    # Start the scheduler
    start_scheduler()

    # Run the Flask app (use 5001 to avoid macOS AirPlay conflict)
    port = int(os.getenv('API_PORT', 5001))

    try:
        app.run(host='0.0.0.0', port=port, debug=False)  # debug=False to avoid duplicate scheduler
    finally:
        stop_scheduler()
        if event_loop:
            event_loop.call_soon_threadsafe(event_loop.stop)