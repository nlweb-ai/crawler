import pymssql
from datetime import datetime
import os
import threading
from collections import defaultdict
import re
import config  # This will automatically load .env file

# Per-site semaphores to prevent concurrent operations on the same site
_site_locks = defaultdict(lambda: threading.Semaphore(1))
_lock_mutex = threading.Lock()  # Mutex to protect the _site_locks dictionary

def normalize_site_url(site_url):
    """
    Normalize site URL by removing protocol and www prefix.
    Examples:
        https://www.imdb.com -> imdb.com
        http://example.com -> example.com
        www.site.org -> site.org
        site.com -> site.com
    """
    if not site_url:
        return site_url

    # Remove protocol (http:// or https://)
    url = re.sub(r'^https?://', '', site_url)

    # Remove www. prefix
    url = re.sub(r'^www\.', '', url)

    # Remove trailing slash
    url = url.rstrip('/')

    return url

def get_site_lock(site_url):
    """Get or create a semaphore for a specific site"""
    with _lock_mutex:
        return _site_locks[site_url]

def get_connection():
    """Get connection to Azure SQL Database using pymssql (simpler than ODBC)"""
    server = os.getenv('DB_SERVER') or os.getenv('AZURE_SQL_SERVER')
    database = os.getenv('DB_DATABASE') or os.getenv('AZURE_SQL_DATABASE')
    username = os.getenv('DB_USERNAME') or os.getenv('AZURE_SQL_USERNAME')
    password = os.getenv('DB_PASSWORD') or os.getenv('AZURE_SQL_PASSWORD')

    # Remove port if present in server string
    if ':' in server:
        server = server.split(':')[0]

    # Simple connection using pymssql - no ODBC complexity
    # TDS version and encryption configured in /etc/freetds/freetds.conf
    conn = pymssql.connect(
        server=server,
        user=username,
        password=password,
        database=database
    )
    return conn

def create_tables(conn: pymssql.Connection):
    """Create tables if they don't exist"""
    cursor = conn.cursor()

    # Users table for OAuth authentication
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
    CREATE TABLE users (
        user_id VARCHAR(255) PRIMARY KEY,
        email VARCHAR(255),
        name VARCHAR(255),
        provider VARCHAR(50),
        api_key VARCHAR(64) UNIQUE,
        created_at DATETIME DEFAULT GETUTCDATE(),
        last_login DATETIME DEFAULT GETUTCDATE()
    )
    """)

    # Add api_key column if it doesn't exist (for existing tables)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('users') AND name = 'api_key')
    ALTER TABLE users ADD api_key VARCHAR(64) UNIQUE
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sites')
    CREATE TABLE sites (
        site_url VARCHAR(500),
        user_id VARCHAR(255),
        process_interval_hours INT DEFAULT 24,
        last_processed DATETIME,
        is_active BIT DEFAULT 1,
        created_at DATETIME DEFAULT GETUTCDATE(),
        PRIMARY KEY (site_url, user_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'files')
    CREATE TABLE files (
        site_url VARCHAR(500),
        user_id VARCHAR(255),
        file_url VARCHAR(500),
        schema_map VARCHAR(500),
        last_read_time DATETIME,
        number_of_items INT,
        is_manual BIT DEFAULT 0,
        is_active BIT DEFAULT 1,
        PRIMARY KEY (file_url, user_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ids')
    CREATE TABLE ids (
        file_url VARCHAR(500),
        user_id VARCHAR(255),
        id VARCHAR(500),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processing_errors (
        id INT IDENTITY(1,1) PRIMARY KEY,
        file_url VARCHAR(500) NOT NULL,
        user_id VARCHAR(255) NOT NULL,
        error_type VARCHAR(100) NOT NULL,
        error_message VARCHAR(MAX),
        error_details VARCHAR(MAX),
        occurred_at DATETIME DEFAULT GETUTCDATE(),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """)

    conn.commit()

def log_processing_error(conn: pymssql.Connection, file_url, user_id, error_type, error_message, error_details=None):
    """Log a processing error for a file"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO processing_errors (file_url, user_id, error_type, error_message, error_details)
        VALUES (%s, %s, %s, %s, %s)
    """, (file_url, user_id, error_type, error_message, error_details))
    conn.commit()

def get_file_errors(conn: pymssql.Connection, file_url, user_id, limit=50):
    """Get recent errors for a file"""
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""
        SELECT TOP (%s) error_type, error_message, error_details, occurred_at
        FROM processing_errors
        WHERE file_url = %s AND user_id = %s
        ORDER BY occurred_at DESC
    """, (limit, file_url, user_id))
    return cursor.fetchall()

def clear_file_errors(conn: pymssql.Connection, file_url: str, user_id: str):
    """Clear all errors for a file (called when file successfully processes)"""
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM processing_errors
        WHERE file_url = %s AND user_id = %s
    """, (file_url, user_id))
    conn.commit()

def get_site_files(conn: pymssql.Connection, site_url: str, user_id: str):
    """Get all active files currently associated with a site"""
    cursor = conn.cursor()
    cursor.execute('SELECT file_url FROM files WHERE site_url = %s AND user_id = %s AND is_active = 1', (site_url, user_id))
    return [row[0] for row in cursor.fetchall()]

def update_site_files(conn: pymssql.Connection, site_url: str, user_id: str, current_files: list[tuple[str, str, str]]):
    """Update files for a site, returns (added_files, removed_files)

    Args:
        site_url: Site URL
        user_id: User ID
        current_files: List of triples (site_url, schema_map_url, file_url)
    """
    # Acquire semaphore for this site to prevent concurrent modifications
    site_lock = get_site_lock(site_url)

    with site_lock:
        cursor = conn.cursor()

        existing_files = get_site_files(conn, site_url, user_id)

        # Convert current_files to dict for easy lookup
        # Triples format: (site_url, schema_map_url, file_url)
        current_files_dict = {file_url: schema_map for _, schema_map, file_url in current_files}

        current_set = set(current_files_dict.keys())
        existing_set = set(existing_files)
        added = current_set - existing_set
        removed = existing_set - current_set

        # For "added" files, use MERGE pattern to handle existing records
        for file_url in added:
            schema_map = current_files_dict[file_url]
            # Use MERGE statement for atomic upsert
            cursor.execute("""
                MERGE files AS target
                USING (SELECT %s AS site_url, %s AS user_id, %s AS file_url, %s AS schema_map) AS source
                ON target.file_url = source.file_url AND target.user_id = source.user_id
                WHEN MATCHED THEN
                    UPDATE SET is_active = 1, site_url = source.site_url, schema_map = source.schema_map
                WHEN NOT MATCHED THEN
                    INSERT (site_url, user_id, file_url, schema_map, is_active) VALUES (source.site_url, source.user_id, source.file_url, source.schema_map, 1);
            """, (site_url, user_id, file_url, schema_map))

        if removed:
            # Mark removed files as inactive instead of deleting
            cursor.execute(
                'UPDATE files SET is_active = 0 WHERE site_url = %s AND user_id = %s AND file_url IN ({})'.format(
                    ','.join('%s' * len(removed))
                ),
                tuple([site_url, user_id] + list(removed))
            )

        conn.commit()
        return (list(added), list(removed))

def get_file_ids(conn: pymssql.Connection, file_url: str, user_id: str) -> set[str]:
    """Get all IDs associated with a file"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM ids WHERE file_url = %s AND user_id = %s', (file_url, user_id))
    return {row[0] for row in cursor.fetchall()}

def update_file_ids(conn: pymssql.Connection, file_url: str, user_id: str, current_ids: set[str]):
    """Update IDs for a file, returns (added_ids, removed_ids)"""
    cursor = conn.cursor()

    existing_ids = get_file_ids(conn, file_url, user_id)

    added = current_ids - existing_ids
    removed = existing_ids - current_ids

    if added:
        cursor.executemany(
            'INSERT INTO ids (file_url, user_id, id) VALUES (%s, %s, %s)',
            [(file_url, user_id, id) for id in added]
        )

    if removed:
        # If removing all IDs (current_ids is empty), use simple DELETE
        # to avoid SQL Server's 2100 parameter limit
        if not current_ids:
            cursor.execute(
                'DELETE FROM ids WHERE file_url = %s AND user_id = %s',
                (file_url, user_id)
            )
        else:
            # Batch deletions to avoid parameter limit (max 2100 params in SQL Server)
            # Use batches of 500 to be safe
            removed_list = list(removed)
            batch_size = 500
            for i in range(0, len(removed_list), batch_size):
                batch = removed_list[i:i + batch_size]
                cursor.execute(
                    'DELETE FROM ids WHERE file_url = %s AND user_id = %s AND id IN ({})'.format(
                        ','.join('%s' * len(batch))
                    ),
                    tuple([file_url, user_id] + batch)
                )

    cursor.execute(
        'UPDATE files SET last_read_time = GETUTCDATE(), number_of_items = %s WHERE file_url = %s AND user_id = %s',
        (len(current_ids), file_url, user_id)
    )

    conn.commit()
    return (list(added), list(removed))

def count_id_references(conn: pymssql.Connection, id: str, user_id: str):
    """Count how many files reference an ID"""
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM ids WHERE id = %s AND user_id = %s', (id, user_id))
    return cursor.fetchone()[0]

def clear_all_data(conn: pymssql.Connection):
    """Clear all data from database tables (for testing)"""
    cursor = conn.cursor()

    print("Clearing database tables...")

    # Delete in correct order due to foreign keys
    cursor.execute("DELETE FROM ids")
    print(f"  Deleted {cursor.rowcount} rows from ids table")

    cursor.execute("DELETE FROM files")
    print(f"  Deleted {cursor.rowcount} rows from files table")

    cursor.execute("DELETE FROM sites")
    print(f"  Deleted {cursor.rowcount} rows from sites table")

    conn.commit()
    print("  ✓ Database cleared successfully")

def get_all_sites(conn: pymssql.Connection, user_id: str):
    """Get all sites with their status"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT site_url, process_interval_hours, last_processed, is_active, created_at
        FROM sites
        WHERE user_id = %s
        ORDER BY site_url
    """, (user_id,))
    return [
        {
            'site_url': row[0],
            'process_interval_hours': row[1],
            'last_processed': row[2].isoformat() if row[2] else None,
            'is_active': bool(row[3]),
            'created_at': row[4].isoformat() if row[4] else None
        }
        for row in cursor.fetchall()
    ]

def add_site(conn: pymssql.Connection, site_url: str, user_id: str, interval_hours: int = 24):
    """Add a new site to monitor"""
    # Normalize site URL
    site_url = normalize_site_url(site_url)

    cursor = conn.cursor()

    # Check if site already exists for this user
    cursor.execute("SELECT site_url FROM sites WHERE site_url = %s AND user_id = %s", (site_url, user_id))
    if cursor.fetchone():
        # Update existing site
        cursor.execute("""
            UPDATE sites
            SET process_interval_hours = %s, is_active = 1
            WHERE site_url = %s AND user_id = %s
        """, (interval_hours, site_url, user_id))
        print(f"Site {site_url} already exists - updated settings")
    else:
        # Insert new site
        cursor.execute("""
            INSERT INTO sites (site_url, user_id, process_interval_hours)
            VALUES (%s, %s, %s)
        """, (site_url, user_id, interval_hours))
        print(f"Site {site_url} added successfully")

    conn.commit()

def remove_site(conn: pymssql.Connection, site_url: str, user_id: str):
    """Remove a site (hard delete from database)"""
    cursor = conn.cursor()

    # Delete in correct order due to foreign keys
    # First delete IDs associated with files from this site
    cursor.execute("""
        DELETE FROM ids
        WHERE user_id = %s AND file_url IN (SELECT file_url FROM files WHERE site_url = %s AND user_id = %s)
    """, (user_id, site_url, user_id))

    # Then delete files from this site
    cursor.execute("""
        DELETE FROM files WHERE site_url = %s AND user_id = %s
    """, (site_url, user_id))

    # Finally delete the site itself
    cursor.execute("""
        DELETE FROM sites WHERE site_url = %s AND user_id = %s
    """, (site_url, user_id))

    conn.commit()

def add_manual_schema_file(conn: pymssql.Connection, site_url: str, user_id: str, file_url: str, schema_map=None):
    """Add a manual schema file for a site"""
    cursor = conn.cursor()
    # Check if file exists first
    cursor.execute("SELECT file_url FROM files WHERE file_url = %s AND user_id = %s", (file_url, user_id))
    if cursor.fetchone():
        # Update existing file
        cursor.execute("""
            UPDATE files SET is_active = 1, is_manual = 1, schema_map = %s
            WHERE file_url = %s AND user_id = %s
        """, (schema_map, file_url, user_id))
    else:
        # Insert new file
        cursor.execute("""
            INSERT INTO files (site_url, user_id, file_url, schema_map, is_manual, is_active)
            VALUES (%s, %s, %s, %s, 1, 1)
        """, (site_url, user_id, file_url, schema_map))
    conn.commit()

def remove_schema_file(conn: pymssql.Connection, file_url: str, user_id: str):
    """Remove a schema file (soft delete)"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE files SET is_active = 0 WHERE file_url = %s AND user_id = %s
    """, (file_url, user_id))
    conn.commit()

def get_site_status(conn: pymssql.Connection, user_id: str):
    """Get status information for all sites for a specific user"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            s.site_url,
            s.is_active,
            s.last_processed,
            COUNT(DISTINCT f.file_url) as total_files,
            COUNT(DISTINCT CASE WHEN f.is_manual = 1 THEN f.file_url END) as manual_files,
            COUNT(DISTINCT i.id) as total_ids
        FROM sites s
        LEFT JOIN files f ON s.site_url = f.site_url AND s.user_id = f.user_id AND f.is_active = 1
        LEFT JOIN ids i ON f.file_url = i.file_url AND f.user_id = i.user_id
        WHERE s.user_id = %s
        GROUP BY s.site_url, s.is_active, s.last_processed
        ORDER BY s.site_url
    """, (user_id,))
    return [
        {
            'site_url': row[0],
            'is_active': bool(row[1]),
            'last_processed': row[2].isoformat() if row[2] else None,
            'total_files': row[3],
            'manual_files': row[4],
            'total_ids': row[5]
        }
        for row in cursor.fetchall()
    ]


# ========== User Management Functions ==========

def get_user_by_api_key(conn: pymssql.Connection, api_key: str) -> dict | None:
    """Get user by API key. Returns `None` if not found."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, email, name, provider, api_key, created_at, last_login
        FROM users
        WHERE api_key = %s
    """, (api_key,))
    row = cursor.fetchone()
    if row:
        return {
            'user_id': row[0],
            'email': row[1],
            'name': row[2],
            'provider': row[3],
            'api_key': row[4],
            'created_at': row[5],
            'last_login': row[6]
        }
    return None


def get_user_by_id(conn: pymssql.Connection, user_id: str) -> dict | None:
    """Get user by user_id"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, email, name, provider, api_key, created_at, last_login
        FROM users
        WHERE user_id = %s
    """, (user_id,))
    row = cursor.fetchone()
    if row:
        return {
            'user_id': row[0],
            'email': row[1],
            'name': row[2],
            'provider': row[3],
            'api_key': row[4],
            'created_at': row[5],
            'last_login': row[6]
        }
    return None


def create_user(conn: pymssql.Connection, user_id: str, email: str, name: str, provider: str) -> str:
    """Create a new user with auto-generated API key"""
    import secrets
    cursor = conn.cursor()

    # Generate secure random API key
    api_key = secrets.token_urlsafe(48)

    cursor.execute("""
        INSERT INTO users (user_id, email, name, provider, api_key, created_at, last_login)
        VALUES (%s, %s, %s, %s, %s, GETUTCDATE(), GETUTCDATE())
    """, (user_id, email, name, provider, api_key))
    conn.commit()

    return api_key


def update_user_login(conn: pymssql.Connection, user_id: str):
    """Update last_login timestamp for a user"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE users
        SET last_login = GETUTCDATE()
        WHERE user_id = %s
    """, (user_id,))
    conn.commit()


def get_user_api_key(conn: pymssql.Connection, user_id: str) -> str | None:
    """Get user's API key"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT api_key FROM users WHERE user_id = %s
    """, (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None