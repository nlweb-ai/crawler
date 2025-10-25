import pymssql
from datetime import datetime
import os
import threading
from collections import defaultdict
import config  # This will automatically load .env file

# Per-site semaphores to prevent concurrent operations on the same site
_site_locks = defaultdict(lambda: threading.Semaphore(1))
_lock_mutex = threading.Lock()  # Mutex to protect the _site_locks dictionary

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

def create_tables(conn):
    """Create tables if they don't exist"""
    cursor = conn.cursor()

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sites')
    CREATE TABLE sites (
        site_url VARCHAR(500) PRIMARY KEY,
        process_interval_hours INT DEFAULT 24,
        last_processed DATETIME,
        is_active BIT DEFAULT 1,
        created_at DATETIME DEFAULT GETUTCDATE()
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'files')
    CREATE TABLE files (
        site_url VARCHAR(500),
        file_url VARCHAR(500) PRIMARY KEY,
        schema_map VARCHAR(500),
        last_read_time DATETIME,
        number_of_items INT,
        is_manual BIT DEFAULT 0,
        is_active BIT DEFAULT 1
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ids')
    CREATE TABLE ids (
        file_url VARCHAR(500),
        id VARCHAR(500),
        FOREIGN KEY (file_url) REFERENCES files(file_url)
    )
    """)

    conn.commit()

def get_site_files(conn, site_url):
    """Get all active files currently associated with a site"""
    cursor = conn.cursor()
    cursor.execute('SELECT file_url FROM files WHERE site_url = %s AND is_active = 1', (site_url,))
    return [row[0] for row in cursor.fetchall()]

def update_site_files(conn, site_url, current_files):
    """Update files for a site, returns (added_files, removed_files)

    Args:
        current_files: List of triples (site_url, schema_map_url, file_url)
    """
    # Acquire semaphore for this site to prevent concurrent modifications
    site_lock = get_site_lock(site_url)

    with site_lock:
        cursor = conn.cursor()

        existing_files = get_site_files(conn, site_url)

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
                USING (SELECT %s AS site_url, %s AS file_url, %s AS schema_map) AS source
                ON target.file_url = source.file_url
                WHEN MATCHED THEN
                    UPDATE SET is_active = 1, site_url = source.site_url, schema_map = source.schema_map
                WHEN NOT MATCHED THEN
                    INSERT (site_url, file_url, schema_map, is_active) VALUES (source.site_url, source.file_url, source.schema_map, 1);
            """, (site_url, file_url, schema_map))

        if removed:
            # Mark removed files as inactive instead of deleting
            cursor.execute(
                'UPDATE files SET is_active = 0 WHERE site_url = %s AND file_url IN ({})'.format(
                    ','.join('%s' * len(removed))
                ),
                tuple([site_url] + list(removed))
            )

        conn.commit()
        return (list(added), list(removed))

def get_file_ids(conn, file_url):
    """Get all IDs associated with a file"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM ids WHERE file_url = %s', (file_url,))
    return {row[0] for row in cursor.fetchall()}

def update_file_ids(conn, file_url, current_ids):
    """Update IDs for a file, returns (added_ids, removed_ids)"""
    cursor = conn.cursor()
    
    existing_ids = get_file_ids(conn, file_url)
    
    added = current_ids - existing_ids
    removed = existing_ids - current_ids
    
    if added:
        cursor.executemany(
            'INSERT INTO ids (file_url, id) VALUES (%s, %s)',
            [(file_url, id) for id in added]
        )

    if removed:
        # If removing all IDs (current_ids is empty), use simple DELETE
        # to avoid SQL Server's 2100 parameter limit
        if not current_ids:
            cursor.execute(
                'DELETE FROM ids WHERE file_url = %s',
                (file_url,)
            )
        else:
            # Batch deletions to avoid parameter limit (max 2100 params in SQL Server)
            # Use batches of 500 to be safe
            removed_list = list(removed)
            batch_size = 500
            for i in range(0, len(removed_list), batch_size):
                batch = removed_list[i:i + batch_size]
                cursor.execute(
                    'DELETE FROM ids WHERE file_url = %s AND id IN ({})'.format(
                        ','.join('%s' * len(batch))
                    ),
                    tuple([file_url] + batch)
                )

    cursor.execute(
        'UPDATE files SET last_read_time = GETUTCDATE(), number_of_items = %s WHERE file_url = %s',
        (len(current_ids), file_url)
    )
    
    conn.commit()
    return (list(added), list(removed))

def count_id_references(conn, id):
    """Count how many files reference an ID"""
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM ids WHERE id = %s', (id,))
    return cursor.fetchone()[0]

def clear_all_data(conn):
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

def get_all_sites(conn):
    """Get all sites with their status"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT site_url, process_interval_hours, last_processed, is_active, created_at
        FROM sites
        ORDER BY site_url
    """)
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

def add_site(conn, site_url, interval_hours=24):
    """Add a new site to monitor"""
    cursor = conn.cursor()

    # Check if site already exists
    cursor.execute("SELECT site_url FROM sites WHERE site_url = %s", (site_url,))
    if cursor.fetchone():
        # Update existing site
        cursor.execute("""
            UPDATE sites
            SET process_interval_hours = %s, is_active = 1
            WHERE site_url = %s
        """, (interval_hours, site_url))
        print(f"Site {site_url} already exists - updated settings")
    else:
        # Insert new site
        cursor.execute("""
            INSERT INTO sites (site_url, process_interval_hours)
            VALUES (%s, %s)
        """, (site_url, interval_hours))
        print(f"Site {site_url} added successfully")

    conn.commit()

def remove_site(conn, site_url):
    """Remove a site (hard delete from database)"""
    cursor = conn.cursor()

    # Delete in correct order due to foreign keys
    # First delete IDs associated with files from this site
    cursor.execute("""
        DELETE FROM ids
        WHERE file_url IN (SELECT file_url FROM files WHERE site_url = %s)
    """, (site_url,))

    # Then delete files from this site
    cursor.execute("""
        DELETE FROM files WHERE site_url = %s
    """, (site_url,))

    # Finally delete the site itself
    cursor.execute("""
        DELETE FROM sites WHERE site_url = %s
    """, (site_url,))

    conn.commit()

def add_manual_schema_file(conn, site_url, file_url, schema_map=None):
    """Add a manual schema file for a site"""
    cursor = conn.cursor()
    # Check if file exists first
    cursor.execute("SELECT file_url FROM files WHERE file_url = %s", (file_url,))
    if cursor.fetchone():
        # Update existing file
        cursor.execute("""
            UPDATE files SET is_active = 1, is_manual = 1, schema_map = %s
            WHERE file_url = %s
        """, (schema_map, file_url))
    else:
        # Insert new file
        cursor.execute("""
            INSERT INTO files (site_url, file_url, schema_map, is_manual, is_active)
            VALUES (%s, %s, %s, 1, 1)
        """, (site_url, file_url, schema_map))
    conn.commit()

def remove_schema_file(conn, file_url):
    """Remove a schema file (soft delete)"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE files SET is_active = 0 WHERE file_url = %s
    """, (file_url,))
    conn.commit()

def get_site_status(conn):
    """Get status information for all sites"""
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
        LEFT JOIN files f ON s.site_url = f.site_url AND f.is_active = 1
        LEFT JOIN ids i ON f.file_url = i.file_url
        GROUP BY s.site_url, s.is_active, s.last_processed
        ORDER BY s.site_url
    """)
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