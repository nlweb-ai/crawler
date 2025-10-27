#!/usr/bin/env python3
"""
Clean all data from tables and update schema to add authentication support.
WARNING: This will delete all existing data!
"""

import sys
import os

# Add code/core to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code', 'core'))

import db

def clean_and_migrate_database():
    """Clean all data and run database migration"""
    print("=" * 80)
    print("DATABASE CLEAN AND MIGRATION")
    print("WARNING: This will delete all existing data!")
    print("=" * 80)

    print("\nConnecting to database...")
    try:
        conn = db.get_connection()
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return False

    try:
        cursor = conn.cursor()

        print("\nStep 1: Dropping foreign key constraints...")
        print("-" * 80)

        # Drop foreign key constraints first
        try:
            cursor.execute("""
                IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_sites_users')
                    ALTER TABLE sites DROP CONSTRAINT FK_sites_users
            """)
            cursor.execute("""
                IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_files_users')
                    ALTER TABLE files DROP CONSTRAINT FK_files_users
            """)
            cursor.execute("""
                IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_ids_users')
                    ALTER TABLE ids DROP CONSTRAINT FK_ids_users
            """)
            conn.commit()
            print("✓ Foreign key constraints dropped")
        except Exception as e:
            print(f"Note: Error dropping FKs (may not exist): {e}")

        print("\nStep 2: Deleting all data from tables...")
        print("-" * 80)

        # Delete data in correct order (respecting foreign keys)
        tables_to_clean = ['ids', 'files', 'sites', 'users']
        for table in tables_to_clean:
            try:
                cursor.execute(f"DELETE FROM {table}")
                rows_affected = cursor.rowcount
                conn.commit()
                print(f"✓ Deleted {rows_affected} rows from {table}")
            except Exception as e:
                print(f"Note: Could not delete from {table}: {e}")
                conn.rollback()

        print("\nStep 3: Dropping and recreating tables with new schema...")
        print("-" * 80)

        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS ids")
        cursor.execute("DROP TABLE IF EXISTS files")
        cursor.execute("DROP TABLE IF EXISTS sites")
        cursor.execute("DROP TABLE IF EXISTS users")
        conn.commit()
        print("✓ Old tables dropped")

        # Create tables with new schema
        db.create_tables(conn)
        print("✓ New tables created with authentication support")

        print("\n" + "=" * 80)
        print("✓ Database cleaned and migrated successfully!")
        print("=" * 80)
        print("\nNew schema:")
        print("  - users table (user_id, email, name, provider, api_key, created_at, last_login)")
        print("  - sites table with user_id column (FK to users)")
        print("  - files table with user_id column (FK to users)")
        print("  - ids table with user_id column (FK to users)")
        print("\nAll tables are now empty and ready for use.")

        return True

    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == '__main__':
    success = clean_and_migrate_database()
    sys.exit(0 if success else 1)
