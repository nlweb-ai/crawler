#!/usr/bin/env python3
"""
Migrate database schema to add authentication tables and user_id columns.
Run this once to update the existing database.
"""

import sys
import os

# Add code/core to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code', 'core'))

import db

def migrate_database():
    """Run database migration"""
    print("=" * 80)
    print("DATABASE MIGRATION - Adding Authentication Support")
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

        print("\nRunning migration...")
        print("-" * 80)

        # Call the create_tables function which handles everything
        db.create_tables(conn)

        print("✓ Migration completed successfully!")
        print("\nNew tables and columns:")
        print("  - users table (user_id, email, name, provider, api_key, created_at, last_login)")
        print("  - sites.user_id column (with foreign key to users)")
        print("  - files.user_id column (with foreign key to users)")
        print("  - ids.user_id column (with foreign key to users)")

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
    success = migrate_database()
    sys.exit(0 if success else 1)
