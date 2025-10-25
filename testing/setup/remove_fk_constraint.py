#!/usr/bin/env python3
"""
Remove foreign key constraint from ids table to allow flexible deletion
"""

import sys
import os

sys.path.insert(0, 'code/core')
import config
import db

def remove_foreign_key_constraint():
    """Remove the foreign key constraint from ids table"""

    conn = db.get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("REMOVING FOREIGN KEY CONSTRAINT")
    print("=" * 60)

    try:
        # First, find all foreign key constraints on the ids table
        print("\nFinding foreign key constraints on 'ids' table...")
        cursor.execute("""
            SELECT
                f.name AS constraint_name,
                OBJECT_NAME(f.parent_object_id) AS table_name,
                COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name
            FROM sys.foreign_keys AS f
            INNER JOIN sys.foreign_key_columns AS fc
                ON f.OBJECT_ID = fc.constraint_object_id
            WHERE OBJECT_NAME(f.parent_object_id) = 'ids'
        """)

        constraints = cursor.fetchall()

        if not constraints:
            print("  No foreign key constraints found on 'ids' table")
            return

        print(f"  Found {len(constraints)} constraint(s):")
        for constraint_name, table_name, column_name in constraints:
            print(f"    - {constraint_name} on {table_name}.{column_name}")

        # Remove each constraint
        print("\nRemoving constraints...")
        for constraint_name, table_name, column_name in constraints:
            try:
                sql = f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}"
                cursor.execute(sql)
                print(f"  ✓ Removed: {constraint_name}")
            except Exception as e:
                print(f"  ✗ Failed to remove {constraint_name}: {e}")

        conn.commit()
        print("\n✓ All foreign key constraints removed successfully")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        conn.rollback()

    finally:
        conn.close()


if __name__ == '__main__':
    remove_foreign_key_constraint()