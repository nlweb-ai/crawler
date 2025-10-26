#!/usr/bin/env python3
"""
Create a test user with API key for testing authentication system.
"""

import sys
import os

# Add code/core to path to import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code', 'core'))

import db
import secrets

def create_test_user():
    """Create a test user with a known API key"""

    # Test user details
    user_id = "test:user001"
    email = "test@example.com"
    name = "Test User"
    provider = "test"

    # Generate a readable API key for testing
    api_key = secrets.token_urlsafe(48)

    print("=" * 80)
    print("Creating Test User")
    print("=" * 80)

    # Connect to database
    try:
        conn = db.get_connection()
        print("✓ Connected to database")

        # Ensure tables exist
        print("✓ Creating/updating database tables...")
        db.create_tables(conn)
        print("✓ Database tables ready")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        return

    try:
        cursor = conn.cursor()

        # Check if user already exists
        cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
        existing = cursor.fetchone()

        if existing:
            print(f"\n✓ Test user already exists: {user_id}")
            print("  Updating API key...")

            cursor.execute("""
                UPDATE users
                SET api_key = %s, last_login = GETUTCDATE()
                WHERE user_id = %s
            """, (api_key, user_id))
            conn.commit()
            print("  ✓ API key updated")
        else:
            print(f"\n✓ Creating new test user: {user_id}")

            cursor.execute("""
                INSERT INTO users (user_id, email, name, provider, api_key, created_at, last_login)
                VALUES (%s, %s, %s, %s, %s, GETUTCDATE(), GETUTCDATE())
            """, (user_id, email, name, provider, api_key))
            conn.commit()
            print("  ✓ Test user created")

        print("\n" + "=" * 80)
        print("TEST USER CREDENTIALS")
        print("=" * 80)
        print(f"User ID:  {user_id}")
        print(f"Email:    {email}")
        print(f"Name:     {name}")
        print(f"Provider: {provider}")
        print(f"\nAPI Key:\n{api_key}")
        print("=" * 80)

        print("\n" + "=" * 80)
        print("TESTING COMMANDS")
        print("=" * 80)
        print("\n1. Test API key authentication (list sites):")
        print(f"   curl -H 'X-API-Key: {api_key}' http://localhost:5001/api/sites")

        print("\n2. Add a test site:")
        print(f"""   curl -X POST http://localhost:5001/api/sites \\
     -H 'X-API-Key: {api_key}' \\
     -H 'Content-Type: application/json' \\
     -d '{{"site_url": "https://www.hebbarskitchen.com", "interval_hours": 24}}'""")

        print("\n3. Get current user info:")
        print(f"   curl -H 'X-API-Key: {api_key}' http://localhost:5001/api/me")

        print("\n4. Get status:")
        print(f"   curl -H 'X-API-Key: {api_key}' http://localhost:5001/api/status")

        print("\n5. Add schema map to site:")
        print(f"""   curl -X POST http://localhost:5001/api/sites/https://www.hebbarskitchen.com/schema-files \\
     -H 'X-API-Key: {api_key}' \\
     -H 'Content-Type: application/json' \\
     -d '{{"schema_map_url": "https://www.hebbarskitchen.com/schema_map.xml"}}'""")

        print("\n" + "=" * 80)
        print("SAVE THIS API KEY!")
        print("=" * 80)
        print(f"\nexport TEST_API_KEY='{api_key}'")
        print("\nThen you can use: $TEST_API_KEY in your curl commands")
        print("=" * 80 + "\n")

        # Write to a file for easy access
        with open('.test_api_key', 'w') as f:
            f.write(api_key)
        print("✓ API key saved to .test_api_key file")
        print("  You can load it with: export TEST_API_KEY=$(cat .test_api_key)")

    except Exception as e:
        print(f"\n✗ Error creating test user: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    create_test_user()
