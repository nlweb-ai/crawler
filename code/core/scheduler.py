import os
import time
from datetime import datetime, timedelta
import db
import master

def get_sites_to_process():
    """Get sites that need processing based on their interval"""
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Get sites where last_processed + interval_hours < now
    cursor.execute("""
        SELECT site_url, process_interval_hours 
        FROM sites 
        WHERE DATEADD(hour, process_interval_hours, last_processed) <= GETUTCDATE() 
           OR last_processed IS NULL
    """)
    return cursor.fetchall()

def update_site_last_processed(site_url):
    """Update the last_processed time for a site"""
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE sites SET last_processed = GETUTCDATE() WHERE site_url = %s",
        (site_url,)
    )
    conn.commit()
    conn.close()

def scheduler_loop():
    """Main scheduler loop that processes sites at their intervals"""
    while True:
        try:
            # Get sites that need processing
            sites = get_sites_to_process()
            
            for site_url, interval_hours in sites:
                print(f"Processing site: {site_url}")
                try:
                    # Process the site using existing master functionality
                    master.process_site(site_url)
                    
                    # Update last processed time
                    update_site_last_processed(site_url)
                    
                except Exception as e:
                    print(f"Error processing site {site_url}: {e}")
                    continue
            
            # Sleep for a while before next check
            # In production this would be configured based on needs
            time.sleep(300)  # 5 minutes
            
        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(60)  # Wait a minute on error

if __name__ == '__main__':
    scheduler_loop()