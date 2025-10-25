#!/usr/bin/env python3
"""
Monitor crawler progress via API endpoints
"""

import requests
import time
import sys
from datetime import datetime

API_BASE = "http://localhost:5001/api"  # Changed to 5001 to avoid macOS AirPlay conflict

def get_queue_status():
    """Get current queue status"""
    try:
        response = requests.get(f"{API_BASE}/queue/status", timeout=2)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def get_site_status():
    """Get current site status"""
    try:
        response = requests.get(f"{API_BASE}/status", timeout=2)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

def clear_screen():
    """Clear the terminal screen"""
    print("\033[2J\033[H")

def format_time(iso_string):
    """Format ISO time string to readable format"""
    if not iso_string:
        return "Never"
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return iso_string

def display_status(refresh_rate=5):
    """Display live status updates"""
    print("Starting crawler monitor... Press Ctrl+C to exit\n")

    try:
        while True:
            clear_screen()

            print("=" * 70)
            print(f"CRAWLER MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)

            # Get queue status
            queue_status = get_queue_status()
            if queue_status:
                print("\n📊 QUEUE STATUS")
                print("-" * 40)
                print(f"  Pending:     {queue_status['pending_jobs']:3d} jobs")
                print(f"  Processing:  {queue_status['processing_jobs']:3d} jobs")
                print(f"  Failed:      {queue_status['failed_jobs']:3d} jobs")
                print(f"  Total:       {queue_status['total_jobs']:3d} jobs")

                if queue_status['jobs']:
                    print("\n  Recent Jobs:")
                    for job in queue_status['jobs'][:5]:
                        status_emoji = {
                            'pending': '⏳',
                            'processing': '🔄',
                            'failed': '❌'
                        }.get(job['status'], '❓')

                        print(f"    {status_emoji} [{job['status']:10s}] {job['type']:20s}")
                        if job.get('file_url'):
                            print(f"       File: {job['file_url'][:50]}...")
            else:
                print("\n⚠️  Cannot connect to API server")

            # Get site status
            site_status = get_site_status()
            if site_status:
                print("\n🌐 SITE STATUS")
                print("-" * 40)

                total_files = sum(s['total_files'] for s in site_status)
                total_ids = sum(s['total_ids'] for s in site_status)
                active_sites = sum(1 for s in site_status if s['is_active'])

                print(f"  Active Sites: {active_sites}")
                print(f"  Total Files:  {total_files}")
                print(f"  Total IDs:    {total_ids}")

                if site_status:
                    print("\n  Sites:")
                    for site in site_status[:5]:
                        status = "✅" if site['is_active'] else "⏸️"
                        print(f"    {status} {site['site_url']}")
                        print(f"       Files: {site['total_files']:3d} | IDs: {site['total_ids']:5d} | "
                              f"Last: {format_time(site['last_processed'])[:19]}")

            print("\n" + "-" * 70)
            print(f"Refreshing every {refresh_rate} seconds... Press Ctrl+C to exit")

            time.sleep(refresh_rate)

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
        sys.exit(0)

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Monitor crawler progress')
    parser.add_argument('-r', '--refresh', type=int, default=5,
                        help='Refresh rate in seconds (default: 5)')
    parser.add_argument('--once', action='store_true',
                        help='Show status once and exit')

    args = parser.parse_args()

    if args.once:
        # Just show status once
        queue_status = get_queue_status()
        site_status = get_site_status()

        if queue_status:
            print("Queue Status:")
            print(f"  Pending: {queue_status['pending_jobs']}")
            print(f"  Processing: {queue_status['processing_jobs']}")
            print(f"  Failed: {queue_status['failed_jobs']}")

        if site_status:
            print("\nSite Status:")
            for site in site_status:
                print(f"  {site['site_url']}: {site['total_files']} files, {site['total_ids']} IDs")
    else:
        # Live monitoring
        display_status(args.refresh)

if __name__ == "__main__":
    main()