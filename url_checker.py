#!/usr/bin/env python3
"""
Quick URL checker to see what Hindalco PDFs are available online
"""

import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def check_url(url):
    """Check if a URL is accessible."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.head(url, headers=headers, timeout=10)
        return url, response.status_code, response.headers.get('content-length', 'Unknown')
    except Exception as e:
        return url, None, str(e)


def generate_urls_for_date(date):
    """Generate all possible URL formats for a given date."""
    day = date.day
    month_num = date.strftime('%m')  # '05'
    month_short = date.strftime('%b').lower()  # 'may'
    month_full = date.strftime('%B').lower()  # 'may'
    year = date.year
    
    # Get ordinal suffix
    if 10 <= day % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    
    urls = [
        # Most common format
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        
        # With ordinal suffix
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day}{suffix}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day}{suffix}-{month_short}-{year}.pdf",
        
        # Full month name
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month_full}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_full}-{year}.pdf",
        
        # Numeric month
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month_num}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_num}-{year}.pdf",
    ]
    
    return urls


def check_recent_availability():
    """Check availability of PDFs for recent dates."""
    print("=" * 80)
    print("CHECKING RECENT PDF AVAILABILITY")
    print("=" * 80)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    available_files = []
    all_urls = []
    
    # Generate all URLs to check
    current_date = start_date
    while current_date <= end_date:
        urls = generate_urls_for_date(current_date)
        for url in urls:
            all_urls.append((current_date, url))
        current_date += timedelta(days=1)
    
    print(f"Checking {len(all_urls)} URLs for dates {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("\nChecking URLs (this may take a few minutes)...")
    
    # Check URLs in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {executor.submit(check_url, url): (date, url) for date, url in all_urls}
        
        completed = 0
        for future in as_completed(future_to_date):
            date, original_url = future_to_date[future]
            url, status_code, content_length = future.result()
            completed += 1
            
            if completed % 50 == 0:
                print(f"  Checked {completed}/{len(all_urls)} URLs...")
            
            if status_code == 200:
                available_files.append({
                    'date': date,
                    'url': url,
                    'status': status_code,
                    'size': content_length
                })
                print(f"✅ FOUND: {date.strftime('%Y-%m-%d')} - {url}")
    
    print(f"\n" + "=" * 80)
    print(f"RESULTS SUMMARY")
    print(f"=" * 80)
    print(f"Total URLs checked: {len(all_urls)}")
    print(f"Available PDFs found: {len(available_files)}")
    print(f"Success rate: {(len(available_files)/31)*100:.1f}% (31 days checked)")
    
    if available_files:
        print(f"\n📋 AVAILABLE FILES:")
        print("-" * 80)
        available_files.sort(key=lambda x: x['date'])
        
        for file_info in available_files:
            size_str = f"({file_info['size']} bytes)" if file_info['size'] != 'Unknown' else ""
            print(f"{file_info['date'].strftime('%Y-%m-%d')}: {file_info['url']} {size_str}")
        
        # Save to file
        with open('available_pdfs.txt', 'w') as f:
            f.write(f"Available Hindalco PDFs (checked on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
            f.write("=" * 80 + "\n\n")
            for file_info in available_files:
                f.write(f"{file_info['date'].strftime('%Y-%m-%d')}: {file_info['url']}\n")
        
        print(f"\n💾 Results saved to 'available_pdfs.txt'")
    else:
        print(f"\n❌ No PDFs found in the last 30 days")
        print("This might indicate:")
        print("  - URL format has changed")
        print("  - Website structure has changed")
        print("  - PDFs are published with different naming convention")
    
    return available_files


def check_specific_date():
    """Check URLs for a specific date."""
    print("\n" + "=" * 60)
    print("CHECK SPECIFIC DATE")
    print("=" * 60)
    
    date_str = input("Enter date (YYYY-MM-DD) or press Enter for today: ").strip()
    
    if not date_str:
        check_date = datetime.now()
    else:
        try:
            check_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            print("Invalid date format. Using today.")
            check_date = datetime.now()
    
    print(f"\nChecking URLs for {check_date.strftime('%Y-%m-%d')}:")
    print("-" * 60)
    
    urls = generate_urls_for_date(check_date)
    
    for i, url in enumerate(urls, 1):
        print(f"{i:2d}. Checking: {url}")
        url_result, status_code, content_length = check_url(url)
        
        if status_code == 200:
            size_info = f" (Size: {content_length})" if content_length != 'Unknown' else ""
            print(f"    ✅ AVAILABLE{size_info}")
        elif status_code:
            print(f"    ❌ Status: {status_code}")
        else:
            print(f"    ❌ Error: {content_length}")
        
        time.sleep(0.5)  # Be nice to the server


def main():
    """Main function."""
    print("🔍 HINDALCO PDF URL AVAILABILITY CHECKER")
    
    while True:
        print("\n" + "=" * 60)
        print("SELECT OPTION:")
        print("=" * 60)
        print("1. Check recent availability (last 30 days)")
        print("2. Check specific date")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == '1':
            available_files = check_recent_availability()
            
            if available_files:
                download_choice = input(f"\nFound {len(available_files)} files. Download them? (y/n): ").strip().lower()
                if download_choice == 'y':
                    print("\nTo download these files, run:")
                    print("python test_download.py")
                    print("Then select option 2 (Download historical data)")
        
        elif choice == '2':
            check_specific_date()
        
        elif choice == '3':
            print("\n👋 Goodbye!")
            break
        
        else:
            print("\n❌ Invalid choice. Please enter 1-3.")


if __name__ == "__main__":
    main()
