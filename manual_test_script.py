#!/usr/bin/env python3
"""
Manual Test Script for Hindalco PDF Downloader
This script helps you:
1. Test the current system
2. Download historical PDFs
3. Verify CSV generation
4. Check data extraction
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests

# Import your main functions
try:
    from main import (
        create_directories, get_pdf_url, get_alternative_pdf_urls,
        download_pdf, extract_data_from_pdf, update_csv_files,
        check_pdf_validity, PRODUCTS, CSV_DIR, PDF_DIR
    )
except ImportError:
    print("Error: Could not import from main.py. Make sure main.py is in the same directory.")
    sys.exit(1)


def test_current_system():
    """Test the current system with today's date."""
    print("=" * 60)
    print("TESTING CURRENT SYSTEM")
    print("=" * 60)
    
    # Create directories
    create_directories()
    
    today = datetime.now()
    print(f"Testing for date: {today.strftime('%Y-%m-%d')}")
    
    # Test URL generation
    primary_url = get_pdf_url(today)
    alternative_urls = get_alternative_pdf_urls(today)
    
    print(f"\nPrimary URL: {primary_url}")
    print("\nAlternative URLs:")
    for i, url in enumerate(alternative_urls, 1):
        print(f"  {i}. {url}")
    
    # Test URL accessibility
    print("\n" + "-" * 40)
    print("TESTING URL ACCESSIBILITY")
    print("-" * 40)
    
    all_urls = [primary_url] + alternative_urls
    working_url = None
    
    for i, url in enumerate(all_urls):
        print(f"Testing URL {i+1}: ", end="")
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                print("✅ ACCESSIBLE")
                working_url = url
                break
            else:
                print(f"❌ Status: {response.status_code}")
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}...")
    
    if working_url:
        print(f"\n✅ Found working URL: {working_url}")
        
        # Test PDF download
        file_name = f"test-{today.day:02d}-{today.strftime('%b').lower()}-{today.year}.pdf"
        save_path = PDF_DIR / file_name
        
        print(f"\nDownloading to: {save_path}")
        if download_pdf(working_url, save_path):
            print("✅ PDF downloaded successfully")
            
            # Test PDF extraction
            if check_pdf_validity(save_path):
                print("✅ PDF is valid")
                
                extracted_data = extract_data_from_pdf(save_path, today)
                if extracted_data and len(extracted_data) > 1:  # More than just date
                    print("✅ Data extraction successful")
                    print(f"Extracted data: {extracted_data}")
                    
                    # Test CSV update
                    update_csv_files(extracted_data)
                    print("✅ CSV files updated")
                else:
                    print("⚠️  Data extraction returned minimal data")
                    print(f"Extracted: {extracted_data}")
            else:
                print("❌ PDF validation failed")
        else:
            print("❌ PDF download failed")
    else:
        print("❌ No working URLs found for today")
    
    return working_url is not None


def generate_date_range(start_date, end_date):
    """Generate list of dates between start and end date."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def download_historical_data():
    """Download historical PDF data."""
    print("\n" + "=" * 60)
    print("HISTORICAL DATA DOWNLOAD")
    print("=" * 60)
    
    # Ask user for date range
    print("Enter date range for historical download:")
    
    try:
        start_str = input("Start date (YYYY-MM-DD) or press Enter for last 30 days: ").strip()
        if not start_str:
            start_date = datetime.now() - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
        
        end_str = input("End date (YYYY-MM-DD) or press Enter for today: ").strip()
        if not end_str:
            end_date = datetime.now()
        else:
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
            
    except ValueError:
        print("Invalid date format. Using last 7 days.")
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
    
    print(f"\nDownloading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    dates = generate_date_range(start_date, end_date)
    successful_downloads = 0
    total_dates = len(dates)
    
    for i, date in enumerate(dates, 1):
        print(f"\n[{i}/{total_dates}] Processing {date.strftime('%Y-%m-%d')}...")
        
        # Generate filename
        file_name = f"primary-ready-reckoner-{date.day:02d}-{date.strftime('%b').lower()}-{date.year}.pdf"
        save_path = PDF_DIR / file_name
        
        # Skip if already exists and is valid
        if save_path.exists() and check_pdf_validity(save_path):
            print(f"  ✅ File already exists and is valid")
            
            # Extract and update CSV
            extracted_data = extract_data_from_pdf(save_path, date)
            if extracted_data:
                update_csv_files(extracted_data)
            successful_downloads += 1
            continue
        
        # Try to download
        urls_to_try = [get_pdf_url(date)] + get_alternative_pdf_urls(date)
        downloaded = False
        
        for url in urls_to_try:
            if download_pdf(url, save_path):
                print(f"  ✅ Downloaded from: {url}")
                
                # Extract data and update CSV
                extracted_data = extract_data_from_pdf(save_path, date)
                if extracted_data:
                    update_csv_files(extracted_data)
                    print(f"  ✅ Updated CSV files")
                
                downloaded = True
                successful_downloads += 1
                break
        
        if not downloaded:
            print(f"  ❌ No PDF found for this date")
            
            # Still update CSV with previous rates
            empty_data = {'date': date.strftime('%Y-%m-%d')}
            update_csv_files(empty_data)
            print(f"  ⚠️  Updated CSV with previous rates")
    
    print(f"\n" + "=" * 60)
    print(f"DOWNLOAD SUMMARY")
    print(f"=" * 60)
    print(f"Total dates processed: {total_dates}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Success rate: {(successful_downloads/total_dates)*100:.1f}%")


def verify_csv_files():
    """Verify generated CSV files."""
    print("\n" + "=" * 60)
    print("CSV FILE VERIFICATION")
    print("=" * 60)
    
    for product in PRODUCTS:
        from main import PRODUCT_FILE_NAMES
        csv_file = CSV_DIR / f"{PRODUCT_FILE_NAMES[product]}.csv"
        
        print(f"\nProduct: {product}")
        print(f"File: {csv_file}")
        
        if csv_file.exists():
            try:
                df = pd.read_csv(csv_file)
                print(f"  ✅ Rows: {len(df)}")
                print(f"  ✅ Columns: {list(df.columns)}")
                
                if len(df) > 0:
                    print(f"  ✅ Date range: {df['date'].min()} to {df['date'].max()}")
                    print(f"  ✅ Rate range: {df['rate'].min()} to {df['rate'].max()}")
                    
                    # Show last few entries
                    print(f"  📊 Last 3 entries:")
                    for _, row in df.tail(3).iterrows():
                        print(f"      {row['date']}: ₹{row['rate']:,}")
                else:
                    print(f"  ⚠️  File is empty")
                    
            except Exception as e:
                print(f"  ❌ Error reading file: {e}")
        else:
            print(f"  ❌ File not found")


def check_system_requirements():
    """Check if all required dependencies are installed."""
    print("=" * 60)
    print("SYSTEM REQUIREMENTS CHECK")
    print("=" * 60)
    
    requirements = [
        ('requests', 'requests'),
        ('pandas', 'pandas'),
        ('PyPDF2', 'PyPDF2'),
        ('tabula-py', 'tabula'),
        ('pathlib', 'pathlib')
    ]
    
    all_good = True
    
    for package_name, import_name in requirements:
        try:
            __import__(import_name)
            print(f"  ✅ {package_name}")
        except ImportError:
            print(f"  ❌ {package_name} - NOT INSTALLED")
            all_good = False
    
    # Check Java for tabula
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✅ Java (required for tabula-py)")
        else:
            print(f"  ❌ Java - NOT FOUND")
            all_good = False
    except:
        print(f"  ❌ Java - NOT FOUND")
        all_good = False
    
    if all_good:
        print("\n✅ All requirements satisfied!")
    else:
        print("\n❌ Some requirements missing. Install with:")
        print("pip install requests pandas PyPDF2 tabula-py")
        print("Also install Java for tabula-py to work properly.")
    
    return all_good


def main():
    """Main test function."""
    print("🔍 HINDALCO PDF DOWNLOADER - MANUAL TEST SCRIPT")
    print("=" * 60)
    
    # Check requirements first
    if not check_system_requirements():
        print("\n❌ Please install missing requirements before proceeding.")
        return
    
    while True:
        print("\n" + "=" * 60)
        print("SELECT OPTION:")
        print("=" * 60)
        print("1. Test current system (today's date)")
        print("2. Download historical data")
        print("3. Verify CSV files")
        print("4. Check system requirements")
        print("5. Run full test (all above)")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            test_current_system()
        
        elif choice == '2':
            download_historical_data()
        
        elif choice == '3':
            verify_csv_files()
        
        elif choice == '4':
            check_system_requirements()
        
        elif choice == '5':
            print("\n🚀 RUNNING FULL TEST SUITE")
            check_system_requirements()
            test_current_system()
            download_historical_data()
            verify_csv_files()
            print("\n✅ Full test completed!")
        
        elif choice == '6':
            print("\n👋 Goodbye!")
            break
        
        else:
            print("\n❌ Invalid choice. Please enter 1-6.")


if __name__ == "__main__":
    main()
