#!/usr/bin/env python3
"""
Hindalco PDF Downloader - Improved Version with GitHub Actions Support
Downloads daily price ready reckoner PDFs and extracts data to CSV files.
"""

import os
import sys
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Try importing PDF processing libraries
try:
    import PyPDF2
    import tabula
    PDF_PROCESSING_AVAILABLE = True
except ImportError:
    PDF_PROCESSING_AVAILABLE = False
    print("Warning: PDF processing libraries not available. Install with: pip install PyPDF2 tabula-py")

# Configuration
PDF_DIR = Path("pdfs")
CSV_DIR = Path("csv_data")
LOG_DIR = Path("logs")

# Product definitions
PRODUCTS = [
    "COPPER_BRIGHT_BARS",
    "COPPER_RODS", 
    "COPPER_FLATS",
    "COPPER_ANGLES",
    "COPPER_SHEETS",
    "BRASS_RODS",
    "BRASS_FLATS", 
    "BRASS_SHEETS",
    "ALUMINIUM_SHEETS",
    "ALUMINIUM_CIRCLES"
]

PRODUCT_FILE_NAMES = {
    "COPPER_BRIGHT_BARS": "copper_bright_bars",
    "COPPER_RODS": "copper_rods",
    "COPPER_FLATS": "copper_flats", 
    "COPPER_ANGLES": "copper_angles",
    "COPPER_SHEETS": "copper_sheets",
    "BRASS_RODS": "brass_rods",
    "BRASS_FLATS": "brass_flats",
    "BRASS_SHEETS": "brass_sheets", 
    "ALUMINIUM_SHEETS": "aluminium_sheets",
    "ALUMINIUM_CIRCLES": "aluminium_circles"
}

# Setup logging
def setup_logging():
    """Setup logging configuration."""
    LOG_DIR.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_DIR / f'hindalco_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def create_directories():
    """Create necessary directories."""
    PDF_DIR.mkdir(exist_ok=True)
    CSV_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

def get_pdf_url(date):
    """Generate primary PDF URL for a given date."""
    day = date.day
    month = date.strftime('%b').lower()  # 'jan', 'feb', etc.
    year = date.year
    
    return f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month}-{year}.pdf"

def get_alternative_pdf_urls(date):
    """Generate alternative PDF URLs for a given date."""
    day = date.day
    month_short = date.strftime('%b').lower()
    month_full = date.strftime('%B').lower()
    month_num = date.strftime('%m')
    year = date.year
    
    # Get ordinal suffix
    if 10 <= day % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    
    alternatives = [
        # Different cases and paths
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/Upload/Pdf/primary-ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        
        # With ordinal suffix
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day}{suffix}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day}{suffix}-{month_short}-{year}.pdf",
        
        # Without zero padding
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day}-{month_short}-{year}.pdf",
        
        # Full month name
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month_full}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_full}-{year}.pdf",
        
        # Numeric month
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month_num}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_num}-{year}.pdf",
        
        # Different separators
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}_{month_short}_{year}.pdf",
        f"https://www.hindalco.com/Upload/PDF/primary_ready_reckoner_{day:02d}_{month_short}_{year}.pdf",
        
        # Alternative naming patterns
        f"https://www.hindalco.com/Upload/PDF/ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/Upload/PDF/primary-reckoner-{day:02d}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/Upload/PDF/primary-rates-{day:02d}-{month_short}-{year}.pdf",
    ]
    
    return alternatives

def download_pdf(url, save_path, timeout=30):
    """Download PDF from URL with error handling."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf,application/octet-stream,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        response = requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True)
        response.raise_for_status()
        
        # Check if response is actually a PDF
        content_type = response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
            # Check the first few bytes for PDF signature
            first_chunk = next(response.iter_content(chunk_size=1024), b'')
            if not first_chunk.startswith(b'%PDF'):
                return False
        
        # Save file
        with open(save_path, 'wb') as f:
            f.write(first_chunk if 'first_chunk' in locals() else b'')
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Verify file was saved and has content
        if save_path.exists() and save_path.stat().st_size > 1000:  # At least 1KB
            return True
        else:
            save_path.unlink(missing_ok=True)  # Remove empty file
            return False
            
    except Exception as e:
        logging.warning(f"Error downloading {url}: {e}")
        if save_path.exists():
            save_path.unlink(missing_ok=True)
        return False

def check_pdf_validity(pdf_path):
    """Check if PDF file is valid and readable."""
    if not PDF_PROCESSING_AVAILABLE:
        # Basic check - file exists and has reasonable size
        return pdf_path.exists() and pdf_path.stat().st_size > 1000
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            return len(pdf_reader.pages) > 0
    except Exception:
        return False

def extract_data_from_pdf(pdf_path, date):
    """Extract pricing data from PDF."""
    if not PDF_PROCESSING_AVAILABLE:
        logging.warning("PDF processing not available - skipping data extraction")
        return {'date': date.strftime('%Y-%m-%d')}
    
    try:
        # Try tabula first (for table extraction)
        tables = tabula.read_pdf(str(pdf_path), pages='all', multiple_tables=True)
        
        extracted_data = {'date': date.strftime('%Y-%m-%d')}
        
        # Process tables to extract pricing information
        for table in tables:
            if isinstance(table, pd.DataFrame) and not table.empty:
                # Look for product names and prices
                for product in PRODUCTS:
                    # Simplified extraction - look for product keywords
                    product_keywords = product.lower().replace('_', ' ').split()
                    
                    for _, row in table.iterrows():
                        row_text = ' '.join(str(cell).lower() for cell in row if pd.notna(cell))
                        
                        # Check if row contains product keywords
                        if any(keyword in row_text for keyword in product_keywords):
                            # Extract numeric values (prices)
                            import re
                            numbers = re.findall(r'\d+\.?\d*', row_text)
                            if numbers:
                                # Take the largest number as price
                                price = max(float(n) for n in numbers if float(n) > 10)
                                extracted_data[product] = price
                                break
        
        return extracted_data if len(extracted_data) > 1 else {'date': date.strftime('%Y-%m-%d')}
        
    except Exception as e:
        logging.warning(f"Error extracting data from {pdf_path}: {e}")
        return {'date': date.strftime('%Y-%m-%d')}

def update_csv_files(data):
    """Update CSV files with new data."""
    date_str = data.get('date')
    if not date_str:
        return
    
    for product in PRODUCTS:
        csv_file = CSV_DIR / f"{PRODUCT_FILE_NAMES[product]}.csv"
        
        # Load existing data or create new DataFrame
        if csv_file.exists():
            try:
                df = pd.read_csv(csv_file)
            except Exception:
                df = pd.DataFrame(columns=['date', 'rate'])
        else:
            df = pd.DataFrame(columns=['date', 'rate'])
        
        # Check if date already exists
        if date_str in df['date'].values:
            # Update existing record
            if product in data:
                df.loc[df['date'] == date_str, 'rate'] = data[product]
        else:
            # Add new record
            new_rate = data.get(product)
            if new_rate is None and len(df) > 0:
                # Use previous rate if no new rate found
                new_rate = df.iloc[-1]['rate']
            elif new_rate is None:
                new_rate = 0  # Default rate
            
            new_row = pd.DataFrame({'date': [date_str], 'rate': [new_rate]})
            df = pd.concat([df, new_row], ignore_index=True)
        
        # Sort by date and save
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        df.to_csv(csv_file, index=False)

def process_date(date, logger):
    """Process a single date - download PDF and extract data."""
    logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}")
    
    # Generate filename
    file_name = f"primary-ready-reckoner-{date.day:02d}-{date.strftime('%b').lower()}-{date.year}.pdf"
    save_path = PDF_DIR / file_name
    
    # Skip if file already exists and is valid
    if save_path.exists() and check_pdf_validity(save_path):
        logger.info(f"PDF already exists and is valid: {save_path}")
        extracted_data = extract_data_from_pdf(save_path, date)
        update_csv_files(extracted_data)
        return True
    
    # Try to download PDF
    urls_to_try = [get_pdf_url(date)] + get_alternative_pdf_urls(date)
    
    for i, url in enumerate(urls_to_try):
        logger.info(f"Trying URL {i+1}/{len(urls_to_try)}: {url}")
        if download_pdf(url, save_path):
            logger.info(f"Successfully downloaded: {url}")
            
            # Extract data and update CSV
            extracted_data = extract_data_from_pdf(save_path, date)
            update_csv_files(extracted_data)
            return True
        else:
            logger.debug(f"Failed to download from: {url}")
        
        # Small delay between attempts
        time.sleep(0.5)
    
    logger.warning(f"No PDF found for date: {date.strftime('%Y-%m-%d')}")
    
    # Still update CSV with previous rates
    empty_data = {'date': date.strftime('%Y-%m-%d')}
    update_csv_files(empty_data)
    return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Hindalco PDF Downloader')
    parser.add_argument('--historical-days', type=int, default=0,
                       help='Number of historical days to download (0 for today only)')
    parser.add_argument('--start-date', type=str,
                       help='Start date for historical download (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, 
                       help='End date for historical download (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Setup logging and directories
    logger = setup_logging()
    create_directories()
    
    logger.info("Starting Hindalco PDF Downloader")
    
    # Determine date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    elif args.historical_days > 0:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.historical_days)
    else:
        # Today only
        start_date = end_date = datetime.now()
    
    logger.info(f"Processing dates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Generate list of dates to process (skip weekends for business data)
    dates = []
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += timedelta(days=1)
    
    successful_downloads = 0
    
    # Process dates
    for date in dates:
        if process_date(date, logger):
            successful_downloads += 1
        
        # Small delay to be respectful to the server
        time.sleep(2)
    
    # Summary
    total_dates = len(dates)
    success_rate = (successful_downloads / total_dates) * 100 if total_dates > 0 else 0
    
    logger.info(f"Processing complete!")
    logger.info(f"Total dates processed: {total_dates}")
    logger.info(f"Successful downloads: {successful_downloads}")
    logger.info(f"Success rate: {success_rate:.1f}%")
    
    # Generate summary for GitHub Actions
    if os.getenv('GITHUB_ACTIONS'):
        print(f"::notice::Processed {total_dates} dates with {success_rate:.1f}% success rate")
    
    return 0 if success_rate > 0 or total_dates == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
