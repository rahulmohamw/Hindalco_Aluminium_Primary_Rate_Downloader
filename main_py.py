#!/usr/bin/env python3
"""
Hindalco PDF Downloader and CSV Updater
Downloads daily Hindalco primary ready reckoner PDFs and updates CSV files
"""

import os
import csv
import time
import logging
import requests
import pandas as pd
import PyPDF2
from datetime import datetime, timedelta
from pathlib import Path

# Try to import tabula, handle gracefully if not available
try:
    import tabula
    TABULA_AVAILABLE = True
except ImportError:
    TABULA_AVAILABLE = False
    print("Warning: tabula-py not available. Will use alternative extraction methods.")

# Configuration
BASE_DIR = Path(__file__).parent
CSV_DIR = BASE_DIR / "csv_data"
PDF_DIR = BASE_DIR / "pdf_downloads"

# Product configurations
PRODUCTS = [
    "P0406",
    "P0610", 
    "CG Grade",
    "EC Grade Wire Rods",
    "6201 Alloy Wire Rod",
    "Billets (AA6063) 7\", 8\" & 9\"",
    "Billets (AA6063) 5\", 6\""
]

PRODUCT_FILE_NAMES = {
    "P0406": "p0406",
    "P0610": "p0610",
    "CG Grade": "cg_grade",
    "EC Grade Wire Rods": "ec_grade_wire_rods",
    "6201 Alloy Wire Rod": "6201_alloy_wire_rod",
    "Billets (AA6063) 7\", 8\" & 9\"": "billets_aa6063_large",
    "Billets (AA6063) 5\", 6\"": "billets_aa6063_small"
}

PRODUCT_DESCRIPTIONS = {
    "P0406": "Aluminium Ingot P0406",
    "P0610": "Aluminium Ingot P0610",
    "CG Grade": "CG Grade Aluminium",
    "EC Grade Wire Rods": "EC Grade Wire Rods",
    "6201 Alloy Wire Rod": "6201 Alloy Wire Rod",
    "Billets (AA6063) 7\", 8\" & 9\"": "Billets (AA6063) 7\", 8\" & 9\"",
    "Billets (AA6063) 5\", 6\"": "Billets (AA6063) 5\", 6\""
}

# Fallback rates (updated based on recent market prices)
FALLBACK_RATES = {
    "P0406": 252500,
    "P0610": 251000,
    "CG Grade": 250500,
    "EC Grade Wire Rods": 259750,
    "6201 Alloy Wire Rod": 267500,
    "Billets (AA6063) 7\", 8\" & 9\"": 268600,
    "Billets (AA6063) 5\", 6\"": 270100
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hindalco_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_directories():
    """Create necessary directories if they don't exist."""
    CSV_DIR.mkdir(exist_ok=True)
    PDF_DIR.mkdir(exist_ok=True)
    logger.info(f"Created directories: {CSV_DIR}, {PDF_DIR}")


def get_pdf_url(date):
    """Generate PDF URL for a given date."""
    # Based on search results, URLs can have different formats
    # Try the most common format first
    day = date.day
    month = date.strftime('%b').lower()  # 'may', 'feb', etc.
    year = date.year
    
    # Primary format: primary-ready-reckoner-20-may-2025.pdf
    return f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day:02d}-{month}-{year}.pdf"


def get_alternative_pdf_urls(date):
    """Generate alternative PDF URL formats for a given date."""
    day = date.day
    month = date.strftime('%m')  # '05' for May
    year = date.year
    month_name = date.strftime('%B').lower()  # 'may'
    month_short = date.strftime('%b').lower()  # 'may'
    
    urls = [
        # Format 1: DD-MM-YYYY
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month}-{year}.pdf",
        # Format 2: DD-month-YYYY (full month name)
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_name}-{year}.pdf",
        # Format 3: Different case for Upload vs upload
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day:02d}-{month_short}-{year}.pdf",
        # Format 4: With ordinal suffix
        f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{day}{get_ordinal_suffix(day)}-{month_short}-{year}.pdf",
        f"https://www.hindalco.com/upload/pdf/primary-ready-reckoner-{day}{get_ordinal_suffix(day)}-{month_short}-{year}.pdf"
    ]
    
    return urls


def get_ordinal_suffix(day):
    """Get ordinal suffix for a day (st, nd, rd, th)."""
    if 10 <= day % 100 <= 20:
        return 'th'
    else:
        return {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')


def download_pdf(url, save_path):
    """Download PDF from URL with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to download from: {url} (attempt {attempt + 1})")
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and len(response.content) < 1000:
                logger.warning(f"Response doesn't appear to be a PDF. Content-Type: {content_type}")
                continue
            
            # Save the PDF
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            # Verify the saved file
            if os.path.exists(save_path) and os.path.getsize(save_path) > 1000:
                logger.info(f"Successfully downloaded PDF to {save_path}")
                return True
            else:
                logger.warning(f"Downloaded file seems too small or doesn't exist")
                
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retry
    
    return False


def check_pdf_validity(pdf_path):
    """Check if the PDF is valid and contains usable data."""
    try:
        if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) < 1000:
            return False
            
        with open(pdf_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            
            if len(pdf_reader.pages) == 0:
                logger.warning(f"PDF at {pdf_path} has no pages")
                return False
                
            # Extract some text to verify content
            first_page_text = pdf_reader.pages[0].extract_text()
            if not first_page_text or len(first_page_text) < 50:
                logger.warning(f"PDF at {pdf_path} appears to have insufficient text content")
                return False
                
            # Look for key indicators
            indicators = ['hindalco', 'aluminium', 'aluminum', 'price', 'rate', 'reckoner', 'primary']
            text_lower = first_page_text.lower()
            if not any(indicator in text_lower for indicator in indicators):
                logger.warning(f"PDF at {pdf_path} doesn't appear to be a Hindalco rate sheet")
                return False
                
            logger.info(f"PDF at {pdf_path} appears valid")
            return True
            
    except Exception as e:
        logger.error(f"Error validating PDF at {pdf_path}: {e}")
        return False


def extract_prices_from_text(text):
    """Extract prices using pattern matching from text."""
    import re
    
    product_rates = {}
    
    # Common patterns for price extraction
    patterns = [
        r'(\d+,?\d*\.?\d*)\s*(?:rs\.?|rupees?|inr)?(?:\s*per\s*(?:mt|tonne?))?',
        r'(?:rs\.?\s*)?(\d+,?\d*\.?\d*)',
        r'(\d{6,})',  # 6+ digit numbers (likely prices)
    ]
    
    # Split text into lines for better parsing
    lines = text.split('\n')
    
    for i, line in enumerate(lines):
        line_lower = line.lower()
        
        # Check each product
        for product in PRODUCTS:
            product_lower = product.lower()
            product_key_words = product_lower.split()
            
            # If line contains product keywords
            if any(keyword in line_lower for keyword in product_key_words):
                # Look for numbers in this line and nearby lines
                search_lines = lines[max(0, i-1):i+3]  # Current line + context
                
                for search_line in search_lines:
                    for pattern in patterns:
                        matches = re.findall(pattern, search_line, re.IGNORECASE)
                        for match in matches:
                            try:
                                # Clean up the number
                                clean_number = match.replace(',', '').replace(' ', '')
                                price = float(clean_number)
                                
                                # Reasonable price range check (100,000 to 500,000)
                                if 100000 <= price <= 500000:
                                    product_rates[product] = int(price)
                                    logger.info(f"Found price for {product}: {price}")
                                    break
                            except ValueError:
                                continue
                    
                    if product in product_rates:
                        break
    
    return product_rates


def extract_data_from_pdf(pdf_path, date):
    """Extract pricing data from the PDF file."""
    try:
        logger.info(f"Starting data extraction from {pdf_path}")
        
        if not check_pdf_validity(pdf_path):
            logger.warning(f"Invalid PDF at {pdf_path}")
            return None
        
        # Extract text using PyPDF2
        with open(pdf_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            pdf_text = ""
            
            for page_num in range(len(pdf_reader.pages)):
                page_text = pdf_reader.pages[page_num].extract_text()
                pdf_text += page_text
                logger.info(f"Extracted text from page {page_num + 1}, length: {len(page_text)}")
        
        logger.info(f"Total extracted text length: {len(pdf_text)}")
        
        # Try different extraction methods
        product_rates = {}
        
        # Method 1: Direct text pattern matching
        text_rates = extract_prices_from_text(pdf_text)
        product_rates.update(text_rates)
        
        # Method 2: Try tabula if available
        if TABULA_AVAILABLE and len(product_rates) < len(PRODUCTS):
            try:
                logger.info("Trying tabula extraction")
                dfs = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
                
                for df in dfs:
                    if df is not None and not df.empty:
                        # Convert DataFrame to text and search for prices
                        df_text = df.to_string()
                        tabula_rates = extract_prices_from_text(df_text)
                        
                        for product, rate in tabula_rates.items():
                            if product not in product_rates:
                                product_rates[product] = rate
                                
            except Exception as e:
                logger.warning(f"Tabula extraction failed: {e}")
        
        logger.info(f"Extracted rates: {product_rates}")
        
        if product_rates:
            product_rates['date'] = date.strftime('%Y-%m-%d')
            return product_rates
        else:
            logger.warning("No pricing data found")
            return {'date': date.strftime('%Y-%m-%d')}
            
    except Exception as e:
        logger.error(f"Error extracting data from PDF: {e}")
        return {'date': date.strftime('%Y-%m-%d')}


def get_previous_rate(product_file, date):
    """Get the most recent rate for a product before the given date."""
    try:
        if os.path.exists(product_file):
            df = pd.read_csv(product_file)
            if not df.empty:
                # Filter dates before current date
                past_rates = df[df['date'] < date].sort_values('date')
                if not past_rates.empty:
                    return past_rates['rate'].iloc[-1]
    except Exception as e:
        logger.error(f"Error getting previous rate: {e}")
    
    return None


def update_csv_files(product_rates):
    """Update CSV files with the latest pricing data."""
    date = product_rates.get('date', datetime.now().strftime('%Y-%m-%d'))
    logger.info(f"Updating CSV files for date: {date}")
    
    for product in PRODUCTS:
        product_file = CSV_DIR / f"{PRODUCT_FILE_NAMES[product]}.csv"
        
        # Prepare product data
        product_data = {
            'date': date,
            'description': PRODUCT_DESCRIPTIONS[product]
        }
        
        # Use extracted rate if available, otherwise use previous rate or fallback
        if product in product_rates:
            product_data['rate'] = product_rates[product]
            logger.info(f"Using extracted rate {product_data['rate']} for {product}")
        else:
            # Try to get previous rate
            prev_rate = get_previous_rate(product_file, date)
            if prev_rate:
                product_data['rate'] = prev_rate
                logger.info(f"Using previous rate {prev_rate} for {product}")
            else:
                product_data['rate'] = FALLBACK_RATES[product]
                logger.info(f"Using fallback rate {product_data['rate']} for {product}")
        
        # Update or create CSV
        try:
            if product_file.exists():
                df = pd.read_csv(product_file)
                
                # Update existing row or add new row
                if date in df['date'].values:
                    df.loc[df['date'] == date, 'rate'] = product_data['rate']
                    logger.info(f"Updated existing row for {product} on {date}")
                else:
                    new_row = pd.DataFrame([product_data])
                    df = pd.concat([df, new_row], ignore_index=True)
                    logger.info(f"Added new row for {product} on {date}")
            else:
                # Create new CSV
                df = pd.DataFrame([product_data])
                logger.info(f"Created new CSV for {product}")
            
            # Sort by date and save
            df = df.sort_values('date').reset_index(drop=True)
            df.to_csv(product_file, index=False)
            logger.info(f"Saved CSV for {product} with {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error updating CSV for {product}: {e}")


def main():
    """Main function to download PDF and update CSV files."""
    logger.info("Starting Hindalco PDF downloader")
    
    # Create directories
    create_directories()
    
    today = datetime.now()
    logger.info(f"Processing for date: {today.strftime('%Y-%m-%d')}")
    
    # Generate file name and path
    file_name = f"primary-ready-reckoner-{today.day:02d}-{today.strftime('%b').lower()}-{today.year}.pdf"
    save_path = PDF_DIR / file_name
    
    product_rates = None
    
    # Check if file already exists
    if save_path.exists() and check_pdf_validity(save_path):
        logger.info(f"Valid PDF already exists: {save_path}")
        product_rates = extract_data_from_pdf(save_path, today)
    else:
        # Try to download PDF
        urls_to_try = [get_pdf_url(today)] + get_alternative_pdf_urls(today)
        
        success = False
        for url in urls_to_try:
            if download_pdf(url, save_path):
                success = True
                break
        
        # If current day fails, try yesterday
        if not success:
            yesterday = today - timedelta(days=1)
            yesterday_file = f"primary-ready-reckoner-{yesterday.day:02d}-{yesterday.strftime('%b').lower()}-{yesterday.year}.pdf"
            yesterday_path = PDF_DIR / yesterday_file
            
            logger.info("Trying yesterday's PDF")
            yesterday_urls = [get_pdf_url(yesterday)] + get_alternative_pdf_urls(yesterday)
            
            for url in yesterday_urls:
                if download_pdf(url, yesterday_path):
                    save_path = yesterday_path
                    today = yesterday
                    success = True
                    break
        
        # Extract data if download was successful
        if success:
            time.sleep(2)  # Wait for file to be fully written
            product_rates = extract_data_from_pdf(save_path, today)
        else:
            logger.warning("Could not download PDF for current or previous day")
    
    # Update CSV files (even if no new data, use previous rates)
    if product_rates is None:
        product_rates = {'date': today.strftime('%Y-%m-%d')}
    
    update_csv_files(product_rates)
    logger.info("Process completed successfully")


if __name__ == "__main__":
    main()
