import os
import re
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import tabula
import PyPDF2
import csv
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Product categories to extract
PRODUCTS = [
    "1. P0406 (Si 0.04% max, Fe 0.06% max) 99.85% (min)",
    "2. P0610 (99.85% min) /P1020/ EC Grade Ingot & Sow 99.7% (min) / Cast Bar",
    "3. CG Grade Ingot & Sow 99.5% (min) purity",
    "4. EC Grade Wire Rods, Dia 9.5 mm - Conductivity 61% min",
    "5. 6201 Alloy Wire Rod - Dia 9.5 mm (HAC-1)",
    "6. Billets (AA6063) Dia 7\", 8\" & 9\" - subject to availability",
    "7. Billets (AA6063) Dia 5\" , 6\" - subject to availability"
]

# Base directory for storage
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.join(BASE_DIR, 'downloads')
CSV_DIR = os.path.join(BASE_DIR, 'csv')


def create_directory_structure(date):
    """Create the year/month directory structure."""
    year_dir = os.path.join(DOWNLOADS_DIR, str(date.year))
    month_dir = os.path.join(year_dir, date.strftime('%m-%B'))
    
    os.makedirs(year_dir, exist_ok=True)
    os.makedirs(month_dir, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)
    
    return month_dir


def get_pdf_url(date):
    """Generate the URL for the specified date."""
    url = f"https://www.hindalco.com/Upload/PDF/primary-ready-reckoner-{date.day:02d}-{date.strftime('%b').lower()}-{date.year}.pdf"
    return url


def download_pdf(url, save_path):
    """Download the PDF file from the URL."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded PDF successfully: {save_path}")
        return True
    
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to download PDF: {e}")
        return False


def extract_data_from_pdf(pdf_path, date):
    """Extract pricing data from the PDF file."""
    try:
        # Use tabula to extract tables from PDF
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
        
        # Read PDF text for additional verification
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        pdf_text = ""
        for page in range(len(pdf_reader.pages)):
            pdf_text += pdf_reader.pages[page].extract_text()
        
        # Dictionary to store product rates
        product_rates = {}
        
        # Process tables and extract data
        for table in tables:
            if table.empty:
                continue
                
            for _, row in table.iterrows():
                row_text = ' '.join([str(cell) for cell in row if pd.notna(cell)])
                
                for product in PRODUCTS:
                    # Remove the number prefix for matching
                    product_name = re.sub(r'^\d+\.\s+', '', product)
                    
                    if product_name in row_text:
                        # Look for price pattern (₹ followed by numbers)
                        price_match = re.search(r'₹\s*([\d,]+)', row_text)
                        if price_match:
                            price = price_match.group(1).replace(',', '')
                            product_rates[product] = int(price)
                            break
        
        # If we couldn't find all products in tables, try full text search
        if len(product_rates) < len(PRODUCTS):
            for product in PRODUCTS:
                if product not in product_rates:
                    product_name = re.sub(r'^\d+\.\s+', '', product)
                    pattern = f"{product_name}.*?₹\\s*([\\d,]+)"
                    match = re.search(pattern, pdf_text, re.DOTALL)
                    if match:
                        price = match.group(1).replace(',', '')
                        product_rates[product] = int(price)
        
        # Debug output
        logger.info(f"Extracted data: {product_rates}")
        
        if not product_rates:
            logger.warning("No pricing data found in the PDF")
            return None
            
        # Add the date to the data
        product_rates['date'] = date.strftime('%Y-%m-%d')
        
        return product_rates
    
    except Exception as e:
        logger.error(f"Error extracting data from PDF: {e}")
        return None


def update_csv_files(product_rates):
    """Update the CSV files with the latest pricing data."""
    if not product_rates:
        return False
    
    date = product_rates.get('date')
    if not date:
        return False
    
    for product in PRODUCTS:
        product_file = os.path.join(CSV_DIR, f"{product.split('.')[0].strip()}.csv")
        product_data = {'date': date}
        
        if product in product_rates:
            product_data['rate'] = product_rates[product]
        
        # Read existing CSV
        if os.path.exists(product_file):
            df = pd.read_csv(product_file)
            
            # Check if this date already exists
            if date in df['date'].values:
                # Update existing row
                df.loc[df['date'] == date, 'rate'] = product_data.get('rate', None)
            else:
                # Add new row
                if 'rate' not in product_data:
                    # Get the last available rate
                    if len(df) > 0:
                        last_rate = df['rate'].iloc[-1]
                        product_data['rate'] = last_rate
                    else:
                        product_data['rate'] = None
                
                new_row = pd.DataFrame([product_data])
                df = pd.concat([df, new_row], ignore_index=True)
            
            # Sort by date
            df = df.sort_values('date')
            
            # Fill missing values with previous day's rate
            df['rate'] = df['rate'].fillna(method='ffill')
            
            # Save the updated DataFrame
            df.to_csv(product_file, index=False)
        else:
            # Create new CSV with headers
            with open(product_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['date', 'rate'])
                writer.writeheader()
                if 'rate' in product_data:
                    writer.writerow(product_data)
    
    return True


def main():
    """Main function to download PDF and update CSV files."""
    today = datetime.now()
    
    # Create directory structure
    save_dir = create_directory_structure(today)
    
    # Generate PDF URL for today
    url = get_pdf_url(today)
    
    # Create file path
    file_name = f"primary-ready-reckoner-{today.day:02d}-{today.strftime('%b').lower()}-{today.year}.pdf"
    save_path = os.path.join(save_dir, file_name)
    
    # Check if file already exists
    if os.path.exists(save_path):
        logger.info(f"PDF already exists for today: {save_path}")
        # Still try to process it in case previous run failed at extraction
        product_rates = extract_data_from_pdf(save_path, today)
        update_csv_files(product_rates)
        return
    
    # Download the PDF
    success = download_pdf(url, save_path)
    
    if success:
        # Wait a bit to ensure the file is fully written
        time.sleep(2)
        
        # Extract data from the PDF
        product_rates = extract_data_from_pdf(save_path, today)
        
        # Update CSV files
        update_csv_files(product_rates)
    else:
        logger.warning("PDF download failed. No updates to CSV files.")


if __name__ == "__main__":
    main()
