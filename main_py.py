def main():
    """Main function to download PDF and update CSV files."""
    today = datetime.now()
    
    # Handle migrating existing CSVs (if needed) - one-time operation for existing files
    migrate_existing_csvs()
    
    # Create directory structure
    save_dir = create_directory_structure(today)
    
    # Generate PDF URL for today
    url = get_pdf_url(today)
    logger.info(f"Attempting to download from URL: {url}")
    
    # Create file path
    file_name = f"primary-ready-reckoner-{today.day:02d}-{today.strftime('%b').lower()}-{today.year}.pdf"
    save_path = os.path.join(save_dir, file_name)
    
    product_rates = None
    
    # Check if file already exists
    if os.path.exists(save_path):
        logger.info(f"PDF already exists for today: {save_path}")
        # Still try to process it in case previous run failed at extraction
        product_rates = extract_data_from_pdf(save_path, today)
    else:
        # Try with current date
        success = download_pdf(url, save_path)
        
        # If download fails, try with yesterday's date as fallback
        if not success:
            yesterday = today - timedelta(days=1)
            yesterday_url = get_pdf_url(yesterday)
            logger.info(f"First attempt failed. Trying yesterday's URL: {yesterday_url}")
            
            yesterday_file_name = f"primary-ready-reckoner-{yesterday.day:02d}-{yesterday.strftime('%b').lower()}-{yesterday.year}.pdf"
            yesterday_save_path = os.path.join(save_dir, yesterday_file_name)
            
            if not os.path.exists(yesterday_save_path):
                success = download_pdf(yesterday_url, yesterday_save_path)
                if success:
                    save_path = yesterday_save_path
                    today = yesterday  # Update date for processing
        
        if success:
            # Wait a bit to ensure the file is fully written
            time.sleep(2)
            
            # Extract data from the PDF
            product_rates = extract_data_from_pdf(save_path, today)
        else:
            logger.warning("PDF download failed for both current and previous day.")
    
    # Even if we couldn't get data from PDF, still update CSVs with previous day's rates
    if product_rates is None or not product_rates:
        logger.info("No valid data extracted from PDF. Will use previous day's rates for continuity.")
        # Create empty product_rates with just the date
        product_rates = {'date': today.strftime('%Y-%m-%d')}
    
    # Update CSV files - even with empty product_rates, this will use previous day's values
    update_csv_files(product_rates)


def update_csv_files(product_rates):
    """Update the CSV files with the latest pricing data."""
    if not product_rates or 'date' not in product_rates:
        logger.warning("No date provided in product rates. Using today's date.")
        product_rates = {'date': datetime.now().strftime('%Y-%m-%d')}
    
    date = product_rates.get('date')
    
    # Track if we successfully updated anything
    update_success = False
    
    for product in PRODUCTS:
        product_file = os.path.join(CSV_DIR, f"{PRODUCT_FILE_NAMES[product]}.csv")
        product_data = {
            'date': date,
            'description': PRODUCT_DESCRIPTIONS[product]
        }
        
        if product in product_rates:
            product_data['rate'] = product_rates[product]
            logger.info(f"Using extracted rate {product_data['rate']} for {product} on {date}")
            update_success = True
        else:
            logger.info(f"No rate found for {product} on {date}, will use previous day's rate")
        
        # Read existing CSV
        if os.path.exists(product_file):
            df = pd.read_csv(product_file)
            logger.info(f"Read existing CSV file {product_file} with {len(df)} rows")
            
            # Check if this date already exists
            if date in df['date'].values:
                # Update existing row only if we have new data
                if 'rate' in product_data:
                    df.loc[df['date'] == date, 'rate'] = product_data['rate']
                    logger.info(f"Updated existing row for date {date}")
            else:
                # Add new row - always use previous rate if no new rate available
                if 'rate' not in product_data and len(df) > 0:
                    prev_rates = df[df['date'] < date]
                    if len(prev_rates) > 0:
                        # Get the most recent rate
                        last_rate = prev_rates['rate'].iloc[-1]
                        product_data['rate'] = last_rate
                        logger.info(f"Using previous rate {last_rate} for date {date}")
                    else:
                        # If no previous rates, use fallback values based on product
                        fallback_rates = {
                            PRODUCTS[0]: 252500,  # P0406
                            PRODUCTS[1]: 251000,  # P0610
                            PRODUCTS[2]: 250500,  # CG Grade
                            PRODUCTS[3]: 259750,  # EC Grade Wire Rods
                            PRODUCTS[4]: 267500,  # 6201 Alloy Wire Rod
                            PRODUCTS[5]: 268600,  # Billets (AA6063) 7", 8" & 9"
                            PRODUCTS[6]: 270100,  # Billets (AA6063) 5", 6"
                        }
                        product_data['rate'] = fallback_rates.get(product, 250000)
                        logger.warning(f"No previous rate available, using fallback rate {product_data['rate']} for {product}")
                
                new_row = pd.DataFrame([product_data])
                df = pd.concat([df, new_row], ignore_index=True)
                logger.info(f"Added new row for date {date}")
            
            # Sort by date
            df = df.sort_values('date')
            
            # Save the updated DataFrame
            df.to_csv(product_file, index=False)
            logger.info(f"Saved updated CSV file with {len(df)} rows")
        else:
            # Create new CSV with headers
            logger.info(f"Creating new CSV file {product_file}")
            with open(product_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['date', 'description', 'rate'])
                writer.writeheader()
                
                # Always ensure we have a rate, even for new files
                if 'rate' not in product_data:
                    # Use fallback values for new files
                    fallback_rates = {
                        PRODUCTS[0]: 252500,  # P0406
                        PRODUCTS[1]: 251000,  # P0610
                        PRODUCTS[2]: 250500,  # CG Grade
                        PRODUCTS[3]: 259750,  # EC Grade Wire Rods
                        PRODUCTS[4]: 267500,  # 6201 Alloy Wire Rod
                        PRODUCTS[5]: 268600,  # Billets (AA6063) 7", 8" & 9"
                        PRODUCTS[6]: 270100,  # Billets (AA6063) 5", 6"
                    }
                    product_data['rate'] = fallback_rates.get(product, 250000)
                    logger.info(f"Using fallback rate {product_data['rate']} for initial data")
                
                writer.writerow(product_data)
                logger.info(f"Wrote first row with date {date} and rate {product_data['rate']}")
    
    return update_success


def check_pdf_validity(pdf_path):
    """Check if the PDF is valid and contains usable data."""
    try:
        # Try to open with PyPDF2
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        if len(pdf_reader.pages) == 0:
            logger.warning(f"PDF at {pdf_path} has no pages")
            return False
            
        # Extract some text to verify content
        first_page_text = pdf_reader.pages[0].extract_text()
        if not first_page_text or len(first_page_text) < 100:
            logger.warning(f"PDF at {pdf_path} appears to have insufficient text content")
            return False
            
        # Look for key indicators that this is the right type of document
        indicators = ['hindalco', 'aluminium', 'price', 'rate', 'reckoner']
        if not any(indicator in first_page_text.lower() for indicator in indicators):
            logger.warning(f"PDF at {pdf_path} doesn't appear to be a Hindalco rate sheet")
            return False
            
        logger.info(f"PDF at {pdf_path} appears valid")
        return True
        
    except Exception as e:
        logger.error(f"Error validating PDF at {pdf_path}: {e}")
        return False


def extract_data_from_pdf(pdf_path, date):
    """Extract pricing data from the PDF file."""
    try:
        logger.info(f"Starting data extraction from {pdf_path}")
        
        # First check if the PDF is valid
        if not check_pdf_validity(pdf_path):
            logger.warning(f"Invalid or corrupted PDF at {pdf_path}, skipping extraction")
            return None
        
        # Continue with existing extraction logic...
        # [rest of the function remains the same]
        
        # Use PyPDF2 to extract all text first
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        pdf_text = ""
        for page in range(len(pdf_reader.pages)):
            page_text = pdf_reader.pages[page].extract_text()
            pdf_text += page_text
            logger.info(f"Extracted text from page {page+1}, length: {len(page_text)} characters")
        
        logger.info(f"Total extracted text length: {len(pdf_text)} characters")
        logger.info(f"First 500 characters of extracted text: {pdf_text[:500]}")
        
        # Try direct price extraction first - most reliable for this specific PDF format
        logger.info("Attempting direct pattern matching for prices from PDF text")
        product_rates = extract_prices_directly(pdf_text)
        
        # If direct extraction fails, fall back to tabula
        if not product_rates or len(product_rates) < len(PRODUCTS):
            logger.info("Direct extraction incomplete. Trying tabula extraction.")
            tabula_rates = extract_with_tabula(pdf_path)
            
            # Merge results, preferring tabula results over direct extraction
            if tabula_rates:
                for product, rate in tabula_rates.items():
                    product_rates[product] = rate
        
        # If still missing products, try text search
        if not product_rates or len(product_rates) < len(PRODUCTS):
            logger.info("Still missing products. Trying full text search.")
            text_search_rates = extract_with_text_search(pdf_text)
            
            # Add any missing products
            if text_search_rates:
                for product, rate in text_search_rates.items():
                    if product not in product_rates:
                        product_rates[product] = rate
        
        # Final fallback to manual extraction
        if not product_rates or len(product_rates) < len(PRODUCTS):
            logger.info("Still missing products. Trying manual extraction.")
            manual_rates = manual_extract_tables(pdf_text)
            
            # Add any missing products
            if manual_rates:
                for product, rate in manual_rates.items():
                    if product not in product_rates:
                        product_rates[product] = rate
        
        logger.info(f"Final extracted data: {product_rates}")
        
        # Add the date to the data if we have any extracted rates
        if product_rates:
            product_rates['date'] = date.strftime('%Y-%m-%d')
            return product_rates
        else:
            logger.warning("No pricing data found using any extraction method")
            return {'date': date.strftime('%Y-%m-%d')}
        
    except Exception as e:
        logger.error(f"Error extracting data from PDF: {e}")
        logger.exception("Full traceback:")
        return {'date': date.strftime('%Y-%m-%d')}  # Return minimal data with just the date
