# Hindalco Price Data Tracker

This repository automatically downloads and processes the primary ready reckoner PDFs from Hindalco's website and maintains a historical record of aluminum pricing data.

## Features

- Automatically downloads PDFs from Hindalco's website twice daily (5 PM and 10 PM)
- Organizes PDFs in a year/month folder structure
- Extracts pricing data for 7 different product categories
- Maintains CSV files with historical pricing trends
- Avoids duplicate downloads
- Fills missing data with the previous day's rates

## Repository Structure

```
├── main.py                    # Main Python script
├── .github/workflows/         # GitHub Actions workflow files
├── downloads/                 # PDF storage (organized by year/month)
│   ├── 2025/
│   │   ├── 05-May/
│   │   │   └── primary-ready-reckoner-14-may-2025.pdf
└── csv/                       # Historical pricing data in CSV format
    ├── 1.csv                  # P0406 pricing data
    ├── 2.csv                  # P0610/P1020/EC Grade pricing data
    └── ...
```

## Product Categories

The script extracts and maintains pricing data for the following product categories:

1. P0406 (Si 0.04% max, Fe 0.06% max) 99.85% (min)
2. P0610 (99.85% min) /P1020/ EC Grade Ingot & Sow 99.7% (min) / Cast Bar
3. CG Grade Ingot & Sow 99.5% (min) purity
4. EC Grade Wire Rods, Dia 9.5 mm - Conductivity 61% min
5. 6201 Alloy Wire Rod - Dia 9.5 mm (HAC-1)
6. Billets (AA6063) Dia 7", 8" & 9" - subject to availability
7. Billets (AA6063) Dia 5" , 6" - subject to availability

## Setup and Configuration

The repository is configured to run automatically via GitHub Actions. The workflow runs at 5 PM and 10 PM every day.

### Local Setup

To run the script locally:

1. Clone the repository
2. Install the required dependencies:
   ```
   pip install requests pandas tabula-py PyPDF2
   ```
3. Run the script:
   ```
   python main.py
   ```

## CSV Data Format

Each CSV file contains two columns:
- `date`: The date in YYYY-MM-DD format
- `rate`: The price for that particular product on that date

If no data is available for a specific date, the rate will be filled with the previous day's rate.
