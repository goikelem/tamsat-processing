## tamsat-processing
This Bash script automates TAMSAT rainfall data processing: downloads daily NetCDF files from JASMIN seriver, validates integrity, merges timeseries with CDO (parallelized), and extracts city-specific rainfall data. Features robust error handling, retry logic, and logging. Generates monthly/seasonal aggregates, CSV outputs, validation reports.


# Features

Automated Download: Retrieves daily TAMSAT rainfall data (v3.1) from JASMIN servers
Data Processing: Uses CDO (Climate Data Operators) for:
Timeseries merging
Monthly/seasonal aggregation
Spatial extraction
City Data Extraction: Generates CSV files for specific locations
Validation: Comprehensive checks at each processing stage
Logging: Detailed logging for troubleshooting
Error Handling: Retry mechanisms and fallback procedures

# Requirements

Essential Tools:
  - `bash` (v4.0+)
  - `cdo` (Climate Data Operators)
  - `wget`
  - `ncdump` (from NetCDF utilities)
  - `awk`, `date`, and other core utilities

- Recommended Specifications:
  - 4+ CPU cores
  - 10GB+ free disk space
  - Stable internet connection

# Installation

1. Clone this repository:
   bash
   git clone https://github.com/goikelem/tamsat-processing.git
   cd tamsat-processing
2. Ensure all dependencies are installed:

<pre> ```bash # On Ubuntu/Debian sudo apt-get install cdo netcdf-bin wget ``` </pre> # 🚀 Required for CDO processing

3. Configuration

Edit the script directly to configure:

 3.1 Date Range:
START_DATE="2023-01-01"
LAST_AVAILABLE="2025-07-25"  # Update this as needed

3.2 Cities (modify the associative array):
declare -A CITIES=(
    ["Mekelle"]="13.50,39.47"
    ["Adigrat"]="14.28,39.46"
    # Add more cities as needed
)

3.3 Parallel Processing (adjust based on your CPU cores):
   CDO_THREADS=4
4. Usage

<pre> ``` ./process_tamsat.sh ``` </pre> #  ✅ Run the script 


5. Output Structure
The script creates the following directory structure in $HOME/TAMSAT_Data:

TAMSAT_Data/
├── daily/                # Raw daily NetCDF files
├── monthly/              # Monthly aggregated data
├── seasonal/             # Seasonal aggregates
├── cities/               # CSV files for each city
│   ├── Mekelle_monthly.csv
│   ├── Adigrat_monthly.csv
│   └── all_cities_monthly.csv  # Combined data
├── logs/                 # Processing logs
├── tmp/                  # Temporary files
└── Tamsat_YYYYMMDD.tar.gz # Final archive


Processing Steps

    Data Download:

        Retrieves daily TAMSAT RFE files

        Implements retry logic for failed downloads

        Validates file integrity

    Data Processing:

        Merges daily files into a complete timeseries

        Generates monthly and seasonal aggregates

        Extracts point data for specified cities

    Validation:

        Checks file sizes and NetCDF integrity

        Verifies temporal coverage

        Validates output formats

    Archiving:

        Creates compressed archive of all outputs

        Generates summary reports

Customization
Adding New Cities

Edit the CITIES associative array in the script:













































