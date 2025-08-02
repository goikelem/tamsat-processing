#!/bin/bash
# TAMSAT Data Downloading and Processing 

# Configuration
START_DATE="2023-01-01"
LAST_AVAILABLE="2025-07-25"  # Update this to match actual data availability
TODAY=$(date +%Y-%m-%d)
MIN_FILE_SIZE=1000  # 1KB minimum file size
MAX_RETRIES=3
CDO_THREADS=4       # Number of parallel threads for CDO

# Validate and adjust date ranges
if [[ $(date -d "$START_DATE" +%s 2>/dev/null) -gt $(date -d "$TODAY" +%s 2>/dev/null) ]]; then
    echo "ERROR: START_DATE cannot be in the future" >&2
    exit 1
fi

END_DATE="$LAST_AVAILABLE"
if [[ $(date -d "$END_DATE" +%s 2>/dev/null) -gt $(date -d "$TODAY" +%s 2>/dev/null) ]]; then
    END_DATE="$TODAY"
    echo "NOTICE: Adjusted END_DATE to today: $END_DATE"
fi

# Directory structure
DATADIR="$HOME/TAMSAT_Data"
DAILY_DIR="$DATADIR/daily"
MONTHLY_DIR="$DATADIR/monthly"
SEASONAL_DIR="$DATADIR/seasonal"
CITIES_DIR="$DATADIR/cities"  # Fixed typo from previous version
LOGS_DIR="$DATADIR/logs"
TEMP_DIR="$DATADIR/tmp"
ARCHIVE_NAME="Tamsat_$(date +%Y%m%d_%H%M%S).tar.gz"

# Cities data (Latitude, Longitude)
declare -A CITIES=(
    ["Mekelle"]="13.50,39.47"
    ["Adigrat"]="14.28,39.46"
    ["Axum"]="14.12,38.72"
#    ["Shire"]="14.10,38.28"
#    ["Adwa"]="14.16,38.89"
#    ["Wukro"]="13.79,39.60"
#    ["Humera"]="14.29,36.61"
#    ["Korem"]="12.51,39.52"
#    ["Alamata"]="12.41,39.56"
#    ["Maychew"]="12.78,39.54"
#    ["Abiy_Addi"]="13.62,39.00"
)

# Create directories with error checking
mkdir -p "$DAILY_DIR" "$MONTHLY_DIR" "$SEASONAL_DIR" \
         "$CITIES_DIR" "$LOGS_DIR" "$TEMP_DIR" || {
    echo "ERROR: Failed to create directories" >&2
    exit 1
}

# Setup logging
LOG_FILE="$LOGS_DIR/process_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== TAMSAT PROCESSING STARTED $(date) ==="
echo "PROCESSING PERIOD: $START_DATE to $END_DATE"

# Enhanced file validation function
check_nc_file() {
    local file=$1
    if [ ! -f "$file" ]; then
        echo "ERROR: File missing: $file" >&2
        return 1
    fi
    if [ $(stat -c%s "$file") -lt $MIN_FILE_SIZE ]; then
        echo "ERROR: File too small (possibly corrupt): $file" >&2
        return 1
    fi
    if ! ncdump -h "$file" >/dev/null 2>&1; then
        echo "ERROR: Corrupt NetCDF file: $file" >&2
        return 1
    fi
    return 0
}

# -------------------------------------------------------------------
# STEP 1: Download daily TAMSAT data with robust error handling
# -------------------------------------------------------------------
echo "=== STEP 1: DOWNLOADING DAILY DATA ==="

cd "$DAILY_DIR" || { echo "ERROR: Cannot access $DAILY_DIR" >&2; exit 1; }

current_date="$START_DATE"
downloaded=0 skipped=0 failed=0

while : ; do
    # Safely break if current date exceeds end date
    current_epoch=$(date -d "$current_date" +%s 2>/dev/null)
    end_epoch=$(date -d "$END_DATE" +%s 2>/dev/null)
    [ -z "$current_epoch" ] || [ -z "$end_epoch" ] && break
    [ "$current_epoch" -gt "$end_epoch" ] && break
    
    year=$(date -d "$current_date" +%Y 2>/dev/null)
    month=$(date -d "$current_date" +%m 2>/dev/null)
    day=$(date -d "$current_date" +%d 2>/dev/null)

    if [[ -z "$year" || -z "$month" || -z "$day" ]]; then
        echo "WARNING: Failed to parse date: $current_date"
        current_date=$(date -I -d "$current_date + 1 day" 2>/dev/null)
        continue
    fi

    filename="rfe${year}_${month}_${day}.v3.1.nc"
    url="https://gws-access.jasmin.ac.uk/public/tamsat/rfe/data/v3.1/daily/${year}/${month}/${filename}"
    
    # Skip if valid file exists
    if [ -f "$filename" ] && check_nc_file "$filename"; then
        ((skipped++))
        current_date=$(date -I -d "$current_date + 1 day" 2>/dev/null)
        continue
    fi

    # Download with retries
    retry=0
    while [ $retry -lt $MAX_RETRIES ]; do
        if wget -q --show-progress "$url" -O "$filename.tmp"; then
            if check_nc_file "$filename.tmp"; then
                mv "$filename.tmp" "$filename"
                ((downloaded++))
                echo "SUCCESS: Downloaded $filename"
                break
            else
                echo "WARNING: Invalid download: $filename.tmp"
                rm -f "$filename.tmp"
            fi
        else
            echo "WARNING: Download attempt $((retry+1)) failed for $filename"
        fi
        
        ((retry++))
        [ $retry -lt $MAX_RETRIES ] && sleep $((retry * 5))
    done

    [ $retry -eq $MAX_RETRIES ] && ((failed++))
    current_date=$(date -I -d "$current_date + 1 day" 2>/dev/null)
done

echo "DOWNLOAD SUMMARY:"
echo "- Successfully downloaded: $downloaded"
echo "- Already existed: $skipped"
echo "- Failed downloads: $failed"

if [ $downloaded -eq 0 ] && [ ! -f "$DAILY_DIR/rfe${year}_${month}_01.v3.1.nc" ]; then
    echo "ERROR: No valid daily files available" >&2
    exit 1
fi

# -------------------------------------------------------------------
# STEP 2: Process with CDO with comprehensive validation
# -------------------------------------------------------------------
echo -e "\n=== STEP 2: DATA PROCESSING ==="

# 1. Validate and prepare files for merging
DAILY_FILES=($(ls rfe*.nc 2>/dev/null))
if [ ${#DAILY_FILES[@]} -eq 0 ]; then
    echo "ERROR: No daily files found to process" >&2
    exit 1
fi

# 2. Create full timeseries with validation
FULL_TS_FILE="$DATADIR/full_timeseries.nc"
if [ ! -f "$FULL_TS_FILE" ]; then
    echo "MERGING ${#DAILY_FILES[@]} DAILY FILES..."
    
    # First attempt with parallel processing
    if ! timeout 3600 cdo -b F32 -P $CDO_THREADS mergetime "${DAILY_FILES[@]}" "$FULL_TS_FILE.tmp" || \
       ! check_nc_file "$FULL_TS_FILE.tmp"; then
        echo "WARNING: Standard merge failed, trying alternative method..."
        rm -f "$FULL_TS_FILE.tmp"
        
        # Alternative method with file list
        printf "%s\n" "${DAILY_FILES[@]}" > "$TEMP_DIR/filelist.txt"
        if ! timeout 5400 cdo -b F32 -P $CDO_THREADS mergetime -f "$TEMP_DIR/filelist.txt" "$FULL_TS_FILE.tmp" || \
           ! check_nc_file "$FULL_TS_FILE.tmp"; then
            echo "ERROR: All merge attempts failed" >&2
            rm -f "$FULL_TS_FILE.tmp"
            exit 1
        fi
    fi
    
    mv "$FULL_TS_FILE.tmp" "$FULL_TS_FILE"
    echo "SUCCESS: Created full timeseries"
else
    echo "SKIPPING: Full timeseries already exists"
fi

# [Continue with monthly and seasonal processing as before...]

# -------------------------------------------------------------------
# STEP 3: Enhanced city data extraction with proper validation
# -------------------------------------------------------------------
echo -e "\n=== STEP 3: EXTRACTING CITY DATA ==="

if check_nc_file "$FULL_TS_FILE"; then
    # Process each city with proper validation
    for city in "${!CITIES[@]}"; do
        IFS=',' read -r lat lon <<< "${CITIES[$city]}"
        city_csv="$CITIES_DIR/${city}_monthly.csv"
        
        echo "EXTRACTING DATA FOR $city (Lat: $lat, Lon: $lon)"
        
        # Temporary working file
        temp_out="$TEMP_DIR/${city}_temp.csv"
        
        # Extract data with proper headers
        if ! cdo -outputtab,date,value \
                 -remapnn,lon=${lon}_lat=${lat} \
                 -select,name=rfe_filled \
                 "$FULL_TS_FILE" > "$temp_out" 2>/dev/null; then
            echo "WARNING: Failed to extract data for $city"
            continue
        fi
        
        # Process output - remove missing values and format as CSV
        awk 'BEGIN {FS=" "; OFS=","} 
             NR==1 {print "date,precipitation"} 
             NR>1 && $4 != -999.9 {print $1,$2,$3,$4}' \
             "$temp_out" > "$city_csv"
        
        # Verify output
        if [ -s "$city_csv" ]; then
            records=$(($(wc -l < "$city_csv") - 1))  # Exclude header
            echo "SUCCESS: $city_csv with $records valid records"
        else
            echo "WARNING: No valid data for $city"
            rm -f "$city_csv"
        fi
        
        rm -f "$temp_out"
    done
    
    # Create combined CSV only if we have city files
    city_files=($(ls "$CITIES_DIR"/*_monthly.csv 2>/dev/null))
    if [ ${#city_files[@]} -gt 0 ]; then
        echo "CREATING COMBINED CSV FILE..."
        combined_csv="$CITIES_DIR/all_cities_monthly.csv"
        
        # Get the first city file for dates reference
        first_city_file="${city_files[0]}"
        dates=($(awk -F, 'NR>1 {print $1}' "$first_city_file"))
        
        # Create header
        echo "date,$(printf '%s,' "${!CITIES[@]}" | sed 's/,$//')" > "$combined_csv"
        
        # Process each date
        for date in "${dates[@]}"; do
            line="$date"
            for city in "${!CITIES[@]}"; do
                city_file="$CITIES_DIR/${city}_monthly.csv"
                if [ -f "$city_file" ]; then
                    value=$(awk -F, -v d="$date" '$1==d {print $4}' "$city_file")
                    line="$line,${value:-NA}"
                else
                    line="$line,NA"
                fi
            done
            echo "$line" >> "$combined_csv"
        done
        
        echo "SUCCESS: Combined CSV created with ${#dates[@]} records"
    else
        echo "WARNING: No valid city files found for combined CSV"
    fi
else
    echo "ERROR: Cannot extract city data - full timeseries is missing or invalid" >&2
    echo "DEBUG: FULL_TS_FILE status:"
    ls -l "$FULL_TS_FILE" 2>/dev/null || echo "File does not exist"
    exit 1
fi

# -------------------------------------------------------------------
# STEP 4: Archive with comprehensive validation
# -------------------------------------------------------------------
echo -e "\n=== STEP 4: ARCHIVING RESULTS ==="

cd "$DATADIR" || { echo "ERROR: Cannot access $DATADIR" >&2; exit 1; }

# Check what we have to archive
dirs_to_archive=()
[ -d "daily" ] && [ "$(ls -A daily)" ] && dirs_to_archive+=("daily")
[ -d "monthly" ] && [ "$(ls -A monthly)" ] && dirs_to_archive+=("monthly")
[ -d "seasonal" ] && [ "$(ls -A seasonal)" ] && dirs_to_archive+=("seasonal")
[ -d "cities" ] && [ "$(ls -A cities)" ] && dirs_to_archive+=("cities")
[ -d "logs" ] && [ "$(ls -A logs)" ] && dirs_to_archive+=("logs")

if [ ${#dirs_to_archive[@]} -gt 0 ]; then
    echo "ARCHIVING: ${dirs_to_archive[*]}"
    if tar -czvf "$ARCHIVE_NAME" "${dirs_to_archive[@]}"; then
        echo "SUCCESS: Created archive $ARCHIVE_NAME"
        echo "ARCHIVE SIZE: $(du -h "$ARCHIVE_NAME" | cut -f1)"
        echo "ARCHIVE CONTENTS:"
        tar -tzvf "$ARCHIVE_NAME" | head -5
        echo "[...]"
    else
        echo "ERROR: Failed to create archive" >&2
        exit 1
    fi
else
    echo "WARNING: No directories with content to archive"
fi

# -------------------------------------------------------------------
# Final validation and reporting
# -------------------------------------------------------------------
echo -e "\n=== PROCESSING COMPLETE ==="
echo "=== VALIDATION SUMMARY ==="

# Check key files
declare -A file_checks=(
    ["Full Timeseries"]="$FULL_TS_FILE"
    ["Monthly Totals"]="$MONTHLY_DIR/monthly_totals.nc"
    ["Combined CSV"]="$CITIES_DIR/all_cities_monthly.csv"
)

for label in "${!file_checks[@]}"; do
    file="${file_checks[$label]}"
    if [ -f "$file" ]; then
        if [ "${file##*.}" == "nc" ] && ! check_nc_file "$file"; then
            echo "[INVALID] $label: $file (corrupt)"
        else
            echo "[VALID]   $label: $file"
        fi
    else
        echo "[MISSING] $label: $file"
    fi
done

# Count outputs
echo -e "\n=== OUTPUT COUNTS ==="
echo "Daily Files:    $(ls -1 "$DAILY_DIR" 2>/dev/null | wc -l)"
echo "Monthly Files:  $(ls -1 "$MONTHLY_DIR" 2>/dev/null | wc -l)"
echo "Seasonal Files: $(ls -1 "$SEASONAL_DIR" 2>/dev/null | wc -l)"
echo "City CSVs:      $(ls -1 "$CITIES_DIR"/*_monthly.csv 2>/dev/null | wc -l)"

echo -e "\n=== NEXT STEPS ==="
echo "1. Verify city data: head -5 $CITIES_DIR/Mekelle_monthly.csv"
echo "2. Check for errors: grep -i error $LOG_FILE"
echo "3. Transfer archive: scp $DATADIR/$ARCHIVE_NAME yourserver:/path/"
echo "4. Clean up temporary files: rm -rf $TEMP_DIR"

exit 0
