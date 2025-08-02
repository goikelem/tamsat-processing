# tamsat-processing
This Bash script automates TAMSAT rainfall data processing: downloads daily NetCDF files from JASMIN seriver, validates integrity, merges timeseries with CDO (parallelized), and extracts city-specific rainfall data. Features robust error handling, retry logic, and logging. Generates monthly/seasonal aggregates, CSV outputs, validation reports.
