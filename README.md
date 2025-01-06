# Automate_ETL_Process
"""
ETL Process for Bank Marketing Data

This script implements an ETL (Extract, Transform, Load) process to clean and transform the bank marketing campaign dataset and load the transformed data into a new CSV file.

### Key Features:
1. **Extract**: Reads data from a CSV file (`bank_marketing.csv`) and stores it in a pandas DataFrame.
2. **Transform**: Performs various data transformations, including:
   - Replaces periods (`.`) in `job` and `education` columns with underscores (`_`).
   - Replaces the 'unknown' values in the `education` column with NaN (missing values).
   - Converts `credit_default` and `mortgage` columns to boolean values (1 for 'yes', 0 for 'no').
   - Maps `previous_outcome` and `campaign_outcome` columns to binary values (1 for 'success'/'yes' and 0 for 'failure'/'no').
   - Adds a `last_contact_date` column in a datetime format.
   - Drops unnecessary columns like `month`, `day`, and `year`.
3. **Load**: Saves the transformed DataFrame into a new CSV file (`Transformed_bank_marketing.csv`).

### Logging:
The script uses Python's `logging` module to log the status of each step (extraction, transformation, and loading), ensuring clear tracking of any issues encountered during the ETL process.

### Error Handling:
Each function includes error handling to ensure that any failures in extraction, transformation, or loading are logged with relevant error messages.

### Usage:
- The ETL process can be triggered by running the script, which automatically extracts, transforms, and loads the data.
- The final output is stored in a CSV file that is ready for further analysis or use in a database.

Dependencies:
- pandas
- numpy
- logging

"""
