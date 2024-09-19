import pandas as pd
import numpy as np
import re
import logging
import swifter  # For parallel processing
import json
import time
import psutil
import gc

# Set up logging for tracking the progress and errors in the script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from JSON
def load_config(config_file='config.json'):
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file {config_file} not found.")
        raise
    except json.JSONDecodeError:
        logging.error("Error decoding the configuration file.")
        raise

# Function to log memory usage
def log_memory_usage():
    process = psutil.Process()
    memory_usage = process.memory_info().rss / (1024 ** 2)  # Convert to MB
    logging.info(f"Current memory usage: {memory_usage:.2f} MB")

# Optimize memory usage by converting object columns to category and date columns to datetime
def optimize_memory(df):
    categorical_columns = ['Brand', 'Model', 'Derivative']
    for col in categorical_columns:
        df[col] = df[col].astype('category')
    logging.info("Memory optimization completed by converting object columns to 'category'.")
    return df

# Convert date columns to datetime to reduce memory usage
def optimize_date_columns(df):
    df['Introduced'] = pd.to_datetime(df['Introduced'], errors='coerce', format='%Y-%m-%d')
    df['Discontinued'] = pd.to_datetime(df['Discontinued'], errors='coerce', format='%Y-%m-%d')
    logging.info("Date columns converted to datetime for better memory efficiency.")
    return df

# Load the CSV file into a pandas DataFrame with error handling
def load_data(file_path):
    try:
        dtype_mapping = {
            'Brand': 'object',
            'Model': 'object',
            'Derivative': 'object',
            'Introduced': 'object',
            'Discontinued': 'object'
        }

        df = pd.read_csv(file_path, low_memory=False, dtype=dtype_mapping)
        logging.info("Data Frame loaded successfully.")
        return df
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found.")
        raise
    except pd.errors.EmptyDataError:
        logging.error("The file is empty.")
        raise
    except pd.errors.ParserError:
        logging.error("Error while parsing the file. Please check if the file format is correct.")
        raise

# Function to process the 'Brand' column and extract relevant information
def process_brand_column(df):
    rows_affected_pattern = r'\((\d+) rows affected\)'
    completion_time_pattern = r'Completion time:\s*(\S+)'

    # Extract rows affected using vectorized regex
    df['rows_affected'] = df['Brand'].str.extract(rows_affected_pattern, expand=False).astype(float)

    # Extract completion time using vectorized regex
    df['completion_time'] = df['Brand'].str.extract(completion_time_pattern, expand=False)

    # Log the total rows affected and completion time
    total_rows_affected = df['rows_affected'].sum()
    logging.info(f"Total rows affected: {total_rows_affected}")

    completion_times = df['completion_time'].dropna().unique()
    for time in completion_times:
        logging.info(f"Completion time: {time}")

    # Drop rows with extracted information (where either 'rows_affected' or 'completion_time' is not NaN)
    df_cleaned = df[df['rows_affected'].isna() & df['completion_time'].isna()].copy()

    # Drop the temporary columns
    df_cleaned.drop(['rows_affected', 'completion_time'], axis=1, inplace=True)

    return df_cleaned

# Function to fix swapped Brand, Model, and Derivative fields using vectorized operations
def fix_swapped_fields(df, known_brands):
    try:
        model_mask = df['Model'].isin(known_brands)
        derivative_mask = df['Derivative'].isin(known_brands)

        df.loc[model_mask, ['Brand', 'Model']] = df.loc[model_mask, ['Model', 'Brand']].values
        df.loc[derivative_mask, ['Brand', 'Derivative']] = df.loc[derivative_mask, ['Derivative', 'Brand']].values
        logging.info("Brand, Model, and Derivative fields fixed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error in fixing swapped fields: {e}", exc_info=True)
        raise

# Handle fields cleaning and bracket correction
def clean_fields(df):
    try:
        df['Derivative'] = df['Derivative'].apply(
            lambda x: x + ']' if isinstance(x, str) and '[' in x and ']' not in x else x
        )
        df['Package'] = df['Derivative'].str.extract(r'\[(.*?)\]')
        df['Derivative'] = df['Derivative'].str.replace(r'\s*\[.*?\]', '', regex=True)
        logging.info("Fields cleaned and package information extracted.")
        return df
    except Exception as e:
        logging.error(f"Error in cleaning fields: {e}", exc_info=True)
        raise

# Handle missing Brand and Model values based on matching Derivative
def fill_missing_brand_model_from_derivative(row, df):
    try:
        if pd.isna(row['Brand']) and pd.isna(row['Model']) and pd.notna(row['Derivative']):
            matching_rows = df[(df['Derivative'] == row['Derivative']) & df['Brand'].notna() & df['Model'].notna()]
            if not matching_rows.empty:
                matching_row = matching_rows.iloc[0]
                return pd.Series([matching_row['Brand'], matching_row['Model'], row['Derivative']])
        return pd.Series([row['Brand'], row['Model'], row['Derivative']])
    except Exception as e:
        logging.error(f"Error in filling missing Brand and Model: {e}", exc_info=True)
        raise

# Function to clean the Introduced column and extract the valid date
def clean_introduced_field(field):
    try:
        field = str(field).strip()
        field = re.sub(r'^[^\d]*', '', field)  # Remove all non-numeric characters from the start
        match = re.search(r'(20\d{2}-\d{2}-\d{2})', field)
        return match.group(0) if match else np.nan
    except Exception as e:
        logging.error(f"Error in cleaning Introduced field: {e}", exc_info=True)
        return np.nan

# Function to clean the Discontinued column
def clean_discontinued_field(value):
    try:
        if pd.notna(value):
            value = str(value).strip()
            if value.startswith('0'):
                return value.split()[1]  # Return date part only
            return value.split()[0]  # Return first part before any time component
        return value
    except Exception as e:
        logging.error(f"Error in cleaning Discontinued field: {e}", exc_info=True)
        raise

# Remove duplicate rows
def remove_duplicates(df):
    try:
        duplicate_rows = df[df.duplicated(keep=False)]
        if not duplicate_rows.empty:
            logging.info(f"Found {duplicate_rows.shape[0]} duplicate rows. Removing duplicates.")
            df = df.drop_duplicates(keep='first')
        return df
    except Exception as e:
        logging.error(f"Error in removing duplicates: {e}", exc_info=True)
        raise

# Check for remaining missing values
def check_missing_values(df):
    try:
        if df.isnull().values.any():
            logging.warning("There are still some missing values in the DataFrame.")
        else:
            logging.info("No missing values in the DataFrame.")
    except Exception as e:
        logging.error(f"Error in checking missing values: {e}", exc_info=True)
        raise

# Main function to handle the data cleaning process
def main():
    try:
        # Start timing the entire script
        start_time = time.time()

        # Load the configuration
        config = load_config()

        # Load the CSV file into a pandas DataFrame
        df_cars = load_data(config['data_file'])

        # Process the Brand column to extract and log rows affected and completion time
        df_cars = process_brand_column(df_cars)

        # Perform data cleaning steps
        df_cars = fix_swapped_fields(df_cars, config['known_brands'])
        df_cars['Brand'] = df_cars['Brand'].fillna(method='ffill').fillna(method='bfill')
        df_cars = clean_fields(df_cars)
        df_cars[['Brand', 'Model', 'Derivative']] = df_cars.swifter.apply(
            lambda row: fill_missing_brand_model_from_derivative(row, df_cars), axis=1
        )
        
        # Apply the updated cleaning function to the 'Introduced' column
        df_cars['Introduced'] = df_cars['Introduced'].apply(clean_introduced_field)
        df_cars['Discontinued'] = df_cars['Discontinued'].apply(clean_discontinued_field)
        
        # Remove duplicates and check for missing values
        df_cars = remove_duplicates(df_cars)
        check_missing_values(df_cars)

         # Now optimize memory usage
        df_cars = optimize_memory(df_cars)
        df_cars = optimize_date_columns(df_cars)

        # Log memory usage after optimization
        log_memory_usage()

        # Save the cleaned DataFrame to a new CSV file
        df_cars.to_csv(config['output_file'], index=False)
        logging.info("Data cleaning completed successfully. Updated DataFrame saved.")

        # Log total time taken
        total_time = time.time() - start_time
        logging.info(f"Data cleaning completed in {total_time:.2f} seconds.")

    except Exception as e:
        logging.error(f"An error occurred during the data cleaning process: {e}", exc_info=True)

# Run the main function
if __name__ == "__main__":
    main()

