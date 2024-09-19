# Data Engineering Project

## Overview
This project is a comprehensive data engineering task focused on cleaning, transforming, and preparing vehicle-related data for a lead generation system. The data is ingested, cleaned, and prepared for both structured and unstructured storage formats, such as relational databases (SQL) and NoSQL systems (like JSON). The code has been optimized for running efficiently both locally and in cloud environments, specifically leveraging Azure resources.

## Project Files
### 1. `Data_Cleaning_V8.py`
This Python script handles the core data cleaning process. It:
- Loads a CSV file containing vehicle data with errors.
- Cleans and normalizes the data, including removing unwanted characters and fixing inconsistent formats in columns such as `Brand`, `Model`, and `Introduced`.
- Handles missing values, deduplication, and memory optimization for large datasets.
- Provides parallel processing capabilities using `swifter` for improved performance.
- Exports the cleaned data to a new CSV file for downstream consumption.

### 2. `Data_Model.py`
This script:
- Takes the cleaned vehicle data and prepares it for insertion into a relational database.
- Splits the data into separate tables (`Brands`, `Models`, `Derivatives`, and `Vehicle Info`) to maintain data integrity and avoid redundancy.
- Outputs each table as a CSV file, ready for import into a relational database system.

### 3. `Untitled-1.json`
This JSON file provides an example of the cleaned data in an unstructured format, demonstrating how the vehicle data can be organized for NoSQL databases. The structure follows a hierarchical format:
- `Brand` -> `Models` -> `Derivatives` -> `VehicleInfo`.
This format is suitable for document-based databases like MongoDB or Azure Cosmos DB.

## Prerequisites
- Python 3.8+
- Required Python libraries (can be installed via `pip`):
  - `pandas`
  - `numpy`
  - `swifter`
  - `psutil`
  - `re` (regular expressions)

## Instructions for Running
1. Clone the project repository:

2. Install the required libraries:
   ```bash
   pip install -r requirements.txt
   ```

3. Modify the `config.json` file to ensure the file paths are correct:
   ```json
   {
      "data_file": "path/to/input_file.csv",
      "output_file": "path/to/output_file.csv",
      "log_level": "INFO",
      "known_brands": ["BMW", "Mercedes-Benz", "Audi", "Toyota"]
   }
   ```

4. Run the data cleaning script:
   ```bash
   python Data_Cleaning_V8.py
   ```

5. To prepare the cleaned data for insertion into a relational database, run:
   ```bash
   python Data_Model.py
   ```

## Features
- **Data Cleaning**: Corrects formatting issues, removes unwanted characters, and handles missing data.
- **Memory Optimization**: Converts object columns to categorical types and optimizes date columns to reduce memory usage.
- **Parallel Processing**: Utilizes `swifter` for parallelized operations, improving performance on large datasets.
- **Structured & Unstructured Formats**: Outputs data for both relational (SQL) and non-relational (NoSQL) databases.

## Example NoSQL JSON Structure
```json
{
  "Brand": "BMW",
  "Models": [
    {
      "Model": "Series 3",
      "Derivatives": [
        {
          "Derivative": "320i",
          "VehicleInfo": {
            "Introduced": "2019-05-01",
            "Discontinued": "2023-01-01"
          }
        },
        {
          "Derivative": "330i",
          "VehicleInfo": {
            "Introduced": "2020-04-01",
            "Discontinued": null
          }
        }
      ]
    }
  ]
}
```

## Technologies Used
- **Python**: For data processing and cleaning.
- **Pandas**: For data manipulation.
- **Swifter**: For parallelized data processing.
- **JSON**: For unstructured data format.
- **CSV**: For storing relational data outputs.


## License
This project is licensed under the MIT License. See the `LICENSE` file for more details.
