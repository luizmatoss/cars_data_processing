import pandas as pd

# Load the cleaned data
df_cars = pd.read_csv("/Users/luizcaprio/Desktop/ETL/TLA_Data_With_Errors_Cleaned_Final.csv")

# Step 1: Extract unique brands
df_brands = df_cars[['Brand']].drop_duplicates().reset_index(drop=True)
df_brands['BrandID'] = df_brands.index + 1  # Add BrandID
df_brands = df_brands[['BrandID', 'Brand']]

# Step 2: Extract unique models with foreign key BrandID
df_models = df_cars[['Brand', 'Model']].drop_duplicates().reset_index(drop=True)
df_models = pd.merge(df_models, df_brands, on='Brand', how='left')  # Merge with brands to get BrandID
df_models['ModelID'] = df_models.index + 1  # Add ModelID
df_models = df_models[['ModelID', 'Model', 'BrandID']]

# Step 3: Extract unique derivatives with foreign key ModelID
df_derivatives = df_cars[['Model', 'Derivative']].drop_duplicates().reset_index(drop=True)
df_derivatives = pd.merge(df_derivatives, df_models, on='Model', how='left')  # Merge with models to get ModelID
df_derivatives['DerivativeID'] = df_derivatives.index + 1  # Add DerivativeID
df_derivatives = df_derivatives[['DerivativeID', 'Derivative', 'ModelID']]

# Step 4: Vehicle Info table with Introduced, Discontinued, and DerivativeID
df_vehicle_info = df_cars[['Derivative', 'Introduced', 'Discontinued']].drop_duplicates().reset_index(drop=True)
df_vehicle_info = pd.merge(df_vehicle_info, df_derivatives, on='Derivative', how='left')  # Merge with derivatives to get DerivativeID
df_vehicle_info = df_vehicle_info[['DerivativeID', 'Introduced', 'Discontinued']]

# Save each table to CSV for review
df_brands.to_csv('/Users/luizcaprio/Desktop/ETL/brands.csv', index=False)
df_models.to_csv('/Users/luizcaprio/Desktop/ETL/models.csv', index=False)
df_derivatives.to_csv('/Users/luizcaprio/Desktop/ETL/derivatives.csv', index=False)
df_vehicle_info.to_csv('/Users/luizcaprio/Desktop/ETL/vehicle_info.csv', index=False)

# Save each table to CSV for review
df_brands.to_csv('/Users/luizcaprio/Desktop/ETL/brands.csv', index=False)
df_models.to_csv('/Users/luizcaprio/Desktop/ETL/models.csv', index=False)
df_derivatives.to_csv('/Users/luizcaprio/Desktop/ETL/derivatives.csv', index=False)
df_vehicle_info.to_csv('/Users/luizcaprio/Desktop/ETL/vehicle_info.csv', index=False)

# Optional: Print out to check in the console
print("Brands Table:")
print(df_brands.head())
print("Models Table:")
print(df_models.head())
print("Derivatives Table:")
print(df_derivatives.head())
print("Vehicle Info Table:")
print(df_vehicle_info.head())
