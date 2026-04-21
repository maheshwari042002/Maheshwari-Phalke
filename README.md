Q1. Python Script:-

import boto3
import pandas as pd

# S3 configuration
bucket_name = "student-etl-yopurnsme"  # Update this with your bucket name
input_file = "sales.csv"  # Input CSV file
output_file = "an/output/filtered_sales.csv"  # Output file location in S3

# Initialize S3 client
s3 = boto3.client('s3')

# Download the CSV file from S3
s3.download_file(bucket_name, input_file, 'sales.csv')

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('sales.csv')

# Filter the records where amount > 1000
filtered_df = df[df['amount'] > 1000]

# Save the filtered data to a new CSV file
filtered_df.to_csv('filtered_sales.csv', index=False)

# Upload the filtered file back to S3 in the 'an/output' folder
s3.upload_file('filtered_sales.csv', bucket_name, output_file)

print("ETL process completed successfully.")

RDS:- Q2. 
CREATE TABLE processed_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    value INT,
    processed_timestamp DATETIME
);


