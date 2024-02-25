# Import necessary modules.
# CSV for handling CSV files, boto3 for AWS SDK, datetime for date operations.
import csv
import boto3
from datetime import datetime

# Initialize the s3 resource using boto3
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    
    # Extract the bucket name and the CSV file name from the 'event' input.
    billing_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file       = event['Records'][0]['s3']['object']['key']
    
    # Define the name of the error bucket where you want to copy the erroneous CSV files.
    error_bucket   = 'dct-billing-errors-150179'
    
    # Download the CSV file from S3, read the content, decode from bytes to string, and split the content by lines.
    obj = s3.Object(billing_bucket, csv_file)
    data = obj.get()['Body'].read().decode('utf-8').splitlines()
    
    # Initialize a flag (error_found) to false. We'll set this flag to true when we find an error.
    error_found = False
    
    # Define valid product lines and valid currencies.
    valid_product_lines = ['Bakery', 'Meat', 'Dairy']
    valid_currencies    = ['USD', 'MXN', 'CAD']
    
    # Read the CSV content line by line using Python's csv reader. Ignore the header line (data[1:]).
    for row in csv.reader(data[1:], delimiter=','):
        # For each row, extract the date, product line, currency, and bill amount.
        date         = row[6]
        product_line = row[4]
        currency     = row[7]
        bill_amount  = float(row[8])

        # Check if the product line is valid. If not, set the error_found flag to true and print an error message.
        if product_line not in valid_product_lines:
            error_found = True
            print(f'Error in record {row[0]}: invalid product line: {product_line}')

        # Check if the currency is valid. If not, set the error_found flag to true and print an error message.
        if currency not in valid_currencies:
            error_found = True
            print(f'Error in record {row[0]}: invalid currency: {currency}')
            
        # Check if the bill amount is greater than 0. If not, set the error_found flag to true and print an error message.
        if bill_amount <= 0:
            error_found = True
            print(f'Error in record {row[0]}: invalid bill amount: {bill_amount}')        

        # Check if the date is in the correct format ('%Y-%m-%d'). If not, set the error_found flag to true and print an error message.
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            error_found = True
            print(f'Error in record {row[0]}: invalid date format: {date}')
 
    # If errors were found, copy the CSV file to the error bucket and delete it from the original bucket.
    if error_found:
        s3.Object(error_bucket, csv_file).copy_from(CopySource=f'{billing_bucket}/{csv_file}')
        s3.Object(billing_bucket, csv_file).delete()
        print(f'Error found in {csv_file}. Copied to {error_bucket} and deleted from {billing_bucket}.')
        return {
            'statusCode': 400,
            'body': f'Error found in {csv_file}.'
        }

    # If no errors were found, print and return a success message with status code 200.
    print(f'No errors found in {csv_file}.')
    return {
        'statusCode': 200,
        'body': 'No errors found.'
    }