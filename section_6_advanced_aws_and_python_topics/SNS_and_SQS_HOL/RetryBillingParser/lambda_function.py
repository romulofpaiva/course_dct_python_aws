# Import necessary modules.
# CSV for handling CSV files, boto3 for AWS SDK, datetime for date operations.
import csv
import boto3
import re
from datetime import datetime

# Initialize resources
s3 = boto3.resource('s3')
sns = boto3.client('sns')


# make a fake api call to get international taxes 
def get_international_taxes(valid_product_lines, billing_bucket, csv_file):
    # Simulate API Call Success
    print("API Success: International Taxes API called.") 
    return


def lambda_handler(event, context):
    
    # Extract the bucket name and the CSV file name from the 'event' input.
    message = event['Records'][0]['body']
    match = re.search("for '(.*?)' bucket and file '(.*?)'", message)
    if match:
        billing_bucket, csv_file = match.groups()
    else:
        print(f"Error parsing message: {message}")
        return

    # Define the name of the error bucket where you want to copy the erroneous CSV files.
    error_bucket     = 'dct-billing-errors-150179'
    processed_bucket = 'dct-billing-processed-150179'
    
    # Download the CSV file from S3, read the content, decode from bytes to string, and split the content by lines.
    try:
        obj = s3.Object(billing_bucket, csv_file)
        data = obj.get()['Body'].read().decode('utf-8').splitlines()
    except Exception as error:
        # If an error occurs while downloading the CSV file, print an error message and return.
        print(f'Error downloading CSV file {billing_bucket}/{csv_file} from S3. Error: {error}')
        return
    
    # Initialize a flag (error_found) to false. We'll set this flag to true when we find an error.
    error_found = False
    
    # Define valid product lines and valid currencies.
    valid_product_lines = ['Bakery', 'Meat', 'Dairy']
    valid_currencies    = ['USD', 'MXN', 'CAD']
    
    # Call the get_international_taxes function
    try:
        get_international_taxes(valid_product_lines, billing_bucket, csv_file)
    except Exception as error:
        # If an error occurs while calling the get_international_taxes function, print an error message and return.
        print(f'Error calling get_international_taxes function. Error: {error}')
        return

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
        try:
            s3.Object(error_bucket, csv_file).copy_from(CopySource=f'{billing_bucket}/{csv_file}')
            print(f'Error found in {csv_file}. Copied to {error_bucket}.')
        except Exception as error:
            print(f'Error copying CSV file {billing_bucket}/{csv_file} to {error_bucket} from S3. Error: {error}')
            return
        
        try:
            s3.Object(billing_bucket, csv_file).delete()
            print(f'{csv_file} deleted from {billing_bucket}.')
        except Exception as error:
            print(f'Error deleting CSV file {billing_bucket}/{csv_file} from S3. Error: {error}')
            return
        
    # If no erros were found, return success message with status code 200 and a body message indicating that no erros were found
    try:
        s3.Object(processed_bucket, csv_file).copy_from(CopySource=f'{billing_bucket}/{csv_file}')
        print(f'Moved processed file {csv_file} from {billing_bucket} to {processed_bucket}.')
    except Exception as error:
        print(f'Error moving processed file {csv_file} from {billing_bucket} to {processed_bucket}. Error: {error}')
        return

    try:
        s3.Object(billing_bucket, csv_file).delete()
        print(f'Deleted processed file {csv_file} from {billing_bucket}.')
    except Exception as error:
        print(f'Error deleting processed file {csv_file} from {billing_bucket}. Error: {error}')
        return