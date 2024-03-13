"""
This lambda function is triggered wheneve a new billing CSV file is uploaded to the S3 bucket.
The function read the file, convert any billing amount not in USD into USD, and insert the 
data into an Aurora Serverless V1 Database.
"""

# imports
import boto3
import io
import csv
import logging

# constants (database, credentials, currency convertion rates)

# boto3 clients
s3_client = boto3.client('s3')

# configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# function to process each record/row from the CSV file
#def process_record(record):
    # convert the bill amount to USD using conversion rates
    
    # if no conversion rate is found for the currency, log an info message

    # prepare SQL statement with placeholders for inserting record into the database

    # prepare parameters for SQL statement

    # execute sql statement and log the response
    

# function to execute SQL statement
#def execute_sql(sql, params):
    # use RDS Data API to execute the SQL statement

    # if an error occurs while connecting to the database, log an error message


# lambda handler
def lambda_handler(event, context):
    try:
        # get the bucket name and file name from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name   = event['Records'][0]['s3']['object']['key']

        # read the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

        # parse data from the file
        data = response['Body'].read().decode('utf-8')

        # use csv reader to process the CSV data
        csv_reader = csv.reader(io.StringIO(data))
        next(csv_reader) # skip header row

        # process each record in the CSV file
        for record in csv_reader:
            print(record)

        logger.info("Lambda has finished execution.")
    except Exception as e:
        # if an unexpected error occurs, log an error message
        logger.error(f"Unexpected occurred: {e}")

