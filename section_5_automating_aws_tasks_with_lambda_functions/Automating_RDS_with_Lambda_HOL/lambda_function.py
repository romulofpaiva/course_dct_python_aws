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
currency_rates = {"USD": 1, 'CAD': 0.79, 'MXN': 0.05}
database_name = 'rds_hol_db'
secret_store_arn = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
db_cluster_arn = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

# boto3 clients
s3_client  = boto3.client('s3')
rds_client = boto3.client('rds-data')

# configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# function to process each record/row from the CSV file
def process_record(record):
    # parse record columns into variables
    id, company_name, country, city, product_line, item, bill_date, currency, bill_amount = record
    bill_amount = float(bill_amount)
    
    # convert the bill amount to USD using conversion rates
    usd_amount = 0
    rate = currency_rates.get(currency)
    if rate:
        usd_amount = bill_amount * rate
    else:
        # if no conversion rate is found for the currency, log an info message
        logger.warn(f"No conversion rate found for {currency}")

    # prepare SQL statement with placeholders for inserting record into the database
    sql_statement = ("INSERT IGNORE INTO billing_data "
                        "(id, company_name, country, city, product_line, item, bill_date, currency, bill_amount, bill_amount_usd) "
                     "VALUES (:id, :company_name, :country, :city, :product_line, :item, :bill_date, :currency, :bill_amount, :usd_amount)")

    # prepare parameters for SQL statement
    sql_parameters = [
        {'name': 'id', 'value': {'stringValue': id}},
        {'name': 'company_name', 'value': {'stringValue': company_name}},
        {'name': 'country', 'value': {'stringValue': country}},
        {'name': 'city', 'value': {'stringValue': city}},
        {'name': 'product_line', 'value': {'stringValue': product_line}},
        {'name': 'item', 'value': {'stringValue': item}},
        {'name': 'bill_date', 'value': {'stringValue': bill_date}},
        {'name': 'currency', 'value': {'stringValue': currency}},
        {'name': 'bill_amount', 'value': {'doubleValue': bill_amount}},
        {'name': 'usd_amount', 'value': {'doubleValue': usd_amount}}
    ]

    # execute sql statement and log the response
    response = execute_statement(sql_statement, sql_parameters)
    logger.info(f"SQL execution response: {response}")

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
            process_record(record)

        logger.info("Lambda has finished execution.")
    except Exception as e:
        # if an unexpected error occurs, log an error message
        logger.error(f"Unexpected occurred: {e}")

