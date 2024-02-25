import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    logger.info("Starting lambda...")
        
    current_date = datetime.now().strftime("%Y-%m-%d")

    try:
        ec2 = boto3.client('ec2')

        response = ec2.create_snapshot(
            VolumeId = 'vol-0f1596b80a26dab2d',
            Description='My EC2 Snapshot',
            TagSpecifications = [{
                'ResourceType': 'snapshot',
                'Tags': [{
                    'Key': 'Name',
                    'Value': f"My EC2 snapshot {current_date}"
                }]
                    
            }]
        )
        
        logger.info(f"Successfully created snapshot: {json.dumps(response, default=str)}")
    
    except Exception as e:
        logger.error(f"Error creating snapshot: {str(e)}")
        
    
    logger.info("Finishing lambda...")