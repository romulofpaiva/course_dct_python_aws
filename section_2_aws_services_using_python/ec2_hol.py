"""
This script manages Amazon EC2 instances using the Boto3 library.
"""

# Import the Boto3 library
import boto3

# Create an EC2 resource
ec2 = boto3.resource('ec2')

# List EC2 instances
instances = ec2.instances.all()

# Print the instance IDs, ImageId and State
INSTANCE_NAME = 'DCT-HandOnLesson'
instance_exists = False
instance_id = ""

for instance in instances:
    print(instance.id, instance.image.id, instance.state['Name'])
    for tag in instance.tags:
        if tag['Key'] == 'Name' and tag['Value'] == INSTANCE_NAME:
            instance_exists = True
            instance_id = instance.id
            break;

# Terminate the EC2 instance if it exists
if instance_exists:
    print(f"Instance with name {INSTANCE_NAME} already exists.")
    ec2.Instance(instance_id).terminate()
    print(f"Instance with name {INSTANCE_NAME} has been terminated.")
else:
    # Create a new EC2 instance if it not exists
    new_instance = ec2.create_instances(
        ImageId='ami-0e9107ed11be76fde',
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
        KeyName='north-virginia-key-pair',
        SecurityGroupIds=['sg-09b81ea0620f8c255'],
        SubnetId='subnet-05638d135b4947597',
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': INSTANCE_NAME
                    }
                ]
            }
        ]
    )

# Print the instance IDs, ImageId and State
for instance in instances:
    print(instance.id, instance.image.id, instance.state['Name'])






