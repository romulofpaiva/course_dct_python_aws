"""Manage Elastic IP address, releasing with it's not in use"""

import boto3

# create boto3 ec2 client/resource
ec2_resource = boto3.resource("ec2")

def lambda_handler(event, context):
    # iterate over all elastic ips
    for ip in ec2_resource.vpc_addresses.all():
        # check if in use
        if ip.instance_id is None:
            # release elastic ip
            print(f"Releasing {ip.public_ip}")
            ip.release()
        else:
            print(f"{ip.public_ip} is in use")