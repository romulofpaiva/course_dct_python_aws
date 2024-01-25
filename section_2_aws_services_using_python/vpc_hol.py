"""
Create VPC, internet gateway, route table, public route and subnets with Boto3
"""

# Import boto3
import boto3

# Create EC2 client
ec2 = boto3.client('ec2')

# Create VPC if it not exists
def create_vpc():
    vpc_name = 'vpc-hol'

    response = ec2.describe_vpcs(
        Filters=[{'Name': 'tag:Name', 'Values': [vpc_name]}]
    )
    vpcs = response.get('Vpcs', [])

    if vpcs:
        vpc_id = vpcs[0]['VpcId']
        print(f'VPC: {vpc_name} with ID {vpc_id} already exists.')
    else:
        vpc = ec2.create_vpc(CidrBlock='11.0.0.0/16')
        vpc_id = vpc['Vpc']['VpcId']

        # Check if the VPC is available
        vpc_available = ec2.get_waiter('vpc_available')
        vpc_available.wait(VpcIds=[vpc_id])
        print(f'VPC: {vpc_name} with ID {vpc_id} has been created.')

        ec2.create_tags(Resources=[vpc_id], Tags=[{'Key': 'Name', 'Value': vpc_name}])

    return vpc_id


# Create public subnet if it not exists
def create_subnet(subnet_name, az, cidr_block, vpc_id):
    response = ec2.describe_subnets(
        Filters=[{'Name': 'tag:Name', 'Values': [subnet_name]}]
    )
    subnets = response.get('Subnets', [])
    if subnets:
        subnet_id = subnets[0]['SubnetId']
        print(f'Subnet: {subnet_name} with ID {subnet_id} already exists.')
    else:
        subnet = ec2.create_subnet(
            VpcId=vpc_id,
            AvailabilityZone=az,
            CidrBlock=cidr_block
        )
        subnet_id = subnet['Subnet']['SubnetId']
        print(f'Subnet: {subnet_name} with ID {subnet_id} has been created.')

        ec2.create_tags(Resources=[subnet_id], Tags=[{'Key': 'Name', 'Value': subnet_name}])


# Create internet gateway if it not exists
def create_ig(vpc_id):
    ig_name = 'ig-vpc-hol'
    response = ec2.describe_internet_gateways(
        Filters=[{'Name': 'tag:Name', 'Values': [ig_name]}]
    )
    igs = response.get('InternetGateways', [])
    if igs:
        ig_id = igs[0]['InternetGatewayId']
        print(f'Internet Gateway: {ig_name} with ID {ig_id} already exists.')
    else:
        ig = ec2.create_internet_gateway()
        ig_id = ig['InternetGateway']['InternetGatewayId']
        print(f'Internet Gateway: {ig_name} with ID {ig_id} has been created.')

        ec2.create_tags(Resources=[ig_id], Tags=[{'Key': 'Name', 'Value': ig_name}])
        ec2.attach_internet_gateway(VpcId=vpc_id, InternetGatewayId=ig_id)

    return ig_id


def create_route_table(vpc_id, ig_id):
    # Create route table if it not exists
    rt_name = 'rt-vpc-hol'
    response = ec2.describe_route_tables(
        Filters=[{'Name': 'tag:Name', 'Values': [rt_name]}]
    )
    rts = response.get('RouteTables', [])
    if rts:
        rt_id = rts[0]['RouteTableId']
        print(f'Route Table: {rt_name} with ID {rt_id} already exists.')
    else:
        rt = ec2.create_route_table(VpcId=vpc_id)
        rt_id = rt['RouteTable']['RouteTableId']
        print(f'Route Table: {rt_name} with ID {rt_id} has been created.')

        ec2.create_tags(Resources=[rt_id], Tags=[{'Key': 'Name', 'Value': rt_name}])
        ec2.create_route(
            RouteTableId=rt_id,
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=ig_id
        )


def main():
    vpc_id = create_vpc()

    ig_id = create_ig(vpc_id)

    create_route_table(vpc_id, ig_id)

    create_subnet('subnet1-vpc-hol', 'us-east-1a', '11.0.1.0/24', vpc_id)
    create_subnet('subnet2-vpc-hol', 'us-east-1b', '11.0.2.0/24', vpc_id)
    create_subnet('subnet3-vpc-hol', 'us-east-1c', '11.0.3.0/24', vpc_id)


if __name__ == '__main__':
    main()