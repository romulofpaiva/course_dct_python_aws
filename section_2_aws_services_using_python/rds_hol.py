"""
RDS Client (não tem resource. apenas client)

create_db_cluster()
describe_db_clusters()
modify_db_cluster()
delete_db_cluster()
"""

import boto3
import time

# Instantiate a boto3 client for RDS
rds = boto3.client('rds')

# Define the parameters for the new DB cluster
username = 'dctuser1'
password = '2Lxu1hlT'
db_subnet_group_name = 'subnet-group-hol'
db_cluster_identifier = 'rds-hol-cluster'

# Create the new DB cluster
try:
    response = rds.describe_db_clusters(DBClusterIdentifier=db_cluster_identifier)
    print(f"The DB cluster named {db_cluster_identifier} already exists. Skipping creation.")
except rds.exceptions.DBClusterNotFoundFault:
    response = rds.create_db_cluster(
        DBClusterIdentifier=db_cluster_identifier,
        DBSubnetGroupName=db_subnet_group_name,
        Engine='aurora-mysql',
        EngineVersion='5.7.mysql_aurora.2.08.3',
        EngineMode='serverless',
        EnableHttpEndpoint=True,
        MasterUsername=username,
        MasterUserPassword=password,
        ScalingConfiguration={
            'MinCapacity': 1, # minimum ACU
            'MaxCapacity': 2, # maximum ACU
            'AutoPause': True,
            'SecondsUntilAutoPause': 300 # pause after 5 minutes of inactivity
        }
    )
    print(f"The DB cluster named {db_cluster_identifier} has been created.")

# Wait for the DB cluster to be available
print("Waiting for the DB cluster to be available...")
rds.get_waiter('db_cluster_available').wait(DBClusterIdentifier=db_cluster_identifier)
print("The DB cluster is available.")

# Modify the DB cluster. Update the scaling configuration.
print("Modifying the DB cluster...")
rds.modify_db_cluster(
    DBClusterIdentifier=db_cluster_identifier,
    ScalingConfiguration={
        'MinCapacity': 1,
        'MaxCapacity': 4,
        'AutoPause': True,
        'SecondsUntilAutoPause': 600
    }
)
print("The DB cluster has been modified.")

# Delete the DB cluster
print("Deleting the DB cluster...")
rds.delete_db_cluster(DBClusterIdentifier=db_cluster_identifier, SkipFinalSnapshot=True)
print("The DB cluster has been deleted.")