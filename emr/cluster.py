import os


def generate_flow_overrides(cluster_name):
    env_vars = {
        "ENVIRONMENT": os.getenv('ENVIRONMENT'),
        "HADOOP_HEAPSIZE": "2048"
    }

    spark_env_vars = {
        "PYSPARK_PYTHON": "/usr/bin/python3"
    }
    spark_env_vars.update(env_vars)

    return {
        "Name": cluster_name,
        "ReleaseLabel": "emr-6.2.0",
        "Applications": [
            {
                "Name": "Spark"
            },
        ],
        "Configurations": [
            {
                "Classification": "spark-log4j",
                "Properties": {
                    "log4j.rootCategory": "WARN, console"
                }
            },
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": spark_env_vars
                    }
                ],
                "Properties": {}
            },
            {
                "Classification": "hadoop-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": env_vars
                    }
                ],
                "Properties": {}
            },
            {
                "Classification": "yarn-env",
                "Properties": {},
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": env_vars
                    }
                ]
            }
        ],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "r5.2xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Slave nodes",
                    "Market": "SPOT",
                    "InstanceRole": "CORE",
                    "InstanceType": "r5.xlarge",
                    "InstanceCount": 2,
                }
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetId": os.getenv("PUBLIC_SUBNET_ID", "subnet-0f4023c86cdbdf9ba"),
        },
        "BootstrapActions": [{
            "Name": "main_finops_bootstrap",
            "ScriptBootstrapAction": {
                "Path": "s3://{}/bootstrap.sh".format(os.getenv('SPARK_JOBS_BUCKET'))
            }
        }],
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
        "VisibleToAllUsers": True,
        "LogUri": "s3://{}/elasticmapreduce/".format(os.getenv('EMR_LOGS_BUCKET')),
        "ManagedScalingPolicy": {
            'ComputeLimits': {
                'UnitType': 'Instances',
                'MinimumCapacityUnits': 1,
                'MaximumCapacityUnits': 100,
                'MaximumOnDemandCapacityUnits': 10,
                'MaximumCoreCapacityUnits': 35
            }
        },
        "StepConcurrencyLevel": 15
    }
