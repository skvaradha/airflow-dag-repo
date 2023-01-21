import json

default_instance = {'InstanceGroups': [
    {
        'Name': 'Master node',
        'Market': 'ON_DEMAND',
        'InstanceRole': 'MASTER',
        'InstanceType': 'm5.2xlarge',
        'InstanceCount': 1,
    },
    {
        'Name': "Slave nodes",
        'Market': 'ON_DEMAND',
        'InstanceRole': 'CORE',
        'InstanceType': 'm5.4xlarge',
        'InstanceCount': 4,
    }
]}


def get_emr_instance_type(size_in_gb: float, job_override):
    if 0.0 < size_in_gb <= 5.0:
        return job_override
    elif 5.0 < size_in_gb <= 10.0:
        default_instance['InstanceGroups'][1]['InstanceCount'] = 6
        default_instance['InstanceGroups'][1]['InstanceType'] = 'm5.8xlarge'
        job_override['Instances']['InstanceGroups'] = default_instance['InstanceGroups']
        return job_override
    else:
        default_instance['InstanceGroups'][1]['InstanceType'] = 'm5.8xlarge'
        default_instance['InstanceGroups'][1]['InstanceCount'] = 6
        job_override['Instances']['InstanceGroups'] = default_instance['InstanceGroups']
        return job_override


def create_path_list(customer, year_month_pairs):
    prefix_list = []
    year_month_list = year_month_pairs.split(' ')

    for year_month in year_month_list:
        year = year_month.split('_')[0]
        month = year_month.split('_')[-1]
        prefix = f'customer-cur/customer={customer}/year={year}/month={month}/'
        prefix_list.append(prefix)

    return ','.join(prefix_list)
