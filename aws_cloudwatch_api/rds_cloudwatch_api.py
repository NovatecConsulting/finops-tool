from datetime import datetime, timedelta

# Returns a dictionary of all the OnDemand instances in a given account
def get_rds_on_demand_instances(client):
    instances = dict()
    response = client.describe_db_instances()
    term = "OnDemand"

    # needing to iterate through instances
    for db_instance in response["DBInstances"]:
        db_instance_identifier = db_instance["DBInstanceIdentifier"]      
        db_instance_class = db_instance["DBInstanceClass"] # this for instance type, getting this price category
        allocated_storage = db_instance["AllocatedStorage"] # in GB -> just provisioned storage is billed -> get from cloudwatch!
        deployment_option = db_instance["MultiAZ"] # boolean
        storage_type = db_instance["StorageType"] # for example gp2
        storage_throughput = db_instance["StorageThroughput"]
        network_type = db_instance["NetworkType"]
        iops = 0
        backup_retention_period = db_instance["BackupRetentionPeriod"]
        # max_allocated_storage = db_instance["MaxAllocatedStorage"]

        # print("Max alloc storage", max_allocated_storage)

        if "Iops" in db_instance:
            iops = db_instance["Iops"]

        instances[db_instance_identifier] = {"class" : db_instance_class, "storage": allocated_storage, "storageType": storage_type, "storageThroughput": storage_throughput, "network": network_type, "iops": iops, "deployment" : deployment_option, "backup": backup_retention_period, "term": term}
        # print(db_instance_class, allocated_storage, deployment_option, storage_type, storage_throughput, network_type, iops, backup_retention_period)

        # Pending Modified Values?
    return instances

def get_rds_reserved_instances(client):
    response = client.describe_reserved_db_instances()

    print(response["ReservedDBInstances"])
    return 0

# Returns RDS metrics for a given instance, metric, period, time frame, statistic and unit can be passed to the method
def get_metrics(client, db_instance_identifier, metric_name, start_time, end_time, period, statistic, unit):
    namespace = "AWS/RDS"
    dimensions = [
        {
            "Name": "DBInstanceIdentifier",
            "Value": db_instance_identifier
        }
    ]

    response = client.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=[statistic],
        Unit=unit
    )

    return response["Datapoints"]

# Returns coudwatch provisioned storage space for given instance
def get_cloudwatch_provisioned_storage_space(client, db_instance_identifier, max_storage):
    start_time = datetime.utcnow() - timedelta(minutes=1)
    end_time = datetime.utcnow()

    datapoints = get_metrics(client, db_instance_identifier, "FreeStorageSpace", start_time, end_time, 60, "Average", "Bytes")

    for datapoint in datapoints:
        value = datapoint['Average']
        value = value / 1024 / 1024 / 1024

        return max_storage - value

    return 0

# Returns allocated snapshot storage for given instance
def get_snapshot_storage(client, db_instance_identifier):
    response = client.describe_db_snapshots(
        DBInstanceIdentifier=db_instance_identifier
    )

    try:
        return response["DBSnapshots"][-1]["AllocatedStorage"] # always take the latest allocated storage in the snapshot
    except Exception as e:
        print(f"No Snapshot available for {db_instance_identifier}")

    return 0

# Returns cpu usage of given instance
def get_cpu_usage(client, db_instance_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints = get_metrics(client, db_instance_identifier, "CPUUtilization", start_time, end_time, 3600, "Maximum", "Percent")

    maximum = 0
    for datapoint in datapoints:
        if maximum < datapoint["Maximum"]:
            maximum = datapoint["Maximum"]

    return round(maximum, 2)

# Returns memory usage of given instance
def get_memory_usage(client, db_instance_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints = get_metrics(client, db_instance_identifier, "FreeableMemory", start_time, end_time, 3600, "Maximum", "Bytes")

    maximum = 0
    for datapoint in datapoints:
        if maximum < datapoint["Maximum"]:
            maximum = datapoint["Maximum"]

    maximum_in_gbyte = maximum / 1024 / 1024 / 1024

    return maximum_in_gbyte

# Returns network usage of given instance
def get_network_usage(client, db_instance_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints_transmit = get_metrics(client, db_instance_identifier, "NetworkTransmitThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")
    datapoints_receive = get_metrics(client, db_instance_identifier, "NetworkReceiveThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")

    maximum_transmit = 0
    maximum_receive = 0

    for datapoint in datapoints_transmit:
        if maximum_transmit < datapoint["Maximum"]:
            maximum_transmit = datapoint["Maximum"]

    for datapoint in datapoints_receive:
        if maximum_receive < datapoint["Maximum"]:
            maximum_receive = datapoint["Maximum"]

    network_usage = maximum_transmit + maximum_receive
    network_usage_bits = network_usage * 8
    network_usage = network_usage_bits / 1e6

    return network_usage # in Megabit/s

# Returns iops usage of given instance
def get_iops_usage(client, db_instance_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints_read = get_metrics(client, db_instance_identifier, "ReadIOPS", start_time, end_time, 3600, "Maximum", "Count/Second")
    datapoints_write = get_metrics(client, db_instance_identifier, "WriteIOPS", start_time, end_time, 3600, "Maximum", "Count/Second")

    maximum_read = 0
    maximum_write = 0

    for datapoint in datapoints_read:
        if maximum_read < datapoint["Maximum"]:
            maximum_read = datapoint["Maximum"]

    for datapoint in datapoints_write:
        if maximum_write < datapoint["Maximum"]:
            maximum_write = datapoint["Maximum"]

    iops_usage = maximum_read + maximum_write

    return round(iops_usage, 2)

# Returns throughput usage of given instance
def get_throughput_usage(client, db_instance_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints_read = get_metrics(client, db_instance_identifier, "ReadThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")
    datapoints_write = get_metrics(client, db_instance_identifier, "WriteThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")

    maximum_read = 0
    maximum_write = 0

    for datapoint in datapoints_read:
        if maximum_read < datapoint["Maximum"]:
            maximum_read = datapoint["Maximum"]

    for datapoint in datapoints_write:
        if maximum_write < datapoint["Maximum"]:
            maximum_write = datapoint["Maximum"]

    throughput_usage = maximum_read + maximum_write
    throughput_usage = throughput_usage / 1024 / 1024

    return round(throughput_usage, 2) # Megabyte/Second

# ================
# testing section
# ================
def test(client):
    get_rds_on_demand_instances(client)
    get_rds_reserved_instances(client)

def get_db_clusters(client):
    response = client.describe_db_clusters()

    print(response)
