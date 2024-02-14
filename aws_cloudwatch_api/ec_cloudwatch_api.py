from datetime import datetime, timedelta

# Returns a dictionary containing all the clusters in given account
def get_ec_cache_clusters(client):
    clusters = dict()
    response = client.describe_cache_clusters()
    term = "OnDemand"

    for cache_cluster in response["CacheClusters"]:
        cache_cluster_id = cache_cluster["CacheClusterId"]
        cache_node_type = cache_cluster["CacheNodeType"]
        engine = cache_cluster["Engine"]
        engine_version = cache_cluster["EngineVersion"]
        network_type = cache_cluster["NetworkType"]
        outpost = False
        snapshot_retention_period = cache_cluster["SnapshotRetentionLimit"]

        if "PreferredOutpostArn" in cache_cluster:
            outpost = True

        clusters[cache_cluster_id] = {"cacheNodeType": cache_node_type, "engine": engine, "engineVersion": engine_version, "networkType": network_type, "outpost": outpost, "snapshotRetentionPeriod": snapshot_retention_period, "term": term}

    return clusters

# how to fix  this?
def get_ec_cache_reserved_nodes(client):
    return 0

# Returns the allocated storage snapshot of a  given cluster
def get_snapshot_storage(client, cluster_identifier):
    response = client.describe_snapshots(
        CacheClusterId=cluster_identifier
    )

    try:
        return response["Snapshots"][-1]["AllocatedStorage"] # always take the latest allocated storage in the snapshot
    except Exception as e:
        print(f"No Snapshot available for {cluster_identifier}!")

    return 0

# Returns EC metrics for a given cluster, metric, period, time frame, statistic and unit can be passed to the method
def get_metrics(client, cluster_identifier, metric_name, start_time, end_time, period, statistic, unit):
    namespace = "AWS/ElastiCache"
    dimensions = [
        {
            "Name": "CacheClusterId",
            "Value": cluster_identifier
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

# Returns the cpu usage of given cluster
def get_cpu_usage(client, cluster_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints = get_metrics(client, cluster_identifier, "CPUUtilization", start_time, end_time, 3600, "Maximum", "Percent")

    maximum = 0
    for datapoint in datapoints:
        if maximum < datapoint["Maximum"]:
            maximum = datapoint["Maximum"]

    return round(maximum, 2)

# Returns memory usage of given cluster
def get_memory_usage(client, cluster_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints = get_metrics(client, cluster_identifier, "FreeableMemory", start_time, end_time, 3600, "Maximum", "Bytes")

    maximum = 0
    for datapoint in datapoints:
        if maximum < datapoint["Maximum"]:
            maximum = datapoint["Maximum"]

    maximum_in_gbyte = maximum / 1024 / 1024 / 1024

    return maximum_in_gbyte

# Returns network usage of given cluster
def get_network_usage(client, cluster_identifier):
    start_time = datetime.utcnow() - timedelta(days=7)
    end_time = datetime.utcnow()

    datapoints_transmit = get_metrics(client, cluster_identifier, "NetworkTransmitThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")
    datapoints_receive = get_metrics(client, cluster_identifier, "NetworkReceiveThroughput", start_time, end_time, 3600, "Maximum", "Bytes/Second")

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
