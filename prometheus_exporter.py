import boto3
import argparse
import http.client
import json
import schedule

from prometheus_client import start_http_server, Gauge
from datetime import datetime

from aws_cloudwatch_api import rds_cloudwatch_api
from aws_cloudwatch_api import ec_cloudwatch_api
from aws_pricing_api import rds_pricing_api
from aws_pricing_api import ec_pricing_api

from aws_pricing_api import initialize_rds_price_dict
from aws_pricing_api import initialize_ec_price_dict

connMattermost = http.client.HTTPSConnection("you_mattermost_domain")
headersMattermost = {
    "Content-Type": "application/json"
}

account_ids = []
role_name = "finops-tool-member-role"
enterprise_discount = 0.34

sts_client = boto3.client("sts", region_name="eu-central-1")
pricing_client = boto3.client("pricing", region_name="eu-central-1")
rds_client = boto3.client("rds", region_name="eu-central-1")
cloudwatch_client = boto3.client("cloudwatch", region_name="eu-central-1")
ec_client = boto3.client("elasticache", region_name="eu-central-1")

# initialization of service pricing dictionaries
initialize_rds_price_dict(pricing_client)
print("Initialized RDS Pricing API Dictionary!")

initialize_ec_price_dict(pricing_client)
print("Initialized EC Pricing API Dictionary!")

# Prometheus Gauges
current_costs = Gauge("current_costs", "Shows the current running costs of the resource", ["resource_name", "account", "service"])
monthly_costs = Gauge("monthly_costs", "Shows the forecast of this month's costs", ["resource_name", "account", "service"])
total_current_costs = Gauge("total_current_costs", "Shows the total current running costs of this service", ["account", "service"])
total_monthly_costs = Gauge("total_monthly_costs", "Shows the total forecast of this month's costs", ["account", "service"])

def collect_ec_metrics(account, ec_client):
    try:
        clusters = ec_cloudwatch_api.get_ec_cache_clusters(ec_client)
        ec_prices = ec_pricing_api.calculate_ec_prices(clusters, enterprise_discount, ec_client)

        for cluster in ec_prices:
            if cluster == "totalMonth" or cluster == "totalCurrent":
                continue

            current_costs.labels(resource_name=cluster, account=account, service="ec").set(ec_prices[cluster]["current"])
            monthly_costs.labels(resource_name=cluster, account=account, service="ec").set(ec_prices[cluster]["month"])

        total_current_costs.labels(account=account, service="ec").set(ec_prices["totalCurrent"])
        total_monthly_costs.labels(account=account, service="ec").set(ec_prices["totalMonth"])
    except Exception as e:
        print(e)
        print("[EC] No entry written, error")

def generate_ec_recommendations(account, ec_client, cloudwatch_client):
    try:
        clusters = ec_cloudwatch_api.get_ec_cache_clusters(ec_client)

        for cluster in clusters:
            cpu_usage = ec_cloudwatch_api.get_cpu_usage(cloudwatch_client, cluster)
            memory_usage = ec_cloudwatch_api.get_memory_usage(cloudwatch_client, cluster)
            network_usage = ec_cloudwatch_api.get_network_usage(cloudwatch_client, cluster)
            outpost = clusters[cluster]["outpost"]

            cluster_definition = ec_pricing_api.return_cluster_instance_item(clusters[cluster]["cacheNodeType"], outpost, "OnDemand")
            cluster_vcpu = cluster_definition["vcpu"]
            cluster_costs = cluster_definition["costs"]["OnDemand"]["Hrs"]
            cpu_val = cluster_vcpu * cpu_usage

            possible_clusters = ec_pricing_api.get_possible_clusters(memory_usage, cpu_val, network_usage, outpost, cluster_costs)
            msg = "#### EC Recommendations FinOps Tool"
            msg += f"\n Account: {account}"
            msg += f"\n Instance: {cluster}"
            msg += "\n Recommendations:"

            for p_cluster in possible_clusters:
                msg += f"\n ##### {p_cluster}"
                msg += f"\n OnDemand monthly costs: {round(possible_clusters[p_cluster]['prices']['OnDemand'], 2)}"

                if possible_clusters[p_cluster]["prices"]["Reserved"] != None:
                    if "Heavy Utilization" in possible_clusters[p_cluster]["prices"]["Reserved"].keys():
                        msg += f"\n Heavy Utilization 1yr monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['Heavy Utilization']['1yr'], 2)}"
                        msg += f"\n Heavy Utilization 3yr monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['Heavy Utilization']['3yr'], 2)}"
                    else:
                        msg += f"\n Reserved (No Upfront) monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['NoUpfront'], 2)}"
                        msg += f"\n Reserved (Partial Upfront, 1yr) monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['PartialUpfront']['1yr'], 2)}"
                        msg += f"\n Reserved (Partial Upfront, 3yr) monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['PartialUpfront']['3yr'], 2)}"
                        msg += f"\n Reserved (All Upfront, 1yr) monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['AllUpfront']['1yr'], 2)}"
                        msg += f"\n Reserved (All Upfront, 3yr) monthly costs: {round(possible_clusters[p_cluster]['prices']['Reserved']['AllUpfront']['3yr'], 2)}"

            send_to_mattermost(msg)
    except Exception as e:
        print(e)
        print(f"[EC] Recommendations could not be generated, error in account: {account}")

def collect_rds_metrics(account, rds_client, cloudwatch_client):
    try:
        instances = rds_cloudwatch_api.get_rds_on_demand_instances(rds_client)
        rds_prices = rds_pricing_api.calculate_rds_prices(instances, enterprise_discount, cloudwatch_client, rds_client)

        for instance in rds_prices:
            if instance == "totalMonth" or instance == "totalCurrent":
                continue

            current_costs.labels(resource_name=instance, account=account, service="rds").set(rds_prices[instance]["current"])
            monthly_costs.labels(resource_name=instance, account=account, service="rds").set(rds_prices[instance]["month"])

        total_current_costs.labels(account=account, service="rds").set(rds_prices["totalCurrent"])
        total_monthly_costs.labels(account=account, service="rds").set(rds_prices["totalMonth"])
    except Exception as e:
        print(e)
        print("[RDS] No entry written, error")

def generate_rds_recommendations(account, rds_client, cloudwatch_client):
    try:
        instances = rds_cloudwatch_api.get_rds_on_demand_instances(rds_client)

        for instance in instances:
            cpu_usage = rds_cloudwatch_api.get_cpu_usage(cloudwatch_client, instance)
            memory_usage = rds_cloudwatch_api.get_memory_usage(cloudwatch_client, instance)
            network_usage = rds_cloudwatch_api.get_network_usage(cloudwatch_client, instance)

            deployment = instances[instance]["deployment"]

            instance_definition = rds_pricing_api.return_database_instance_item(instances[instance]["class"], deployment, "OnDemand")
            instance_vcpu = instance_definition["vcpu"]
            instance_costs = instance_definition["costs"]["OnDemand"]["Hrs"]
            cpu_val = instance_vcpu * cpu_usage

            possible_instances = rds_pricing_api.get_possible_instances(memory_usage, cpu_val, network_usage, deployment, instance_costs)
            msg = "#### RDS Recommendations FinOps Tool"
            msg += f"\n Account: {account}"
            msg += f"\n Instance: {instance}"
            msg += "\n Recommendations:"

            for p_instance in possible_instances:
                msg += f"\n ##### {p_instance}"
                msg += f"\n OnDemand monthly costs: {round(possible_instances[p_instance]['prices']['OnDemand'], 2)}"

                if possible_instances[p_instance]["prices"]["Reserved"] != None:
                    msg += f"\n Reserved (No Upfront) monthly costs: {round(possible_instances[p_instance]['prices']['Reserved']['NoUpfront'], 2)}"
                    msg += f"\n Reserved (Partial Upfront, 1yr) monthly costs: {round(possible_instances[p_instance]['prices']['Reserved']['PartialUpfront']['1yr'], 2)}"
                    msg += f"\n Reserved (Partial Upfront, 3yr) monthly costs: {round(possible_instances[p_instance]['prices']['Reserved']['PartialUpfront']['3yr'], 2)}"
                    msg += f"\n Reserved (All Upfront, 1yr) monthly costs: {round(possible_instances[p_instance]['prices']['Reserved']['AllUpfront']['1yr'], 2)}"
                    msg += f"\n Reserved (All Upfront, 3yr) monthly costs: {round(possible_instances[p_instance]['prices']['Reserved']['AllUpfront']['3yr'], 2)}"

            send_to_mattermost(msg)
    except Exception as e:
        print(e)
        print(f"[RDS] Recommendations could not be generated, error in account: {account}")

def fetch_metrics():
    for account in account_ids:
        print(account)
        assume_session = account_assume_session(account)

        sts_assumed_client = assume_session.client("sts", region_name="eu-central-1")
        rds_assumed_client = assume_session.client("rds", region_name="eu-central-1")
        cloudwatch_assumed_client = assume_session.client("cloudwatch", region_name="eu-central-1")
        ec_assumed_client = assume_session.client("elasticache", region_name="eu-central-1")

        # Check if right role assumed
        response = sts_assumed_client.get_caller_identity()
        print(response["Arn"])

        collect_ec_metrics(account, ec_assumed_client)
        collect_rds_metrics(account, rds_assumed_client, cloudwatch_assumed_client)

def fetch_recommendations():
    for account in account_ids:
        print(account)
        assume_session = account_assume_session(account)

        sts_assumed_client = assume_session.client("sts", region_name="eu-central-1")
        rds_assumed_client = assume_session.client("rds", region_name="eu-central-1")
        cloudwatch_assumed_client = assume_session.client("cloudwatch", region_name="eu-central-1")
        ec_assumed_client = assume_session.client("elasticache", region_name="eu-central-1")

        # Check if right role assumed
        response = sts_assumed_client.get_caller_identity()
        print(response["Arn"])

        generate_ec_recommendations(account, ec_assumed_client, cloudwatch_assumed_client)
        generate_rds_recommendations(account, rds_assumed_client, cloudwatch_assumed_client)

def account_assume_session(account):
    role_arn = f"arn:aws:iam::{account}:role/{role_name}"
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="finops-tool"
    )

    credentials = response["Credentials"]

    assume_session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )

    return assume_session

def send_to_mattermost(msg):
    url = "mattermost_webhook" # enter your webhook
    split_url = url.split('/')
    url_post_to_mattermost = "/" + split_url[len(split_url) - 2] + "/" + split_url[len(split_url) - 1]

    payload = json.dumps({
        "text": msg
    })

    try:
        connMattermost.request("POST", url_post_to_mattermost, payload, headersMattermost)
        res = connMattermost.getresponse()
        print(res.read().decode("utf-8"))
    except Exception as e:
         print("sth went wrong: ", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read CSV File which contains all AWS Account IDs")
    parser.add_argument("input_file", type=argparse.FileType("r"), help="Path to the CSV containing the AWS Account IDs")
    args = parser.parse_args()

    # fetch account IDs
    for account in args.input_file.readlines():
        account_ids.append(account.strip())

    # start server
    start_http_server(8000)

    # append methods to scheduler
    schedule.every().hour.at(":00").do(fetch_metrics)
    schedule.every().day.at("15:10").do(fetch_recommendations)

    # start scheduled methods
    while True:
        schedule.run_pending()
