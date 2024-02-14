import json
import datetime
import calendar

from aws_cloudwatch_api import ec_cloudwatch_api
from .ec_utils import *

# write method to get product families?
product_families = {"Cache Instance", "ElastiCache Serverless", "Amazon ElastiCache Global Datastore", "Storage Snapshot"}
price_dict = None

# Returns the deployment option as a string
def get_deployment_option(deployment_option):
    if deployment_option:
        deployment_option = "Multi-AZ"
    else:
        deployment_option = "Single-AZ"

    return deployment_option

def get_snapshot_storage_price():
    return price_dict["Storage Snapshot"]["Amazon S3"]["costs"]["GB-Mo"]

# Returns the price per hour of a given instance
def get_cluster_instance_price(instance, outpost, term):
    pf = "Cache Instance"
    filtered_keys = list()

    if outpost:
        filtered_keys = list(filter(lambda k: k.find(instance) != -1 and k.find("Outpost") != -1, price_dict[pf].keys())) 
    else:
        filtered_keys = list(filter(lambda k: k.find(instance) != -1 and k.find("Outpost") == -1, price_dict[pf].keys()))

    for key in filtered_keys:
        if term == "OnDemand":
            return price_dict[pf][key]["costs"][term]["Hrs"]
        else:
            pass # add more staff later here, after clarifying procedure

    return 0

# Returns a dictionary containing all the pricing information of given clusters
def calculate_ec_prices(clusters, enterprise_discount, ec_client):
    prices = dict()
    total_month = 0
    total_current = 0

    now = datetime.datetime.now()
    total_days_in_month = calendar.monthrange(now.year, now.month)[1]
    total_hours_in_month = total_days_in_month * 24
    current_hours_of_month = (now.day - 1) * 24 + now.hour

    for cluster in clusters:
        cluster_instance = clusters[cluster]["cacheNodeType"]
        outpost = clusters[cluster]["outpost"]
        snapshot_retention_period = clusters[cluster]["snapshotRetentionPeriod"]

        cluster_price = float(get_cluster_instance_price(cluster_instance, outpost, clusters[cluster]["term"]))
        snapshot_price = float(get_snapshot_storage_price())

        cluster_final = cluster_price * total_hours_in_month
        snapshot_final = snapshot_price * ec_cloudwatch_api.get_snapshot_storage(ec_client, cluster)

        cluster_month = (cluster_final + snapshot_final) * (1 - enterprise_discount)
        cluster_current = (cluster_price * current_hours_of_month * (1 - enterprise_discount))

        cluster_month = round(cluster_month, 2)
        cluster_current = round(cluster_current, 2)

        prices[cluster] = {"month": cluster_month, "current": cluster_current}
        total_month += cluster_month
        total_current += cluster_current
    
    prices["totalMonth"] = round(total_month, 2)
    prices["totalCurrent"] = round(total_current, 2)

    return prices

# Returns a dictionary containing all the specs of a given cluster type
def return_cluster_instance_item(cluster_type, outpost, term, term_length=None):
    pf = "Cache Instance"

    filtered_keys = list()
    # filtered_keys = list(filter(lambda k: k.find(cluster_type) != -1, price_dict[pf].keys()))
    if outpost:
        filtered_keys = list(filter(lambda k: k.find(cluster_type) != -1 and k.find("Outpost") != -1, price_dict[pf].keys())) 
    else:
        filtered_keys = list(filter(lambda k: k.find(cluster_type) != -1 and k.find("Outpost") == -1, price_dict[pf].keys()))

    for key in filtered_keys:   
        return price_dict[pf][key]

# Returns a dictionary containing monthly cost forecast for given cluster
def calculate_cluster_monthly_price(cluster):
    pf = "Cache Instance"

    now = datetime.datetime.now()
    total_days_in_month = calendar.monthrange(now.year, now.month)[1]
    hours_in_month = total_days_in_month * 24
    on_demand_costs = float(price_dict[pf][cluster]["costs"]["OnDemand"]["Hrs"]) * hours_in_month

    if price_dict[pf][cluster]["costs"]["Reserved"] != None:
        if "Heavy Utilization" in price_dict[pf][cluster]["costs"]["Reserved"].keys():
            hu_one_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["Heavy Utilization"]["1yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][cluster]["costs"]["Reserved"]["Heavy Utilization"]["1yr"]["upfrontFee"]) / 12)
            hu_three_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["Heavy Utilization"]["3yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][cluster]["costs"]["Reserved"]["Heavy Utilization"]["3yr"]["upfrontFee"]) / 12)

            return {"OnDemand": on_demand_costs, "Reserved": {"Heavy Utilization" : {"1yr": hu_one_costs, "3yr" : hu_three_costs}}}
        else:
            reserved_nu_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["No Upfront"]["1yr"]["Hrs"]) * hours_in_month # no upfront
            reserved_pu_one_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["Partial Upfront"]["1yr"]["Hrs"]) * hours_in_month +  (float(price_dict[pf][cluster]["costs"]["Reserved"]["Partial Upfront"]["1yr"]["upfrontFee"]) / 12)# partial upfront 1 year
            reserved_pu_three_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["Partial Upfront"]["3yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][cluster]["costs"]["Reserved"]["Partial Upfront"]["3yr"]["upfrontFee"]) / 36) # partial upfront 3 years
            reserved_au_one_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["All Upfront"]["1yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][cluster]["costs"]["Reserved"]["All Upfront"]["1yr"]["upfrontFee"]) / 12) # all upfront 1 year
            reserved_au_three_costs = float(price_dict[pf][cluster]["costs"]["Reserved"]["All Upfront"]["3yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][cluster]["costs"]["Reserved"]["All Upfront"]["3yr"]["upfrontFee"]) / 36) # all upfront 3 years

            return {"OnDemand": on_demand_costs, "Reserved": {"NoUpfront" : reserved_nu_costs, "PartialUpfront": {"1yr" : reserved_pu_one_costs, "3yr" : reserved_pu_three_costs}, "AllUpfront": {"1yr": reserved_au_one_costs, "3yr": reserved_au_three_costs}}}
    return {"OnDemand": on_demand_costs}

# Returns a dictionary with possible clusters that are cheaper than given cluster
def get_possible_clusters(memory, cpu_val, network_performance, outpost, costs):
    pf = "Cache Instance"

    possible_cluster_keys = list(filter(lambda cluster: price_dict[pf][cluster]["memory"] >= memory
                                     and price_dict[pf][cluster]["cpuVal"] >= cpu_val
                                     and price_dict[pf][cluster]["networkPerformance"] >= network_performance
                                     and price_dict[pf][cluster]["outpost"] == outpost
                                     and price_dict[pf][cluster]["costs"]["OnDemand"]["Hrs"] <= costs,
                                     price_dict[pf].keys()))
    
    possible_candidates = dict()

    for key in possible_cluster_keys:
        cluster_type = price_dict[pf][key]["cacheNodeType"]
        prices = calculate_cluster_monthly_price(key)

        possible_candidates[cluster_type] = {"prices" : prices}

    return possible_candidates

# Returns the complete pricing information of a given AWS service 
def get_price_list(client, service_code, product_family):
    price_list = []

    next_token = None
    while True:
        if next_token:
            response = client.get_products(
                ServiceCode=service_code,
                Filters=[
                    {
                        'Type': 'TERM_MATCH',
                        'Field': 'location',
                        'Value': 'EU (Frankfurt)'
                    },
                    {
                        'Type': 'TERM_MATCH',
                        'Field': 'productFamily',
                        'Value': product_family
                    }
                ],
                NextToken=next_token
            )
        else:
            response = client.get_products(
                ServiceCode=service_code,
                Filters=[
                    {
                        'Type': 'TERM_MATCH',
                        'Field': 'location',
                        'Value': 'EU (Frankfurt)'
                    },
                    {
                        'Type': 'TERM_MATCH',
                        'Field': 'productFamily',
                        'Value': product_family
                    }
                ],
            )

        price_list.extend(response.get("PriceList", []))

        # Check if there are more pages to retrieve
        next_token = response.get("NextToken")
        if not next_token:
            break

    return price_list

# init the price_dict, gets called in __init__.py at module initialization
def init_ec_price_dict(client):
    global price_dict

    price_dict = dict()

    # Handle RDS Service
    for pf in product_families:
        price_dict[pf] = dict() # creating a dictionary for every product family to add items

        price_list = get_price_list(client, "AmazonElastiCache", pf)

        for price_item in price_list:
            price_item = json.loads(price_item) # load json string as json
            current_item = dict() # current item for the respective dictionary key
         
            product_attributes = price_item["product"]["attributes"]
            terms = price_item["terms"]

            if pf == "Cache Instance":
                current_item = handle_cache_instance_item(product_attributes, terms)
            elif pf == "ElastiCache Serverless":
                current_item = handle_elasticache_serverless_item(product_attributes, terms)
            elif pf == "Amazon ElastiCache Global Datastore":
                current_item = handle_amazon_elasticache_global_datastore(product_attributes, terms)
            elif pf == "Storage Snapshot":
                current_item = handle_storage_snapshot_item(product_attributes, terms)

            price_dict[pf].update(current_item)

    return price_dict

# ================
# testing section
# ================
def test():
    global price_dict

    print(price_dict)

    #return price_dict
