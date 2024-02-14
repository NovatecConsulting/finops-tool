import json
import datetime
import calendar

from aws_cloudwatch_api import rds_cloudwatch_api
from .rds_utils import *

# write method to get product families?
product_families = {"Database Instance", "Database Storage", "RDSProxy", "CPU Credits", "Provisioned IOPS", "System Operation", "Performance Insights", "Provisioned Throughput", "Storage Snapshot"}
price_dict = None

# Returns the deployment option as a string
def get_deployment_option(deployment_option):
    if deployment_option:
        deployment_option = "Multi-AZ"
    else:
        deployment_option = "Single-AZ"

    return deployment_option

# Returns a dictionary containing all the instances with their specs and cost information
def get_database_instance_price_list(client):
    price_list = get_price_list(client, "AmazonRDS", "Database Instance")
    result = dict()

    for price_item in price_list:
            price_item = json.loads(price_item) # load json string as json
         
            product_attributes = price_item["product"]["attributes"]
            terms = price_item["terms"]

            result.update(handle_database_instance_item(product_attributes, terms))

    return result

# Returns a dictionary of the instance spec with the help of the instance type and deplyoment option
def return_database_instance_item(instance_type, deployment_option, term, term_length=None):
    pf = "Database Instance"
    deployment_option = get_deployment_option(deployment_option)
    keys =  get_resource_keys(instance_type, deployment_option, pf)

    for key in keys:   
        return price_dict[pf][key]

# Returns the hourly price of an OnDemand instance
def get_database_instance_price(instance_type, deployment_option, term, term_length=None):
    pf = "Database Instance"
    dp = "deploymentOption"
    deployment_option = get_deployment_option(deployment_option)
    keys = get_resource_keys(instance_type, deployment_option, pf)

    for key in keys:
        if deployment_option in price_dict[pf][key][dp]:
            if term == "OnDemand":
                return price_dict[pf][key]["costs"][term]["Hrs"]
            else:
                # more stuff for reserved instance
                pass

    return 0

# Returns the price per GB-Mo of given storage type
def get_database_storage_price(storage, deployment_option):
    pf = "Database Storage"
    storage = storage.upper()

    deployment_option = get_deployment_option(deployment_option)

    if storage == "STANDARD": # for magnetic storage
        storage = "Usage"

    keys = get_resource_keys(storage, deployment_option, pf)

    for key in keys:
        return price_dict[pf][key]["costs"]["GB-Mo"]

    return 0

# Returns the IOPS-Mo price for given storage type
def get_provisioned_iops_price(storage, deployment_option):
    pf = "Provisioned IOPS"
    storage = storage.upper()

    deployment_option = get_deployment_option(deployment_option)

    if storage != "GP3":
        storage = ""

    keys = get_resource_keys(storage, deployment_option, pf)

    for key in keys:
        return price_dict[pf][key]["costs"]["IOPS-Mo"]

    return 0

def get_database_backup_storage_price():
    return price_dict["Storage Snapshot"]["AmazonS3"]["costs"]["GB-Mo"]

def get_database_storage_throughput_price(deployment_option):
    deployment_option = get_deployment_option(deployment_option)

    return price_dict["Provisioned Throughput"][deployment_option]["costs"]["MBPS-Mo"]

# Returns a dictionary of the given instances and their current price in the running month as well as a forecast for the running month end costs
def calculate_rds_prices(instances, enterprise_discount, cloudwatch_client, rds_client):
    prices = dict()
    total_month = 0
    total_current = 0

    now = datetime.datetime.now()
    total_days_in_month = calendar.monthrange(now.year, now.month)[1]
    total_hours_in_month = total_days_in_month * 24
    current_hours_of_month = (now.day - 1) * 24 + now.hour

    for instance in instances:
        deployment = instances[instance]["deployment"]
        storage = instances[instance]["storage"]
        storage_type = instances[instance]["storageType"]
        backup = instances[instance]["backup"]
        storage_throughput = instances[instance]["storageThroughput"]
        iops = instances[instance]["iops"]

        # get the prices
        instance_price = float(get_database_instance_price(instances[instance]["class"], deployment, instances[instance]["term"]))
        storage_price = float(get_database_storage_price(storage_type, deployment))
        backup_price = float(get_database_backup_storage_price())
        storage_throughput_price = float(get_database_storage_throughput_price(deployment))
        iops_price = float(get_provisioned_iops_price(storage_type, deployment))

        storage_final = storage * storage_price
        instance_final = instance_price * total_hours_in_month

        storage_throughput_final = storage_throughput * storage_throughput_price
        iops_final = iops * iops_price

        provisioned_storage = rds_cloudwatch_api.get_cloudwatch_provisioned_storage_space(cloudwatch_client, instance, storage)
        snapshot_storage_price = rds_cloudwatch_api.get_snapshot_storage(rds_client, instance) * backup_price

        storage_current = provisioned_storage * storage_price
        instance_current = instance_price * current_hours_of_month

        instance_month = (snapshot_storage_price + instance_final + storage_final + storage_throughput_final + iops_final) * (1 - enterprise_discount)
        instance_current = (snapshot_storage_price + storage_current + instance_current + storage_throughput_final + iops_final) * (1 - enterprise_discount)

        instance_month = round(instance_month, 2)
        instance_current = round(instance_current, 2)
        
        prices[instance] = {"month": instance_month, "current": instance_current}
        total_month += instance_month
        total_current += instance_current

    prices["totalMonth"] = round(total_month, 2)
    prices["totalCurrent"] = round(total_current, 2)

    return prices

# Returns the monthly price without any discounts of a given instance
def calculate_instance_monhtly_price(instance):
    pf = "Database Instance"

    now = datetime.datetime.now()
    total_days_in_month = calendar.monthrange(now.year, now.month)[1]
    hours_in_month = total_days_in_month * 24
    on_demand_costs = float(price_dict[pf][instance]["costs"]["OnDemand"]["Hrs"]) * hours_in_month

    if price_dict[pf][instance]["costs"]["Reserved"] != None:
        reserved_nu_costs = float(price_dict[pf][instance]["costs"]["Reserved"]["No Upfront"]["1yr"]["Hrs"]) * hours_in_month # no upfront
        reserved_pu_one_costs = float(price_dict[pf][instance]["costs"]["Reserved"]["Partial Upfront"]["1yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][instance]["costs"]["Reserved"]["Partial Upfront"]["1yr"]["upfrontFee"]) / 12)# partial upfront 1 year
        reserved_pu_three_costs = float(price_dict[pf][instance]["costs"]["Reserved"]["Partial Upfront"]["3yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][instance]["costs"]["Reserved"]["Partial Upfront"]["3yr"]["upfrontFee"]) / 36) # partial upfront 3 years
        reserved_au_one_costs = float(price_dict[pf][instance]["costs"]["Reserved"]["All Upfront"]["1yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][instance]["costs"]["Reserved"]["All Upfront"]["1yr"]["upfrontFee"]) / 12) # all upfront 1 year
        reserved_au_three_costs = float(price_dict[pf][instance]["costs"]["Reserved"]["All Upfront"]["3yr"]["Hrs"]) * hours_in_month + (float(price_dict[pf][instance]["costs"]["Reserved"]["All Upfront"]["3yr"]["upfrontFee"]) / 36) # all upfront 3 years

        return {"OnDemand": on_demand_costs, "Reserved": {"NoUpfront" : reserved_nu_costs, "PartialUpfront": {"1yr" : reserved_pu_one_costs, "3yr" : reserved_pu_three_costs}, "AllUpfront": {"1yr": reserved_au_one_costs, "3yr": reserved_au_three_costs}}}
    return {"OnDemand": on_demand_costs}

# Returns a dictionary with possible instances that are cheaper than given instance
def get_possible_instances(memory, cpu_val, network_performance, deployment_option, costs, iops=0):
    pf = "Database Instance"
    deployment_option = get_deployment_option(deployment_option)

    possible_instance_keys = list(filter(lambda instance: price_dict[pf][instance]["memory"] >= memory
                                     and price_dict[pf][instance]["cpuVal"] >= cpu_val
                                     and price_dict[pf][instance]["networkPerformance"] >= network_performance
                                     and price_dict[pf][instance]["deploymentOption"] == deployment_option
                                     and price_dict[pf][instance]["costs"]["OnDemand"]["Hrs"] <= costs
                                     and price_dict[pf].get(instance, {}).get("iops", float("inf")) >= iops, # this is how an optional dimension can be added to the filtering
                                     price_dict[pf].keys()))
    
    possible_candidates = dict()

    for key in possible_instance_keys:
        instance_type = price_dict[pf][key]["instanceType"]
        prices = calculate_instance_monhtly_price(key)

        possible_candidates[instance_type] = {"prices" : prices}

    return possible_candidates

# Returns the right dictionary key for a resource type based on resource type, deployment option and product family
def get_resource_keys(resource_type, deployment_option, pf):
    filtered_keys = list()

    if deployment_option == "Single-AZ":
        filtered_keys = list(filter(lambda k: k.find(resource_type) != -1 and k.find("Multi-AZ") == -1, price_dict[pf].keys()))
    else:
        filtered_keys = list(filter(lambda k: k.find(resource_type) != -1 and k.find("Multi-AZ") != -1, price_dict[pf].keys()))

    return filtered_keys

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
                        'Field': 'databaseEngine',
                        'Value': "PostgreSQL"
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
                        'Field': 'databaseEngine',
                        'Value': "PostgreSQL"
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
def init_rds_price_dict(client):
    global price_dict

    price_dict = dict()

    # Handle RDS Service
    for pf in product_families:
        price_dict[pf] = dict() # creating a dictionary for every product family to add items

        price_list = get_price_list(client, "AmazonRDS", pf)

        for price_item in price_list:
            price_item = json.loads(price_item) # load json string as json
            current_item = dict() # current item for the respective dictionary key
         
            product_attributes = price_item["product"]["attributes"]
            terms = price_item["terms"]

            if pf == "CPU Credits":
                current_item = handle_cpu_credits_item(product_attributes, terms)
            elif pf == "Database Storage":
                current_item = handle_database_storage_item(product_attributes, terms)
            elif pf == "Provisioned Throughput":
                current_item = handle_provisioned_throughput_item(product_attributes, terms)
            elif pf == "Provisioned IOPS":
                current_item = handle_provisioned_iops_item(product_attributes, terms)
            elif pf == "Storage Snapshot":
                current_item = handle_storage_snapshot_item(product_attributes, terms)
            elif pf == "Performance Insights":
                current_item = handle_performance_insights_item(product_attributes, terms)
            elif pf == "RDSProxy":
                current_item = handle_rds_proxy_item(product_attributes, terms)
            elif pf == "System Operation":
                current_item = handle_system_operation_item(product_attributes, terms)
            elif pf == "Database Instance":
                current_item = handle_database_instance_item(product_attributes, terms)

            price_dict[pf].update(current_item)

    return price_dict

# ===============
# testing section
# ===============
def get_price_dict():
    global price_dict

    return price_dict

def test():
    global price_dict

    # pf = "Database Instance"
    # for instance in price_dict[pf]:
    #     print(instance)
    #     print(price_dict[pf][instance])

    print(price_dict)

