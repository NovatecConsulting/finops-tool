import re

# currently just for Ondemand -> later generic solution also for reserved or separate for reserved?
def get_price_per_unit(terms):
    for sku, sku_data in terms.items():
        for d_key, d_value in sku_data["priceDimensions"].items():
            unit = d_value["unit"]
            price_per_unit = d_value["pricePerUnit"]["USD"]

            return {unit : price_per_unit}

# Returns dictionary with the reserved prices, sorted per unit and price
def get_reserved_prices(terms):
    prices = dict()
    upfrontFeeKey = "upfrontFee"

    for sku, sku_data in terms.items():
            purchase_option = sku_data["termAttributes"]["PurchaseOption"]
            contract_length = sku_data["termAttributes"]["LeaseContractLength"]
            upfront_fee = 0.0
            cost_per_hour = 0.0
            unit = None

            for d_key, d_value in sku_data["priceDimensions"].items():
                if d_value["description"] == "Upfront Fee":
                    upfront_fee = d_value["pricePerUnit"]["USD"]
                else:
                    cost_per_hour = d_value["pricePerUnit"]["USD"]
                    unit = d_value["unit"]

            if purchase_option in prices.keys():
                if contract_length in prices[purchase_option].keys():
                    prices[purchase_option][contract_length][unit] = cost_per_hour
                    prices[purchase_option][contract_length][upfrontFeeKey] = upfront_fee
                else:
                    prices[purchase_option][contract_length] = {unit : cost_per_hour, upfrontFeeKey : upfront_fee}
            else:
                prices[purchase_option] = {contract_length : {unit : cost_per_hour, upfrontFeeKey : upfront_fee}}

    return prices

# Returns a dictionary containing all the necessary information for given cpu credit item
def handle_cpu_credits_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    instance_family = product_attributes["instanceFamily"]

    return {instance_family : get_price_per_unit(on_demand_term)}

# Returns a dictionary containing all the necessary information for a given database storage item        
def handle_database_storage_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    usagetype = product_attributes["usagetype"]
    volume_type = product_attributes["volumeType"]
    storage_media = product_attributes["storageMedia"]
    max_volume = product_attributes["maxVolumeSize"]
    min_volume = product_attributes["minVolumeSize"]

    return {usagetype : {"volumeType" : volume_type, "storageMedia" : storage_media, "maxVolume" : max_volume, "minVolume" : min_volume, "costs" : get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for a given provisioned throughput item
def handle_provisioned_throughput_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family
    
    deployment_option = product_attributes["deploymentOption"]

    return {deployment_option :  {"costs": get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for a given iops item
def handle_provisioned_iops_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    group_description = product_attributes["groupDescription"]
    usagetype = product_attributes["usagetype"]
    deployment_option = product_attributes["deploymentOption"]

    return {usagetype : {"groupDescription" : group_description, "deploymentOption" : deployment_option, "costs" : get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for a given storage snapshot item
def handle_storage_snapshot_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    storage_media = product_attributes["storageMedia"]
    deployment_option = product_attributes["deploymentOption"]

    return {storage_media : {"deploymentOption" : deployment_option, "costs": get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for a given performance insights item
def handle_performance_insights_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    instance_type_family = product_attributes["instanceTypeFamily"]

    return {instance_type_family : get_price_per_unit(on_demand_term)}

# Returns a dictionary containing all the necessary information for a given rds proxy item
def handle_rds_proxy_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    return get_price_per_unit(on_demand_term)

# Returns a dictionary containing all the necessary information for a given system operation item
def handle_system_operation_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    group = product_attributes["group"]

    return {group : get_price_per_unit(on_demand_term)}

# Returns a dictionary containing all the necessary information for a given database instance item
def handle_database_instance_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"]  

    usagetype = product_attributes["usagetype"] # solve with this as unique parameter
    instance_type = product_attributes["instanceType"]
    memory = resolve_available_memory(product_attributes["memory"])
    vcpu = int(product_attributes["vcpu"])
    storage = product_attributes["storage"]
    instance_family = product_attributes["instanceFamily"]
    network_performance = product_attributes["networkPerformance"]
    deployment_option = product_attributes["deploymentOption"]

    network_performance = resolve_network_performance(network_performance)

    cpu_val = vcpu * 70 # 70 % cpu_usage work as a buffer for performance peaks

    if "Reserved" in terms.keys():
        reserved_terms = terms["Reserved"]

        return {usagetype : {"instanceType" : instance_type, "memory" : memory, "vcpu" : vcpu, "storage" : storage, "instanceFamily" : instance_family, "networkPerformance" : network_performance, "deploymentOption" : deployment_option, "cpuVal": cpu_val, "costs" : {"OnDemand" : get_price_per_unit(on_demand_term), "Reserved" : get_reserved_prices(reserved_terms)}}}
    else:
        return {usagetype : {"instanceType" : instance_type, "memory" : memory, "vcpu" : vcpu, "storage" : storage, "instanceFamily" : instance_family, "networkPerformance" : network_performance, "deploymentOption" : deployment_option, "cpuVal": cpu_val, "costs" : {"OnDemand" : get_price_per_unit(on_demand_term), "Reserved" : None}}}

# Calculates the score of a given instance, this score makes the instance comparable to other instances
def calculate_instance_score(vcpu, memory, network, vcpu_usage=70): # 70 % vcpu_usage work as a buffer for performance peaks
    vcpu_max = 12800 # %
    memory_max = 4096 # GB
    network_max = 102400 # Mbit/s

    vcpu_score = (int(vcpu) * vcpu_usage) / vcpu_max
    memory_score = float(memory) / memory_max
    network_score = network / network_max

    score = vcpu_score + memory_score + network_score

    return score

# Resolves the network performance given as a string to an int or float, the network performance is returned in Mbit/s
def resolve_network_performance(network):
    if "Moderate" in network:
        return 300
    elif "High" in network:
        return 1024
    elif "Low" in network:
        return 50
    else:
        n_match = re.search(r'\b\d+(\.\d+)?\b', network)
        network = float(n_match.group())
        network = network * 1024

        return network # Mbit / s
    
def resolve_available_memory(memory):
    m_match = re.search(r'\b\d+(\.\d+)?\b', memory)
    memory = float(m_match.group())

    return memory
