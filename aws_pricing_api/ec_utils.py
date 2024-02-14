import re

# Returns a dictionary containing the unit and the price per unit
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

# Returns a dictionary containing all the necessary information for given cache instance item  
def handle_cache_instance_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"]

    instance_type = product_attributes["instanceType"]

    usagetype = product_attributes["usagetype"] # solve with this as unique parameter
    memory = resolve_available_memory(product_attributes["memory"])
    vcpu = int(product_attributes["vcpu"])
    cache_engine = product_attributes["cacheEngine"]
    instance_family = product_attributes["instanceFamily"]
    network_performance = product_attributes["networkPerformance"]
    outpost = False

    if usagetype.find("Outpost") != -1:
        outpost = True

    network_performance = resolve_network_performance(network_performance)

    cpu_val = vcpu * 70 # 70 % cpu_usage work as a buffer for performance peaks

    if "Reserved" in terms.keys():
        reserved_terms = terms["Reserved"]

        return {usagetype : {"cacheNodeType" : instance_type, "memory" : memory, "vcpu" : vcpu, "cacheEngine" : cache_engine, "instanceFamily" : instance_family, "networkPerformance" : network_performance, "outpost": outpost, "cpuVal": cpu_val, "costs" : {"OnDemand" : get_price_per_unit(on_demand_term), "Reserved" : get_reserved_prices(reserved_terms)}}}
    else:
        return {usagetype : {"cacheNodeType" : instance_type, "memory" : memory, "vcpu" : vcpu, "cacheEngine" : cache_engine, "instanceFamily" : instance_family, "networkPerformance" : network_performance, "outpost": outpost, "cpuVal": cpu_val, "costs" : {"OnDemand" : get_price_per_unit(on_demand_term), "Reserved" : None}}}

# Returns a dictionary containing all the necessary information for given elasticache serverless item
def handle_elasticache_serverless_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just OnDemand term in this product family

    usage_type = product_attributes["usagetype"]
    cache_engine = product_attributes["cacheEngine"]

    return {usage_type : {"cacheEngine" : cache_engine, "costs" : get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for given amazon elasticache global datastore item
def handle_amazon_elasticache_global_datastore(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just OnDemand term in this product family

    usage_type = product_attributes["usagetype"]

    return {usage_type : {"costs": get_price_per_unit(on_demand_term)}}

# Returns a dictionary containing all the necessary information for given storage snapshot item
def handle_storage_snapshot_item(product_attributes, terms):
    on_demand_term = terms["OnDemand"] # there is just the OnDemand term in this product family

    storage_media = product_attributes["storageMedia"]

    return {storage_media : {"costs": get_price_per_unit(on_demand_term)}}

# Calculates the score of a given cluster, this score makes the cluster comparable to other clusters
def calculate_cluster_score(vcpu, memory, network, vcpu_usage=70): # 70 % vcpu_usage work as a buffer for performance peaks
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
