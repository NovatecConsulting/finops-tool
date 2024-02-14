from aws_pricing_api.rds_pricing_api import init_rds_price_dict
from aws_pricing_api.ec_pricing_api import init_ec_price_dict

def initialize_rds_price_dict(client):
    return init_rds_price_dict(client)

def initialize_ec_price_dict(client):
    return init_ec_price_dict(client)