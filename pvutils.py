import pandas as pd

from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient

def create_search_body(keywords, filter):
    search_body = {
        'keywords': keywords if keywords else None,
        'facets': None,
        'filter': filter if filter else None,
    }
    return search_body

def purview_client(purview_account):
    credential = DefaultAzureCredential()
    client = PurviewCatalogClient(
        endpoint=f'https://{purview_account}.purview.azure.com', 
        credential=credential,
        logging_enable=True)
    return client

def query_to_dataframe(purview_client, keywords, filter):
    search_request = create_search_body(keywords, filter)
    purview_search = purview_client.discovery.query(search_request=search_request)
    search_df = pd.DataFrame.from_dict(purview_search['value'])
    return search_df

def get_asset(purview_client, asset_id):
    asset = purview_client.entity.get_by_guid(asset_id)
    return asset

def related_entities_to_dataframe(asset):
    related_entities_df = pd.DataFrame.from_dict(asset['referredEntities'])
    return related_entities_df
