import pandas as pd
import json

from azure.identity import DefaultAzureCredential
from azure.purview.datamap import DataMapClient
from azure.purview.catalog import PurviewCatalogClient

class PurviewClient:
    def __init__(self, purview_account):
        credential = DefaultAzureCredential()
        self.purview_account = purview_account
        self.datamap = DataMapClient(
            endpoint=f'https://{purview_account}.purview.azure.com', 
            credential=credential,
            api_version='2024-03-01-preview',
            logging_enable=True
        )
        self.catalog = PurviewCatalogClient(
            endpoint=f'https://{purview_account}.purview.azure.com', 
            credential=credential,
            logging_enable=True
        )

def create_search_body(keywords, filter):
    if isinstance(filter, str):
        try:
            filter = json.loads(filter)
        except json.JSONDecodeError:
            raise ValueError("Filter must be a valid JSON string.")

    search_body = {
        'keywords': keywords if keywords else None,
        'facets': None,
        'filter': filter if filter else None,
    }
    return search_body

def purview_client(purview_account):
    return PurviewClient(purview_account)

def query_to_dataframe(purview_client, keywords, filter):
    search_request = create_search_body(keywords, filter)
    all_results = []
    continuation_token = None

    while True:
        if continuation_token:
            search_request['continuationToken'] = continuation_token
        purview_search = purview_client.datamap.discovery.query(body=search_request)
        all_results.extend(purview_search['value'])

        continuation_token = purview_search.get('continuationToken')
        if not continuation_token:
            break

    search_df = pd.DataFrame.from_dict(all_results)
    return search_df


def get_asset(purview_client, asset_id):
    asset = purview_client.catalog.entity.get_by_guid(asset_id)
    return asset

def related_entities_to_dataframe(asset):
    related_entities_df = pd.DataFrame.from_dict(asset['referredEntities'])
    return related_entities_df
