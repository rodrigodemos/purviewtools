import pandas as pd

import pvutils

def load_file_to_df(file_path):
    df = pd.read_csv(file_path)
    return df

def update_labels(purview_client, assets_df):
    for index, asset in assets_df.iterrows():
        asset_id = asset['assetId']
        labels = asset['labels'].split(',') if not isinstance(asset['labels'], float) else []
        purview_client.entity.set_labels(asset_id, labels)

def update_contacts(purview_client, assets_df):
    for index, asset in assets_df.iterrows():
        asset_id = asset['assetId']
        pv_asset = pvutils.get_asset(purview_client, asset_id)
        experts = asset['experts'].split(',') if not isinstance(asset['experts'], float) else []
        owners = asset['owners'].split(',') if not isinstance(asset['owners'], float) else []

        experts_array = [{"id": expert.split(':')[0], "info": expert.split(':')[1] if len(expert.split(':')) > 1 else ''} for expert in experts]
        owners_array = [{"id": owner.split(':')[0], "info": owner.split(':')[1] if len(owner.split(':')) > 1 else ''} for owner in owners]
        #stewards = asset['stewards'].split(',') if not isinstance(asset['stewards'], float) else []

        updated_entity = {
            "referredEntities": {},
            "entity": {
                "typeName": pv_asset['entity']['typeName'],
                "attributes": pv_asset['entity']['attributes'],
                "contacts": {
                    "Expert": experts_array,
                    "Owner": owners_array
                }
            }
        }

        purview_client.entity.create_or_update(updated_entity)