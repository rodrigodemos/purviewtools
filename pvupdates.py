import pandas as pd

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
        experts = asset['experts'].split(',') if not isinstance(asset['experts'], float) else []
        owners = asset['owners'].split(',') if not isinstance(asset['owners'], float) else []
        #stewards = asset['stewards'].split(',') if not isinstance(asset['stewards'], float) else []
        #to-do: update entity