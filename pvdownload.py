import os
import datetime
import pandas as pd

import pvutils

def create_output_dataframe(
        purview_client, 
        search_df, 
        include_columns, 
        include_classifications, 
        include_contacts,
        include_tags,
        include_glossary
):
    output_df = pd.DataFrame(columns=[        
        'assetId',
        'assetName',
        'entityType', 
        'collectionId',
        'collectionName',
        'assetDescription',
        'qualifiedName',
        'classifications',
        #'schemaClassifications',
        'experts',
        'owners',
        'stewards',
        'labels',
        'glossaryTerms'
    ])
    
    for asset in search_df.iterrows():
        pv_asset = pvutils.get_asset(purview_client, asset['id'])
        
        asset_classifications = ''
        if include_classifications and 'classifications' in pv_asset['entity']:
            for classification in pv_asset['entity']['classifications']:
                asset_classifications += classification['typeName'] + ','
            asset_classifications = asset_classifications[:-1]
        
        asset_experts = ''
        asset_owners = ''
        asset_stewards = ''
        if include_contacts and 'contacts' in pv_asset['entity']:
            experts = pv_asset['entity']['contacts']['Expert'] if 'Expert' in pv_asset['entity']['contacts'] else ''
            for expert in experts:
                asset_experts += expert['id'] + ':' + expert['info'] + ','
            asset_experts = asset_experts[:-1]

            owners = pv_asset['entity']['contacts']['Owner'] if 'Owner' in pv_asset['entity']['contacts'] else ''
            for owner in owners:
                asset_owners += owner['id'] + ':' + owner['info'] + ','
            asset_owners = asset_owners[:-1]

            stewards = pv_asset['entity']['contacts']['Steward'] if 'Steward' in pv_asset['entity']['contacts'] else ''
            for steward in stewards:
                asset_stewards += steward['id'] + ':' + steward['info'] + ','
            asset_stewards = asset_stewards[:-1]

        asset_labels = ''
        if include_tags and 'labels' in pv_asset['entity']:
            for label in pv_asset['entity']['labels']:
                asset_labels += label + ','
            asset_labels = asset_labels[:-1]

        asset_terms = ''
        if include_glossary and 'meanings' in pv_asset['entity']['relationshipAttributes']:
            for term in pv_asset['entity']['relationshipAttributes']['meanings']:
                asset_terms += term['displayText'] + ','
            asset_terms = asset_terms[:-1]

        row_df = pd.DataFrame.from_records([{
            'assetId': pv_asset['entity']['guid'],
            'assetName': pv_asset['entity']['attributes']['name'],
            'entityType': pv_asset['entity']['typeName'],
            'collectionId': pv_asset['entity']['collectionId'],
            'collectionName': None,
            'assetDescription': pv_asset['entity']['attributes']['description'],
            'qualifiedName': pv_asset['entity']['attributes']['qualifiedName'],
            'classifications': asset_classifications,
            #'schemaClassifications': None,
            'experts': asset_experts,
            'owners': asset_owners,
            'stewards': asset_stewards,
            'labels': asset_labels,
            'glossaryTerms': asset_terms
        }])
        output_df = pd.concat([output_df, row_df], ignore_index=True)

    return output_df

def export_data(output_df, download_path):
    #get current timestamp
    fileTS = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    output_file  = f'{download_path}\purview_export_{fileTS}.csv'

    #create directory from download_path if it doesn't exist
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    #save dataframe to file
    pd.DataFrame.to_csv(
        output_df,
        path_or_buf=output_file,
        index=False
    )
