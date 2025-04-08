import os
import datetime
import pandas as pd

import pvutils

def create_output_dataframe(
        purview_client, 
        search_df,
        expand,
        include_columns, 
        include_classifications, 
        include_contacts,
        include_tags,
        include_glossary,
        include_fabricInfo
):
    columns = [
        'assetId',
        'assetName',
        'entityType', 
        'collectionId',
        'collectionName',
        'assetDescription',
        'sensitivityLabelId',
        'qualifiedName'
    ]
    if include_classifications:
        columns.append('classifications')
    if include_contacts:
        columns.extend(['experts', 'owners', 'stewards'])
    if include_tags:
        columns.append('labels')
    if include_glossary:
        columns.append('glossaryTerms')
    if include_fabricInfo:
        columns.extend(['fabricWorkspaceName', 'fabricCapacityName'])

    output_df = pd.DataFrame(columns=columns)
    
    for index, asset in search_df.iterrows():

        row_data = {
            'assetId': asset.get('id', None),
            'assetName': asset.get('name', None),
            'entityType': asset.get('entityType', None),
            'collectionId': asset.get('collectionId', None),
            'assetDescription': None,
            'sensitivityLabelId': asset.get('sensitivityLabelId', None),
            'qualifiedName': asset.get('qualifiedName', None)
        }

        if expand:
            pv_asset = pvutils.get_asset(purview_client, asset['id'])
            
            if include_classifications:
                asset_classifications = ''
                if 'classifications' in pv_asset['entity']:
                    for classification in pv_asset['entity']['classifications']:
                        asset_classifications += classification['typeName'] + ','
                    asset_classifications = asset_classifications[:-1]
                row_data['classifications'] = asset_classifications

            if include_contacts:
                asset_experts = ''
                asset_owners = ''
                asset_stewards = ''
                if 'contacts' in pv_asset['entity']:
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

                row_data['experts'] = asset_experts
                row_data['owners'] = asset_owners
                row_data['stewards'] = asset_stewards

            if include_tags:
                asset_labels = ''
                if 'labels' in pv_asset['entity']:
                    for label in pv_asset['entity']['labels']:
                        asset_labels += label + ','
                    asset_labels = asset_labels[:-1]
                row_data['labels'] = asset_labels

            if include_glossary:
                asset_terms = ''
                if 'meanings' in pv_asset['entity']['relationshipAttributes']:
                    for term in pv_asset['entity']['relationshipAttributes']['meanings']:
                        asset_terms += term['displayText'] + ','
                    asset_terms = asset_terms[:-1]
                row_data['glossaryTerms'] = asset_terms

            if include_fabricInfo:
                fabric_workspaceName = pv_asset['entity']['attributes'].get('workspaceName', None)
                fabric_capacityName = pv_asset['entity']['attributes'].get('dedicatedCapacityName', None)
                contentProviderType = pv_asset['entity']['attributes'].get('contentProviderType', None)
                reportType = pv_asset['entity']['attributes'].get('reportType', None)
                modifiedBy = pv_asset['entity']['attributes'].get('modifiedBy', None)
                configuredBy = pv_asset['entity']['attributes'].get('configuredBy', None)

                row_data['fabricWorkspaceName'] = fabric_workspaceName
                row_data['fabricCapacityName'] = fabric_capacityName
                row_data['contentProviderType'] = contentProviderType
                row_data['reportType'] = reportType
                row_data['modifiedBy'] = modifiedBy
                row_data['configuredBy'] = configuredBy

        row_df = pd.DataFrame.from_records([row_data])
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

    print(f'Exported {len(output_df.index)} records to {output_file}')
