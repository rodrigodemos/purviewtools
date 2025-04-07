import os
import datetime
import argparse
import pvutils
import pvdownload as pvdown
import pvupdates as pvup
from dotenv import load_dotenv

#Read arguments from command line
parser = argparse.ArgumentParser(
    description='Download, upload or update metadata to/from Purview',
    epilog="Example: python app.py --download --keywords 'sample'"
)
parser.add_argument('--debug', action='store_true')
parser.add_argument('--purview_account', help='Purview account name')
parser.add_argument('--download_path', help='Path to download metadata to')
parser.add_argument('--upload_path', help='Path to upload metadata from')
parser.add_argument('--download', '-d', help='Download metadata from Purview', action='store_true')
parser.add_argument('--upload', '-u', help='Upload metadata to Purview', action='store_true')
parser.add_argument('--update', '-p', help='Update metadata in Purview', action='store_true')
parser.add_argument('--keywords', help='Keywords to search for in Purview')
parser.add_argument('--filter', '-f', help='Filter to apply to search results')
parser.add_argument('--expand','-e', help='Include additional asset data', action='store_true')
parser.add_argument('--include_columns', help='Include columns with asset data', action='store_true')
parser.add_argument('--include_classifications', help='Include classifications with asset data', action='store_true')
parser.add_argument('--include_contacts', help='Include contacts with asset data', action='store_true')
parser.add_argument('--include_tags', help='Include tags with asset data', action='store_true')
parser.add_argument('--include_glossary', help='Include glossary terms with asset data', action='store_true')
parser.add_argument('--include_fabricInfo', help='Include info related to MS Fabric with asset data', action='store_true')
args = parser.parse_args()

# Use info from arguments if available, otherwise use environment variables
load_dotenv()
purview_account = args.purview_account if args.purview_account else os.getenv('PURVIEW_ACCOUNT_NAME')
download_path = args.download_path if args.download_path else os.getenv('DOWNLOAD_DIRECTORY')
upload_path = args.upload_path if args.upload_path else os.getenv('UPLOAD_DIRECTORY')

# Route based on task from arguments
if args.download:
    try:
        print(f'Downloading from account: {purview_account} to path: {download_path}')
        purview_client = pvutils.purview_client(purview_account)
        pv_search_df = pvutils.query_to_dataframe(purview_client, args.keywords, args.filter)

        output_df = pvdown.create_output_dataframe(
            purview_client, 
            pv_search_df,
            args.expand,
            args.include_columns, 
            args.include_classifications, 
            args.include_contacts,
            args.include_tags,
            args.include_glossary,
            args.include_fabricInfo
        )
        
        pvdown.export_data(output_df, download_path)

    except Exception as e:
        print('Something went wrong while downloading metadata')
        print(e)

if args.upload:
    try:
        print(f'Uploading from path: {upload_path} to account: {purview_account}')
        purview_client = pvutils.purview_client(purview_account)
        pvutils.import_data(purview_client, upload_path)
    except Exception as e:
        print('Something went wrong while uploading metadata')
        print(e)

if args.update:
    try:
        print(f'Updating Purview account: {purview_account} with data in file: {upload_path}')
        for file in os.listdir(upload_path):
            filepath = os.path.join(upload_path, file)
            if not file.endswith('.csv'):
                print('Skipping Only CSV files are supported')
                break
            filename = os.path.splitext(file)[0]
            entities, updateType = filename.split('_', 1)
            print(f'Updating {entities}(s) with {updateType}')
            purview_client = pvutils.purview_client(purview_account)
            assets_df = pvup.load_file_to_df(filepath)
            pvup.update_labels(purview_client, assets_df) if updateType == 'labels' else None
            pvup.update_contacts(purview_client, assets_df) if updateType == 'contacts' else None
            
    except Exception as e:
        print('Something went wrong while updating metadata')
        print(e)

