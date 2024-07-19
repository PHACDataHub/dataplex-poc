from google.cloud import storage
import subprocess
from google.oauth2 import service_account
from google.cloud import storage
import google.auth
import os

LOCATION = "NORTHAMERICA-NORTHEAST1"

def upload_blob(bucket_name, source_file_name, destination_blob_name, storage_client):
    """Uploads a file to the bucket.
    from https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-client-libraries"""
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


def create_bucket(bucket_name, storage_client):
    """
    Create a new bucket in the Montreal region if it doesn't alreadye exist
    from https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-python
    """

    try: 
        storage_client.get_bucket(bucket_name)
        print(f'{bucket_name} bucket exists - continuing')
    except:

        bucket = storage_client.bucket(bucket_name)
        new_bucket = storage_client.create_bucket(bucket, location="NORTHAMERICA-NORTHEAST1")


        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )
        return new_bucket


def save_to_bucket(file_name, bucket_name, service_account_key_path):
    # storage_client = storage.Client.from_service_account_json('sa-key.json')
    storage_client = storage.Client.from_service_account_json(service_account_key_path)
    create_bucket(bucket_name, storage_client)
    source_file_name = f'./data/{file_name}'
    destination_blob_name = file_name
    upload_blob(bucket_name, source_file_name, destination_blob_name, storage_client)

# def attach_asset_to_zone(zone_name, bucket_name service_account_key_path):
#     # gcloud dataplex assets create $ASSET_NAME \
#     # --project=$PROJECT_ID \
#     # --location=$LOCATION \
#     # --lake=$LAKE_NAME \
#     # --zone=$ZONE_NAME \
#     # --resource-type=STORAGE_BUCKET \
#     # --resource-name=projects/$PROJECT_ID/buckets/$BUCKET_NAME \
#     # --discovery-enabled 
#     pass

def attach_asset_to_zone(project_id, project_name, zone_name, asset_name, bucket_name, service_account_key_path):
    # asset_name = f"{zone_name}-asset"
    lake_name = project_name

    # authenticate
    # credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
    # client = storage.Client(credentials=credentials)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_key_path
#
    command = [
        'gcloud', 'dataplex', 'assets', 'create', asset_name,
        '--project', project_id,
        '--location', "northamerica-northeast1",
        '--lake', lake_name,
        '--zone', zone_name,
        '--resource-type', 'STORAGE_BUCKET',
        '--resource-name', f'projects/{project_id}/buckets/{bucket_name}',
        '--discovery-enabled'
    ]
    
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"Asset {asset_name} created successfully.")
    else:
        print(f"Failed to create asset {asset_name}.")
        print("Error message:", result.stderr)