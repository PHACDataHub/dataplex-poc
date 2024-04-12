from google.cloud import storage

storage_client = storage.Client.from_service_account_json('sa-key.json')

def upload_blob(bucket_name, source_file_name, destination_blob_name):
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


def create_bucket(bucket_name):
    """
    Create a new bucket in the Montreal region if it doesn't alreadyexist
    from https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-python
    """

    try: 
        storage_client.get_bucket(bucket_name)
        print(f'{bucket_name} bucket exists - continuing')
    except:

        bucket = storage_client.bucket(bucket_name)
        # bucket.storage_class = "COLDLINE"
        # new_bucket = storage_client.create_bucket(bucket, location="NORTHAMERICA-NORTHEAST1")
        new_bucket = storage_client.create_bucket(bucket, location="US-CENTRAL1")


        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )
        return new_bucket


def save_to_bucket(file_name, bucket_name):
    create_bucket(bucket_name)
    source_file_name = f'./data/{file_name}'
    destination_blob_name = file_name
    upload_blob(bucket_name, source_file_name, destination_blob_name)