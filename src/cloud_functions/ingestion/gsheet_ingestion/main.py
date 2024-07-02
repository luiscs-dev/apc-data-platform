import functions_framework
import airbyte as ab
from google.cloud import storage
from google.cloud import secretmanager

PROJECT_ID="data-eng-training-87b25bc6"

def get_secret(secret_id, version_id="latest"):
    """
    Access the secret from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def read_gsheet(spreadsheet_id, tab_name):
    """
    Read the Google Sheet.
    """
    source: ab.Source = ab.get_source('source-google-sheets')

    source.set_config(
        config={
            "spreadsheet_id": spreadsheet_id,
            "credentials": {
                "auth_type": "Service",
                "service_account_info": get_secret("apc-gsheet-sa")
            }, 
            "names_conversion": True
        }
    )

    source.check()

    source.select_all_streams()
    read_results: ab.ReadResult = source.read()

    df = read_results[tab_name].to_pandas()
    
    return df 

def save_to_gcs(df, bucket_name, blob_name):
    """
    Save the DataFrame to Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

@functions_framework.http
def ingest(request):
    request_json = request.get_json(silent=True)
    
    df = read_gsheet(request_json["spreadsheet_id"], request_json["tab_name"])
    save_to_gcs(df, request_json["bucket_name"], request_json["file_name"])
    
    return 'Success!'
