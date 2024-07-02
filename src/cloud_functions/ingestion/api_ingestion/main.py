import functions_framework
from google.cloud import storage
from google.cloud import secretmanager
import requests
import json

PROJECT_ID="data-eng-training-87b25bc6"


def save_to_gcs(data, bucket_name, blob_name):
    """
    Save the DataFrame to Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)

def get_data_from_api(url):
    r = requests.get(url)
    
    return r.content.decode('utf-8')


@functions_framework.http
def ingest(request):
    request_json = request.get_json(silent=True)
    
    data = get_data_from_api(request_json["url"])
    print("data")
    print(data)
    
    save_to_gcs(data, request_json["bucket_name"], request_json["file_name"])
    
    
    return 'Success!'
