import base64
from bs4 import BeautifulSoup
import os
import random
import requests
import time

from google.cloud import storage

def scrape_webpage(event, context): # -> blob?
    """
    Cloud Function to be triggered by Pub/Sub.
    Downloads a link specified as a message attibute and saves to a 
    Google Cloud Storage (GCS) location again specified by a message
    attribute.

    Arguments:
        event (dict): The dictionary with data specific to this type
                      of event (the event payload).
        context (google.cloud.functions.Context): The Cloud Functions
                      event metadata (the triggering event).
    Returns:
        None
    Raises:
        None
    """
    # Specifies the bucket where the .zip files should be saved.
    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data"

    # Sets up a GCS client to handle the download to GCS.
    storage_client = storage.Client()

    # Raises an error if the specified directory (dir_to_save) does not exist.
    try:
        bucket = storage_client.bucket(dir_to_save.split("/")[0])
    except:
        raise ValueError(
        f"The specified directory {dir_to_save} does not exist"
        )

    # Extracts the relevant attributes from the pub/sub message.
    zip_url = event["attributes"]["zip_path"]
    link = event["attributes"]["link_path"]

    # The blob (binary large object) method creates the files. The
    # name of the blob corresponds to the unique path of the object
    # in the bucket.
    if len(dir_to_save.split("/")[1:]) > 0:
        blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
    else:
        blob = bucket.blob(link)
    
    # Makes a request to the zip_url and returns a sequence of bytes.
    print("Downloading " + link + "...")
    zip_file = requests.get(zip_url).content
    
    # Saves the 'zip_files' by uploading the contents to the storage bucket.
    print("Saving zip file " + link + "...")
    blob.upload_from_string(zip_file, content_type="application/zip")
