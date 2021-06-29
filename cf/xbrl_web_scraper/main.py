import base64
from bs4 import BeautifulSoup
import os
import random
import requests
import time
import random
import base64
from google.cloud import storage, pubsub_v1

def scrape_webpage(event: dict, context: google.cloud.functions.Context) -> None:
    """
    Cloud Function to be triggered by Pub/Sub.
    Downloads a link specified as a message attibute and saves to a 
    Google Cloud Storage (GCS) location again specified by a message
    attribute.

    Arguments:
        event (dict): Event payload.
        ---------------------------
         data      
            None/not used
         attributes 
            zip_path:   url of where one given .zip file is saved.
            link_path:  filename for .zip file.
            test_run:   boolean string of whether to trigger unpacker
                        after completion.
        ---------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    """
    # Specify the bucket where .zip files should be saved
    dir_to_save = os.environ['scraped_bucket']

    # Sets up a GCS client to handle the download to GCS.
    storage_client = storage.Client()

    # Check the specified GCS location exists
    try:
        bucket = storage_client.bucket(dir_to_save.split("/")[0])
    except:
        raise ValueError(
        f"The specified directory {dir_to_save} does not exist"
        )

    # Extracts the relevant attributes from the pub/sub message.
    zip_url = event["attributes"]["zip_path"]
    link = event["attributes"]["link_path"]
    test_run = event["attributes"]["test"]

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
    
    # Trigger the unpacker (if it's not a test run)
    if not eval(test_run):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("ons-companies-house-dev", "downloaded_zip_files")
        data = f"Triggering unpacker for {link}".encode("utf-8")
        publisher.publish(
            topic_path, data, zip_path=dir_to_save+"/"+link
        ).result()
