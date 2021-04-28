import base64
import gcsfs
import zipfile
import time
import logging
from google.cloud import pubsub_v1

def callback(future):
    message_id = future.result()
    return message_id

def unpack_xbrl_file(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Unpack a list of files specified in the pub/sub message from 
    a given .zip file
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Create a GCSFS object
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Extract relevant variables from the message payload
    xbrl_list = eval(base64.b64decode(event['data']).decode('utf-8'))

    zip_path = event["attributes"]["zip_path"]
    xbrl_directory = event["attributes"]["xbrl_directory"]

    table_export = event["attributes"]["table_export"]
    test_run = eval(event["attributes"]["test"])
    
    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      # For each file listed, download it to the specified location
      for xbrl_path in xbrl_list:
        upload_path = xbrl_directory + "/" + xbrl_path

        # Attempt to read the relevant file to be unpacked
        try:
            content_file = zip_ref.read(xbrl_path)
        except:
            logging.warn(f"Unable to open {xbrl_path}")
            continue

        # Attempt to write the file to the desired location
        try:
            with fs.open(upload_path, 'wb') as f:
                f.write(content_file)
            fs.setxattrs(
                upload_path,
                content_type="text/"+xbrl_path.split(".")[-1]
                )
        except:
            logging.warn(f"Unable to write to {upload_path}")
            continue
    
    if not test_run:
        ps_batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
        )
        publisher = pubsub_v1.PublisherClient(batch_settings=ps_batching_settings)
        topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_parser_batches")
        data = str(xbrl_list).encode("utf-8")
        future = publisher.publish(
            topic_path, data, xbrl_directory=xbrl_directory, table_export=table_export
        )
        future.add_done_callback(callback)
    
    return f"Finished {len(xbrl_list)} files!"

