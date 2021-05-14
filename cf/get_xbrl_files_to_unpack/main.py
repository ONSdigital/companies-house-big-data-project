import base64
import gcsfs
import zipfile
from google.cloud import pubsub_v1, bigquery

def callback(future):
    """
    Function to allow the callback of the publishing 
    of a pub/sub message to be handled outside of the main
    function.

    Arguments
        future: publisher.publish object for publishing a message
                to a topic
    Returns:
        None
    Raises:
        None
    """
    message_id = future.result()


def mk_bq_table(bq_location, schema="parsed_data_schema.txt"):
    """
    Function to create a BigQuery table in a specified location with a
    specified schema.

    Arguments:
        bq_location:    Location of BigQuery table, in form
                        "<project>.<dataset>.<table_name>"
    Returns:
        None
    Raises:
        None
    """
    # Set up a BigQuery client
    client = bigquery.Client()

    # Check if table exists
    try:
        client.get_table(bq_location)
        table_exists = True
    except:
        table_exists = False

    if table_exists:
        raise ValueError("Table already exists, please remove and retry")
    
    # Define the expected schema (for xbrl data)
    schema = schema = [
        bigquery.SchemaField("date", 
                                bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("name",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("unit",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("value",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("doc_name",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("doc_type",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("doc_upload_date",
                                bigquery.enums.SqlTypeNames.TIMESTAMP), 
        bigquery.SchemaField("arc_name",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("parsed",
                                bigquery.enums.SqlTypeNames.BOOLEAN),  
        bigquery.SchemaField("doc_balancesheetdate",
                                bigquery.enums.SqlTypeNames.DATE),                                                                                             
        bigquery.SchemaField("doc_companieshouseregisterednumber",
                                bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("doc_standard_type",
                                bigquery.enums.SqlTypeNames.STRING),                        
        bigquery.SchemaField("doc_standard_date",
                                bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("doc_standard_link",
                                bigquery.enums.SqlTypeNames.STRING)
    ]

    # Create the BigQuery table
    table = bigquery.Table(bq_location, schema=schema)
    table = client.create_table(table)
  
    
def get_xbrl_files(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Sends all files to be unpacked within a .zip file as pubsub messages.
    Args:
        event (dict): Event payload.
        ----------------------------
        data
            None/not used
        attributes
            zip_path:   GCS location of the file to be unpacked.
            test_run:   boolean string of whether to trigger parser
                        after completion.
        ----------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    """
    # Extract desired attributes from the message payload
    zip_path = event["attributes"]["zip_path"]
    bq_location = "xbrl_parsed_data"
    project = "ons-companies-house-dev"
    test_run = event["attributes"]["test"]

    # Create a GCSFS object
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Check the specified directory is valid
    if not fs.exists(zip_path):
        raise ValueError(
            f"Directory {zip_path} does not exist"
    )

    # Specify the directory where unpacked files should be saved
    xbrl_directory = "ons-companies-house-dev-xbrl-unpacked-data/" + (zip_path.split("/")[-1]).split(".")[0]

    # Check the directory to save to doesn't already exist
    if fs.exists(xbrl_directory + "/"):
        raise ValueError(
        f"The directory {xbrl_directory} already exists. Please remove and retry"
    )

    # Extract the relevant date information from the directory name
    folder_month = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[0:-4]
    folder_year = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[-4:]

    # Define the location where to export results to BigQuery
    table_export = project + "." + bq_location + "." + folder_month + "-" + folder_year

    # Create a BigQuery table
    mk_bq_table(table_export)

    # Configure batching settings to optimise publishing efficiency
    batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
    )

    # Create publisher object allowing function to publish messages to the given topic
    publisher = pubsub_v1.PublisherClient(batch_settings=batching_settings)
    topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_files_to_unpack")
    
    # Set the message batch size
    n = 200

    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
        # Compile all files within the .zip and separate them into batches of
        # size n
        zip_list = zip_ref.namelist()
        names = [zip_list[i*n : (i+1)*n] for i in range((len(zip_list) + n - 1)//n)]

        print(f"Unpacking {len(zip_list)} files using batches of size {n}")
        
        # For each batch, publish a message with the list of files to be unpacked as
        # the message data.
        for i, contentfilename in enumerate(names):
            data = str(contentfilename).encode("utf-8")
            future = publisher.publish(
            topic_path, data, xbrl_directory=xbrl_directory, zip_path=zip_path,
            project=project, table_export=table_export, test=test_run
            )
            future.add_done_callback(callback)