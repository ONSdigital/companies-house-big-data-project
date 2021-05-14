import base64
import gcsfs
import os
from google.cloud import pubsub_v1
from google.cloud import bigquery


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
    print(message_id)

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


def batch_files(event, context):
    """
    Batch a list of unpacked files to be parsed by a cf
    triggered by pub/sub.

    Arguments:
        event (dict): Event payload.
        ----------------------------
        data
            None/not used
        attributes
            xbrl_directory: GCS location where unpacked files are saved.
        ----------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    """
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Retrieve and set necessary variables
    xbrl_directory = event["attributes"]["xbrl_directory"]

    project = "ons-companies-house-dev"
    bq_location = "xbrl_parsed_data"
    csv_location = "ons-companies-house-dev-xbrl-parsed-data"

    all_files = [file.split("/")[-1] for file in fs.ls(xbrl_directory)]

    # Set the batch size
    n = 200

    # Extract the relevant date information from the directory name
    folder_month = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[0:-4]
    folder_year = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[-4:]

    # Define the location where to export results to BigQuery
    table_export = project + "." + bq_location + "." + folder_month + "-" + folder_year

    # Create a BigQuery table
    mk_bq_table(table_export)

    # Batch the filenames in a list of lists of size n
    batched_files = [all_files[i*n : (i+1)*n] for i in range((len(all_files) + n - 1)//n)]

    # Configure publisher settings and create a client
    print(f"Parsing files in {len(batched_files)} batches")
    ps_batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
    )
    publisher = pubsub_v1.PublisherClient(batch_settings=ps_batching_settings)
    topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_parser_batches")
    
    # Trigger the xbrl_parser for each batch of files
    for batch in batched_files:
        data = str(batch).encode("utf-8")
        future = publisher.publish(
            topic_path, data, xbrl_directory=xbrl_directory, table_export=table_export, csv_location=csv_location
        )
        future.add_done_callback(callback)

    return None

