from google.cloud import bigquery, storage
import google.cloud.logging as gc_logs
import pandas as pd
import gcsfs
import time

def check_parser(event, content):
    
    # Cloud Function input arguments
    bq_table = event["attributes"]["bq_table"]
    gcs_location = event["attributes"]["gcs_location"]
    file_name = event["attributes"]["file_name"]

    n = event["attributes"]["num_files"]
    x = event["attributes"]["num_batches"]
    pipeline_start = event["attributes"]["start_time"]
    retries = event["attributes"]["retries"]

    t0 = time.time()
    batches_parsed = 0

    client = gc_logs.Client()

    while ((time.time() - t0 <= 420) and (batches_parsed <= int(0.99*))):
        log_query = f"""
        resource.type = "cloud_function"
        resource.labels.function_name = "xbrl_parser"
        resource.labels.region = "europe-west2"
        textPayload:"finished with status: 'ok'"
        timestamp>={pipeline_start}
        """
        batches_parsed = len(client.list_entries(filter_=log_query, page_size=x+100))
        time.sleep(5)


def export_csv(bq_table, gcs_location, file_name):
    """
    Takes a specified BigQuery table and saves it as a single csv file
    (creates multiple csvs that partition the table as intermidiate steps)

    Arguments:
        bq_table:       Location of BigQuery table, in form
                        "<dataset>.<table_name>"
        gcs_location:   The folder in gcs where resulting csv should be
                        saved - "gs://" prefix should NOT be included
        file_name:      The name of resulting csv file - ".csv" suffix
                        should NOT be included
    Returns:
        None
    Raises:
        None
    """
    # Set up GCP file system object
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Set up a BigQuery client
    client = bigquery.Client()

    # Don't include table header (will mess up combing csvs otherwise)
    job_config = bigquery.job.ExtractJobConfig(print_header=False, field_delimiter="\t")

    # Extract table into multiple smaller csv files
    extract_job = client.extract_table(
        bq_table,
        "gs://" + gcs_location + "/" + file_name + "*.csv",
        location="europe-west2",
        job_config=job_config
    )
    extract_job.result()

    # Recreate the header as a single df with just the header row
    header = pd.DataFrame(columns=['date', 'name', 'unit', 'value',
                        'doc_name', 'doc_type',
                        'doc_upload_date', 'arc_name', 'parsed',
                        'doc_balancesheetdate',
                        'doc_companieshouseregisterednumber',
                        'doc_standard_type',
                        'doc_standard_date', 'doc_standard_link'],)
    header.to_csv("gs://" + gcs_location + "/header_" + file_name + ".csv",
                    header=True, index=False, sep="\t")

    # Specify the files to be combined
    split_files = [f.split("/", 1)[1] for f in fs.ls(gcs_location)
                    if (f.split("/")[-1]).startswith("header_" + file_name)] +\
                    [f.split("/", 1)[1] for f in fs.ls(gcs_location)
                    if (f.split("/")[-1]).startswith(file_name)]

    # Set up a gcs storage client and locations for things
    storage_client = storage.Client()       
    bucket = storage_client.bucket(gcs_location.split("/",1)[0])
    destination = bucket.blob(gcs_location.split("/", 1)[1] + "/" + file_name + ".csv")
    destination.content_type = "text/csv"

    # Combine all the specified files
    sources = [bucket.get_blob(f) for f in split_files]
    destination.compose(sources)

    # Remove the intermediate files
    fs.rm([f for f in fs.ls(gcs_location)
                if ((f.split("/")[-1]).startswith(file_name + "0")) or
                ((f.split("/")[-1]).startswith("header_" + file_name))
                ])

