from google.cloud import bigquery, storage, pubsub_v1
import google.cloud.logging as gc_logs
import pandas as pd
import gcsfs
import time
import datetime as dt
import pytz

def check_parser(event, content):
    """
    Checks parser has run successfully and calls export_csv if
    so.
    Parses a given list of files (as pub/sub message data) and
    saves the result to a BigQuery table and as a .csv

    Arguments:
        event (dict): Event payload.
        ----------------------------
        data
            None/not used
        attributes
            retry_count: Number of function retries before this is triggered.
        ----------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    """
    # Extract and set up retry arguments
    retry_count = int(event["attributes"]["retry_count"])
    
    max_retries = 2
    retry_wait = 300
    
    # If we have exceeded the max retries raise a runtime error. (This does not 
    # affect the usual cloud functions retry as we handle retries manually)
    if retry_count > max_retries:
        raise RuntimeError(
            f"bq_table_to_csv has been retried {max_retries} times and has still failed to execute."
        )
    # Add a time delay between retries.
    if retry_count > 0:
        time.sleep(retry_wait)
    
    utc=pytz.UTC
    client = gc_logs.Client()
    
    # find log of web scraper - extract file name
    scraper_log_query = f"""
    resource.type = "cloud_function"
    resource.labels.function_name = "xbrl_web_scraper"
    resource.labels.region = "europe-west2"
    textPayload:"Saving zip file"
    """
    scraper_log_entry = client.list_entries(filter_=scraper_log_query, order_by=gc_logs.DESCENDING)
    # Find last log entry
    scraper_last_entry = next(scraper_log_entry)

    # Extract relevant variables from the log entry
    payload = scraper_last_entry.payload
    file_name = payload[16:-7]
    bq_table_name = file_name[22:-4] + "-" + file_name[-4:]
    scraper_timestamp = scraper_last_entry.timestamp

    # Find log of get_xbrl_files_to_unpack to determine number of files extracted
    unpack_log_query = f"""
    resource.type = "cloud_function"
    resource.labels.function_name = "get_xbrl_files_to_unpack"
    resource.labels.region = "europe-west2"
    textPayload:"Unpacking"
    """
    unpack_log_entry = client.list_entries(filter_=unpack_log_query, order_by=gc_logs.DESCENDING)
    # Find last log entry
    unpack_last_entry = next(unpack_log_entry)

    no_files_unzipped = int(unpack_last_entry.payload.split(" ")[1])

    # Query BQ table to check number of files parsed
    bq_database = "ons-companies-house-dev.xbrl_parsed_data"
    table_id = bq_database+"."+bq_table_name
    # SQL query to find number of files in dataset
    sql_query = """SELECT COUNT(DISTINCT(doc_name)) FROM `{}`""".format(table_id)
    df = pd.read_gbq(sql_query, 
                     dialect='standard')

    files_processed = int(df.iloc[0,0])

    # Compare no of processed files to expected
    error_rate = 0.001

    # If not enough files have been parsed, retry using pub/sub
    if (1 - error_rate)*no_files_unzipped  >= files_processed:
        print(f"The number of files processed is less than 99 percent of the expected ({files_processed} out of {no_files_unzipped}) retrying in {retry_wait} seconds")
        
        # Record number of retries
        retry_count += 1

        # Re-triggger check_parser via pub/sub
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("ons-companies-house-dev", "export_bq_table")
        data = "Delayed retry".encode("utf-8")
        publisher.publish(topic_path, data, retry_count=str(retry_count)).result()
        
        raise RuntimeError("The number of files processed is less than 99 percent of the expected ({} out of {})".format(files_processed,no_files_unzipped))
    
    # Ensure files have been unpacked in last 30 mins
    export_time = utc.localize(dt.datetime.today())
    timeframe = dt.timedelta(minutes=30)

    if export_time > scraper_timestamp + timeframe:
        raise RuntimeError("The xbrl_web_scraper was last ran over 30 mins ago, please rerun and try again.")
    
    # If previous checks have passed, convert table to a csv
    else:
        # Set up GCP file system object
        fs = gcsfs.GCSFileSystem(cache_timeout=0)

        # Define input arguments for export csv
        gcs_location = "ons-companies-house-dev-test-parsed-csv-data/cloud_functions_test"
        csv_name =  file_name[-4:] + "-" + file_name[22:-4] + "_xbrl_data"

        # Retrieve a list of all files at the location specified
        file_list = [filename.split("/")[-1] for filename in fs.ls(gcs_location)]

        # Raise an error if the csv file already exists
        if csv_name in file_list:
            raise RuntimeError("The csv file " + csv_name + " already exists at the location specified ( " + gcs_location + " )")
        
        else:
            export_csv(table_id, gcs_location, csv_name)
        

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
