import base64
import gcsfs

from xbrl_parser import XbrlParser

def parse_batch(event, context):
    """
    Parses a given list of files (as pub/sub message data) and
    saves the result to a BigQuery table and as a .csv

    Arguments:
        event (dict): Event payload.
        ----------------------------
        data
            list of filenames to be parsed
        attributes
            xbrl_directory: Location of unpacked files.
            table_export:   BigQuery table to upload parsed data to.
        ----------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    """
    # Create parser class instance
    parser = XbrlParser()

    # Extract list of files from message data
    files = eval(base64.b64decode(event['data']).decode('utf-8'))

    # Obtain the relevant attributes from the pub/sub message
    xbrl_directory = event["attributes"]["xbrl_directory"]
    table_export = event["attributes"]["table_export"]

    # Parse the batch of files
    parser.parse_files(files, xbrl_directory, table_export)

    return None

