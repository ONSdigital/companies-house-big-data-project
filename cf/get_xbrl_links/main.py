import base64
import os
import requests
import json
from bs4 import BeautifulSoup
import time
import random
from google.cloud import storage, pubsub_v1


def collect_links(event, content):
    """
    Scrapes target web page and sends the links of all
    zip files found to the pub/sub topic 'run_xbrl_web_scraper'.
    Arguments:
        event (dict): Event payload.
        ---------------------------
            data:       None
            attributes: None
        ---------------------------
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    Notes:
        The base_url is needed as the links to the zip files
        are appended to this, not the html url
        
        Example:
        url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
        base_url = "http://download.companieshouse.gov.uk/"
        dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"
    """

    url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
    base_url = "http://download.companieshouse.gov.uk/"
    dir_to_save = os.environ['scraped_bucket']
    
    # Get url data via a get request
    res = requests.get(url)

    status = res.status_code

    # Check if a test run is being done
    test_run = False
    try:
        if "test" in event["attributes"].keys():
            tes_run = eval(event["attributes"]["test"])
    except:
        pass
    
    # If the scrape was successfull, parse the contents
    if status == 200:
        # Set up objects for pub/sub message publishing
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("ons-companies-house-dev", "run_xbrl_web_scraper")

        soup = BeautifulSoup(res.content, "html.parser")
        links = soup.select("li")

        # Convert to string format
        links = [str(link) for link in links]

        # Extract filename from text if there is a downloadable file
        links = [link.split('<a href="')[1].split('">')[0] for link in links if "<a href=" in link]

        # Filter out files that are not zip
        links = [link for link in links if link[-4:] == ".zip"]

        # Set up storage client for zip destination
        storage_client = storage.Client()
        bucket = storage_client.bucket(dir_to_save.split("/")[0])

        print(f"{len(links)} have been scraped from the page.")

        downloads_count = 0

        # Download and save zip files
        for link in links:
            zip_url = base_url + link

            # Sort out GCS formatting
            if "/" in link: 
                link = link.split("/")[-1]
                blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
            else:
                blob = bucket.blob(link)

            # Only download and save a file if it doesn't exist in the directory
            if not blob.exists():
                downloads_count += 1
                data = "Zip file to download: {}".format(link).encode("utf-8")

                # Publish a message to the relevant topic with arguments for which file to download
                publisher.publish(
                    topic_path, data, zip_path=zip_url, link_path=link, test=str(test_run)
                ).result()
                print(f"{link} is being downloaded")
            else:
                print(f"{link} has already been downloaded")
            
            # Sleep for random time to not overwhelm server
            time.sleep((random.random() * 2.0) + 3.0)
        
        # Check at least one file has been downloaded
        if downloads_count == 0:
            raise RuntimeError(
                f"No zip files were downloaded - check a new one is present at {url}"
        )
    else:
        # Report Stackdriver error
        raise RuntimeError(
            f"Could not scrape web page, encountered unexpected status code: {status}"
        )