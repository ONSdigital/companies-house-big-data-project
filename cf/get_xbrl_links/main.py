import base64
from bs4 import BeautifulSoup
import json
import os
import random
import requests
import time

from google.cloud import storage, pubsub_v1


def collect_links(event: dict, context: google.cloud.functions.Context) -> None:
    """
    Cloud Function to be triggered by Pub/Sub.
    Scrapes target web page and sends the links of all
    zip files found to the pub/sub topic 'run_xbrl_web_scraper'.
    Arguments:
        event (dict): The dictionary with data specific to this type
                      of event (the event payload).
        context (google.cloud.functions.Context): The Cloud Functions
                      event metadata (the triggering event).
    Trigger:
        {get_xbrl_downloads}: Triggers the cloud function 'get_xbrl_links'.
    Returns:
        None
    Raises:
        None
    Notes:
        The base_url is needed as the links to the zip files
        are appended to this, not the html url.
        
        Example:
        url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
        base_url = "http://download.companieshouse.gov.uk/"
        dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"
    """
    url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
    base_url = "http://download.companieshouse.gov.uk/"
    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data"
    
    # Makes a request to the url.
    res = requests.get(url)
    
    # Returns the status of the request.
    status = res.status_code
    #txt = res.text
    
    # If the scrape was successful (status 200), the contents are parsed.
    if status == 200:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("ons-companies-house-dev", "run_xbrl_web_scraper")

#       # ...
        soup = BeautifulSoup(res.content, "html.parser")
        links = soup.select("li")

        # Converts links to string format.
        links = [str(link) for link in links]

        # Extracts the filename from text if there is a downloadable file.
        links = [link.split('<a href="')[1].split('">')[0] for link in links if "<a href=" in link]

        # Filters out files that are not a zip file.
        links = [link for link in links if link[-4:] == ".zip"]

        # Creates a storage bucket.
        storage_client = storage.Client()
        bucket = storage_client.bucket(dir_to_save.split("/")[0])

        print(f"{len(links)} have been scraped from the page.")
        
        # Downloads and saves the zip files.
        for link in links:

            zip_url = base_url + link

            if "/" in link: 
                link = link.split("/")[-1]
                blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
            else:
                blob = bucket.blob(link)

            # Only downloads and saves a file if it doesn't already exist in the directory.
            if not blob.exists():
                data = "Zip file to download: {}".format(link).encode("utf-8")

                # Publishes a message to the relevant topic with arguments for which
                # file/s to download.
                future = publisher.publish(
                    topic_path, data, zip_path=zip_url, link_path=link
                )
                print(f"{link} is being downloaded")
            else:
                print(f"{link} has already been downloaded")
            
#           # ...
            time.sleep((random.random() * 2.0) + 3.0)
    else:
        # Report Stackdriver error
        raise RuntimeError(
            f"Could not scrape web page, encountered unexpected status code: {status}"
        )
