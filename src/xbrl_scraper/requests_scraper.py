# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 09:59:54 2021

@author: derrip
"""

import os
import requests
from bs4 import BeautifulSoup
import time
import random
from google.cloud import storage
from google.oauth2 import service_account

class XbrlScraper:


    def __init__(self, auth):
        self.project = auth.project
        self.key = auth.xbrl_scraper_key

    def scrape_webpage(self, scraper_url, base_url, dir_to_save):
        """
        Scrapes target web page and saves all zip files found to
        a directory

        Arguments:
            scraper_url:    Url of web page to scrape (str)
            domain:         Base url of wep page to scrape (str)
            dir_to_save:    GCS directory to save zip files to, consisting of bucket_name/folder
        Returns:
            None
        Raises:
            None

        Notes:
            The base url (domain) is needed as the links to the zip files
            are appended to this, not the html url
            
            Example:
            scraper_url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
            domain = "http://download.companieshouse.gov.uk/"
            dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"
        """

        print("Fetching content...")
        res = requests.get(scraper_url)

        #txt = res.text
        status = res.status_code
        
        # If the scrape was successfull, parse the contents
        if status == 200:

            soup = BeautifulSoup(res.content, "html.parser")
            links = soup.select("li")

            # Convert to string format
            links = [str(link) for link in links]

            # Extract filename from text if there is a downloadable file
            links = [link.split('<a href="')[1].split('">')[0] for link in links if "<a href=" in link]

            # Filter out files that are not zip
            links = [link for link in links if link[-4:] == ".zip"]

            #creds = service_account.Credentials.from_service_account_info(self.key)
            #storage_client = storage.Client(credentials=creds)
            #bucket = storage_client.bucket(dir_to_save.split("/")[0])

            # Download and save zip files
            for link in links:

                zip_url = base_url + link

                if "/" in link: link = link.split("/")[-1]

                #blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
                storage_client = storage.Client()
                blob = storage_client.bucket(dir_to_save.split("/")[0])
    
                # Only download and save a file if it doesn't exist in the directory
                if not blob.exists():
                                     
                    print("Downloading " + link + "...")
                    zip_file = requests.get(zip_url).content
                    
                    print("Saving zip file " + link + "...")
                    blob.upload_from_string(zip_file, content_type="application/zip")

                    # Random sleep to avoid stressing the target server
                    time.sleep((random.random() * 2.0) + 3.0)

                else:

                    print(link + " already exists")

            print("All zip files saved")

        else:

            print("Unable to scrape web page!")
            print("Error code: " + status)



if __name__ == "__main__":
    scraper = XbrlScraper()

    url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
    base_url = "http://download.companieshouse.gov.uk/"

    # # url = "http://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html"
    # # base_url = "http://download.companieshouse.gov.uk/"

    dir_to_save = "basic_company_data/webscrape_test"

    scraper.scrape_webpage(url, base_url, dir_to_save)
