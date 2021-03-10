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

class XbrlScraper:

    def scrape_webpage(self, url, base_url, dir_to_save):
        """
        Scrapes target web page and saves all zip files found to
        a directory

        Arguments:
            url:            Url of web page to scrape (str)
            base_url:       Base url of wep page to scrape (str)
            dir_save_to:    GCS directory to save zip files to, consisting of bucket_name/folder
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

        print("Fetching content...")
        res = requests.get(url)

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

            storage_client = storage.Client()
            bucket = storage_client.bucket(dir_to_save.split("/")[0])

            # Download and save zip files
            for link in links:

                zip_url = base_url + link

                if "/" in link: link = link.split("/")[-1]

                blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)

                # Only download and save a file if it doesn't exist in the directory
                if (not blob.exists()) and link == "Accounts_Monthly_Data-April2019.zip":
                                     
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

    # url = "http://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html"
    # base_url = "http://download.companieshouse.gov.uk/"

    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"

    scraper.scrape_webpage(url, base_url, dir_to_save)
