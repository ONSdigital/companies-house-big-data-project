from os import listdir, chdir
from os.path import isfile, join
from scrapy.spiders import CrawlSpider
import hashlib
import scrapy
import time
import random
import argparse
import sys
#import cv2
import configparser


class XbrlScraperItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()


class XBRLSpider(CrawlSpider):
    name = "xbrl_scraper"
    
    config = configparser.ConfigParser()
    chdir("..")
    chdir("..")
    config.read("cha_pipeline.cfg")

    allowed_domains = config.get('xbrl_web_scraper_args', 'allowed_domains')\
        .split(',')
    start_urls = config.get('xbrl_web_scraper_args', 'start_urls').split(',')

    filepath = config.get('xbrl_web_scraper_args', 'scraped_dir')
    filepath += "/"

    def start_requests(self):
        """
        Initiates scrapy requests for all urls in start_urls
        """
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        Extracts all zip files from the scraped website given by the variable
        "response"
        Outputs zip file information to be downloaded by scrapy's internal
        pipeline.

        Arguments:
            self:
            response: web page scraped from website crawled by scraper
        Returns:
            XbrlScraperItem :  Url, checksum and path to scraped zip file.
                               This will then be downloaded by scrapy
        Raises:
            None
        """
        # Get a list of all filenames excluding directories and file extensions
        files = [f.split(".")[0] for f in listdir(self.filepath)
                 if isfile(join(self.filepath, f))]
        
        # Extract all links from the web page (the response)
        links = response.xpath('//body//a/@href').extract()

        # Trim out links which do not point to zip files and make the URLs
        # absolute
        links = [response.urljoin(link) for link in links
                 if link.split('.')[-1] == "zip"]

        # Filter list of links to exclude those whose files have already been
        # downloaded
        # This is based on a comparison between the existing files which have
        # SHA1 hashed filenames and the SHA1 hashes of the scraped URLs
        filtered_links = [link for link in links if
                          hashlib.sha1(link.encode('utf-8')).hexdigest()
                          not in files]

        filtered_links = [filtered_links[0]]

        # Yield items for download
        for link in filtered_links:

            # Random sleep to avoid stressing the target server and to
            # ensure all data in the zip files is downloaded
            time.sleep((random.random() * 2.0) + 3.0)

            #if link == 'http://download.companieshouse.gov.uk/\
            # Accounts_Bulk_Data-2020-05-19.zip':
            #if link == 'http://download.companieshouse.gov.uk/archive\
            # /Accounts_Monthly_Data-August2020.zip':

            yield XbrlScraperItem(file_urls=[link])

        #yield XbrlScraperItem(file_urls=
        # ['http://download.companieshouse.gov.uk\
        # /Accounts_Bulk_Data-2020-05-19.zip'])

        # 0c393a225a7afbfaa3f6e7bb7387da19af85f6ec
        # This is a hash of 'http://download.companieshouse.gov.uk\
        # /Accounts_Bulk_Data-2020-05-19.zip'
