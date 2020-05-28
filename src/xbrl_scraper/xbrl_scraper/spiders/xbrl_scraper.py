from os import listdir
from os.path import isfile, join
from scrapy.spiders import CrawlSpider
import hashlib
import scrapy
import time

class XbrlScraperItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()

class XBRLSpider(CrawlSpider):

    name = "xbrl_scraper"

    allowed_domains = ['download.companieshouse.gov.uk/en_accountsdata.html']
    start_urls = ['http://download.companieshouse.gov.uk/en_accountsdata.html']

    # allowed_domains = ['download.companieshouse.gov.uk/en_monthlyaccountsdata.html',
    #                    'download.companieshouse.gov.uk/historicmonthlyaccountsdata.html']
    # start_urls = ['http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html',
    #               'http://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html']

    filepath = "/shares/data/20200519_companies_house_accounts/xbrl_scraped_data_testing"

    def parse(self, response):
        """
        Extracts all zip files from the scraped website given by the variable "response"
        Outputs zip file information to be downloaded by scrapy's internal pipeline.

        Arguments:
            self:
            response: web page scraped from website crawled by scraper
        Returns:
            ExtractedZipFile :  Url, checksum and path to scraped zip file.
                                This will then be downloaded by scrapy
        Raises:
            None
        """
        # Get a list of all filenames excluding directories and file extensions
        files = [f.split(".")[0] for f in listdir(self.filepath) if isfile(join(self.filepath, f))]

        # Extract all links from the web page (the response)
        links = response.xpath('//body//a/@href').extract()

        # Trim out links which do not point to zip files and make the URLs absolute
        links = [response.urljoin(link) for link in links if link.split('.')[-1] == "zip"]

        # Filter list of links to exclude those whose files have already been downloaded
        # This is based on a comparison between the existing files which have SHA1 hashed filenames
        # and the SHA1 hashes of the scraped URLs
        filtered_links = [link for link in links if hashlib.sha1(link.encode('utf-8')).hexdigest() not in files]

        # Yield items for download
        x = 0
        for link in filtered_links:
            print(link)
            x += 1
            #if x == 2: break

            time.sleep(4.0) # Add random sleep time

            #if link == 'http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2020-05-19.zip':
                #yield XbrlScraperItem(file_urls=[link])

        #yield XbrlScraperItem(file_urls=['http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2020-05-19.zip'])

        # 0c393a225a7afbfaa3f6e7bb7387da19af85f6ec
        # This is a hash of 'http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2020-05-19.zip'