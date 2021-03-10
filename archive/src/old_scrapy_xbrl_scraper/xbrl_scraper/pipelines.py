# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.files import FilesPipeline

class XbrlScraperPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):

        """
        Ensures the filename of each file is as shown on the target
        web page and not the default SHA1 hash

        Arguments:
            self:
            request: Scraped URL
            response: web page scraped from website crawled by scraper
            info
        Returns:
            filename :  The filename of the target file extracted from
                        the URL
        Raises:
            None
        """

        url = request.url
        filename = request.url.split("/")[-1]
        print("URL:")
        print(url)
        return filename
