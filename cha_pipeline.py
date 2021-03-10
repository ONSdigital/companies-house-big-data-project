#imports 
import os
from os import listdir, chdir, getcwd, popen
from os.path import isfile, join
import time
import math
from random import shuffle
import argparse
import sys
import configparser
import multiprocessing as mp
import concurrent.futures
import gcsfs
import re
import pandas as pd
import importlib
from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
#custom imports
from src.xbrl_scraper.requests_scraper import XbrlScraper
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from src.data_processing.xbrl_parser import XbrlParser
from src.data_processing.cst_data_processing import DataProcessing
from src.data_processing.xbrl_csv_cleaner import XbrlCSVCleaner
from src.data_processing.combine_csvfiles import XbrlCsvAppender
from src.validators.xbrl_validator_methods import XbrlValidatorMethods
from src.classifier.cst_classifier import Classifier

pd.set_option("display.max_columns", 500)

#read info from config file
config = configparser.ConfigParser()
config.read("cha_pipeline.cfg")
#set GCP project id and authentication key 
project_id = config.get('gcsfs_setup', 'project_id')
key = config.get('gcsfs_setup', 'key')

# Assigning variable for each stage of pipeline 
# ---------------------------------------------
xbrl_web_scraper = config.get('cha_workflow', 'xbrl_web_scraper')
xbrl_web_scraper_validator = config.get('cha_workflow', 'xbrl_validator')
xbrl_unpacker = config.get('cha_workflow', 'xbrl_unpacker')
xbrl_parser = config.get('cha_workflow', 'xbrl_parser')
xbrl_csv_cleaner = config.get('cha_workflow', 'xbrl_csv_cleaner')
xbrl_file_appender = config.get('cha_workflow', 'xbrl_file_appender')
pdf_web_scraper = config.get('cha_workflow', 'pdf_web_scraper')

# 1) XBRL web scraper
xbrl_scraper_url = config.get('xbrl_web_scraper_args', 'url')
xbrl_scraper_base_url = config.get('xbrl_web_scraper_args', 'base_url')
xbrl_scraper_dir_to_save = config.get('xbrl_web_scraper_args', 'dir_to_save')

# 2) XBRL web scraper validator
validator_scraped_dir = config.get('xbrl_validator_args', 'scraped_dir')

# 3) XBRL unpacker
unpacker_source_dir = config.get('xbrl_unpacker_args',
                                 'xbrl_unpacker_file_source_dir')
unpacker_destination_dir = config.get('xbrl_unpacker_args',
                                      'xbrl_unpacker_file_destination_dir')

# 4) XBRL parser
xbrl_unpacked_data = config.get('xbrl_parser_args', 'xbrl_parser_data_dir')
xbrl_processed_csv = config.get('xbrl_parser_args',
                                'xbrl_parser_processed_csv_dir')
xbrl_parser_bq_location = config.get('xbrl_parser_args',
                                     'xbrl_parser_bq_location')
xbrl_tag_frequencies = config.get('xbrl_parser_args',
                                  'xbrl_parser_tag_frequencies')
xbrl_parser_no_of_cores = config.get('xbrl_parser_args',
                                  'xbrl_parser_number_of_cores')
xbrl_tag_list = config.get('xbrl_parser_args', 'xbrl_parser_tag_list')
xbrl_parser_process_year = config.get('xbrl_parser_args',
                                      'xbrl_parser_process_year')
xbrl_parser_process_quarter = config.get('xbrl_parser_args',
                                         'xbrl_parser_process_quarter')
xbrl_parser_custom_input = config.get('xbrl_parser_args',
                                      'xbrl_parser_custom_input')

# 5) XBRL appender
xbrl_file_appender_indir = config.get('xbrl_file_appender_args',
                                      'xbrl_file_appender_indir')
xbrl_file_appender_outdir = config.get('xbrl_file_appender_args',
                                       'xbrl_file_appender_outdir')
xbrl_file_appender_year = config.get('xbrl_file_appender_args',
                                     'xbrl_file_appender_year')
xbrl_file_appender_quarter = config.get('xbrl_file_appender_args',
                                        'xbrl_file_appender_quarter')

# 6) XBRL csv cleaner
xbrl_csv_cleaner_indir = config.get('xbrl_csv_cleaner_args',
                                    'xbrl_csv_cleaner_indir')
xbrl_csv_cleaner_outdir = config.get('xbrl_csv_cleaner_args',
                                     'xbrl_csv_cleaner_outdir')
# Not currently used
# Arguments for the filing_fetcher scraper
# filed_accounts_scraped_dir = config.get('pdf_web_scraper_args',
#                                         'filed_accounts_scraped_dir')
# filed_accounts_scraper = config.get('pdf_web_scraper_args',
#                                     'filed_accounts_scraper')

# Calling pipeline (based on booleans in config file)
# -------------------------------------------------
def main():
    print("-" * 50)
    #set GCP enviroment 
    os.environ["PROJECT_ID"] = project_id
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key
    fs = gcsfs.GCSFileSystem(project=project_id, token=key, cache_timeout=0)

    # Execute module xbrl_web_scraper
    if xbrl_web_scraper == str(True):
        print("XBRL web scraper running...")
        print("Scraping XBRL data to:", xbrl_scraper_dir_to_save)

        scraper = XbrlScraper()
        scraper.scrape_webpage(xbrl_scraper_url,
                               xbrl_scraper_base_url,
                               xbrl_scraper_dir_to_save)

    # Validate xbrl data
    if xbrl_web_scraper_validator == str(True):
        print("Validating xbrl web scraped data...")
        validator = XbrlValidatorMethods(fs)
        validator.validate_compressed_files(validator_scraped_dir)

    # Execute module xbrl_unpacker
    if xbrl_unpacker == str(True):
        print("XBRL unpacker running...")
        print("Reading zip file from directory: ", unpacker_source_dir)
        print("Unpacking zip file to directory: ", unpacker_destination_dir)
        unpacker = DataProcessing(fs)
        unpacker.extract_compressed_files(unpacker_source_dir,
                                          unpacker_destination_dir)

    # Execute module xbrl_parser
    if xbrl_parser == str(True):
        print("XBRL parser running...")
        parser_executer = XbrlParser(fs)
        parser_executer.parse_files(xbrl_parser_process_quarter,
                                    xbrl_parser_process_year,
                                    xbrl_unpacked_data,
                                    xbrl_parser_custom_input,
                                    xbrl_parser_bq_location,
                                    xbrl_processed_csv,
                                    xbrl_parser_no_of_cores)

    # Execute module xbrl_csv_cleaner
    if xbrl_csv_cleaner == str(True):
        print("XBRL CSV cleaner running...")
        XbrlCSVCleaner.clean_parsed_files(xbrl_csv_cleaner_indir,
                                          xbrl_csv_cleaner_outdir)

    # Append XBRL data on an annual or quarterly basis
    if xbrl_file_appender == str(True):
        print("XBRL appender running...")
        appender = XbrlCsvAppender(fs)
        appender.merge_files_by_year(xbrl_file_appender_indir,
                                     xbrl_file_appender_outdir,
                                     xbrl_file_appender_year,
                                     xbrl_file_appender_quarter)

if __name__ == "__main__":
    #do we want to incorperate time metrics in some of our print statements within modules?
    process_start = time.time()

    main()

    print("-" * 50)
    print("Process Complete")
