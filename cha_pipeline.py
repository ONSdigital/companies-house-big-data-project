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
from src.authenticator.gcp_authenticator import GCPAuthenticator
from src.xbrl_scraper.requests_scraper import XbrlScraper
from src.xbrl_validator.xbrl_validator_methods import XbrlValidatorMethods
from src.data_processing.cst_data_processing import DataProcessing
from src.xbrl_parser.xbrl_parser import XbrlParser

pd.set_option("display.max_columns", 500)

# Reads from the config file
cfg = configparser.ConfigParser()
cfg.read("cha_pipeline.cfg")

# Config imports
auth_setup = cfg["auth_setup"]
cha_workflow = cfg["cha_workflow"]
web_scraper_args = cfg["xbrl_web_scraper_args"]
validator_args = cfg["xbrl_validator_args"]
unpacker_args = cfg["xbrl_unpacker_args"]
parser_args = cfg["xbrl_parser_args"]


# Calling pipeline (based on booleans in config file)
# -------------------------------------------------
def main():
    print("-" * 50)

    # Create an instance of GCPAuthenitcator and generate keys
    authenticator = GCPAuthenticator(auth_setup["iam_key"], auth_setup["project_id"])
    authenticator.get_sa_keys(auth_setup["keys_fp"])

    # Execute module xbrl_web_scraper
    if cha_workflow['xbrl_web_scraper'] == str(True):
        print("XBRL web scraper running...")
        print("Scraping XBRL data to:", web_scraper_args['dir_to_save'])

        scraper = XbrlScraper(authenticator)
        scraper.scrape_webpage(web_scraper_args['scraper_url'],
                               web_scraper_args['domain'],
                               web_scraper_args['dir_to_save'])

    # Validate xbrl data
    if cha_workflow['xbrl_validator'] == str(True):
        print("Validating xbrl web scraped data...")
        validator = XbrlValidatorMethods(authenticator)
        validator.validate_compressed_files(validator_args['scraped_dir'])

    # Execute module xbrl_unpacker
    if cha_workflow['xbrl_unpacker'] == str(True):
        print("XBRL unpacker running...")
        print("Reading zip file from directory: ", unpacker_args['source_dir'])
        print("Unpacking zip file to directory: ", unpacker_args['output_dir'])
        unpacker = DataProcessing(authenticator)
        unpacker.extract_compressed_files(unpacker_args['source_dir'],
                                          unpacker_args['output_dir'])

    # Execute module xbrl_parser
    if cha_workflow['xbrl_parser'] == str(True):
        print("XBRL parser running...")
        parser_executer = XbrlParser(authenticator)
        parser_executer.parse_files(parser_args['quarter'],
                                    parser_args['year'],
                                    parser_args['unpacked_data_dir'],
                                    parser_args['custom_input'],
                                    parser_args['bq_location'],
                                    parser_args['processed_csv_dir'],
                                    int(parser_args['no_of_cores']))
    
    authenticator.clean_up_keys()

if __name__ == "__main__":
    #do we want to incorperate time metrics in some of our print statements within modules?
    process_start = time.time()

    main()

    print("-" * 50)
    print("Process Complete")