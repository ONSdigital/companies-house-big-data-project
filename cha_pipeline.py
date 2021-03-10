from src.data_processing.xbrl_csv_cleaner import XbrlCSVCleaner
from src.data_processing.combine_csvfiles import XbrlCsvAppender
from src.validators.xbrl_validator_methods import XbrlValidatorMethods
from src.data_processing.xbrl_parser import XbrlParser
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from src.performance_metrics.binary_classifier_metrics import (
    BinaryClassifierMetrics
)
from src.classifier.cst_classifier import Classifier
from src.data_processing.cst_data_processing import DataProcessing
from src.xbrl_scraper.requests_scraper import XbrlScraper
from os import listdir, chdir, getcwd, popen
from os.path import isfile, join
import time
import math
from random import shuffle
import argparse
import sys
#import cv2
import configparser
import multiprocessing as mp
import concurrent.futures
import gcsfs

import os
import re
import numpy as np
import pandas as pd
import importlib

from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
pd.set_option("display.max_columns", 500)
config = configparser.ConfigParser()
config.read("cha_pipeline.cfg")

project_id = config.get('gcsfs_setup', 'project_id')
key = config.get('gcsfs_setup', 'key')

xbrl_web_scraper = config.get('cha_workflow', 'xbrl_web_scraper')
xbrl_web_scraper_validator = config.get('cha_workflow', 'xbrl_validator')
xbrl_unpacker = config.get('cha_workflow', 'xbrl_unpacker')
xbrl_parser = config.get('cha_workflow', 'xbrl_parser')
xbrl_csv_cleaner = config.get('cha_workflow', 'xbrl_csv_cleaner')
xbrl_file_appender = config.get('cha_workflow', 'xbrl_file_appender')
pdf_web_scraper = config.get('cha_workflow', 'pdf_web_scraper')
pdfs_to_images = config.get('cha_workflow', 'pdfs_to_images')
train_classifier_model = config.get('cha_workflow',
                                    'train_classifier_model')
binary_classifier = config.get('cha_workflow', 'binary_classifier')
ocr_functions = config.get('cha_workflow', 'ocr_functions')
nlp_functions = config.get('cha_workflow', 'nlp_functions')
merge_xbrl_to_pdf_data = config.get('cha_workflow', 'merge_xbrl_to_pdf_data')

# Arguments for the XBRL web scraper
xbrl_scraper_url = config.get('xbrl_web_scraper_args', 'url')
xbrl_scraper_base_url = config.get('xbrl_web_scraper_args', 'base_url')
xbrl_scraper_dir_to_save = config.get('xbrl_web_scraper_args', 'dir_to_save')

# Arguments for the XBRL web scraper validator
validator_scraped_dir = config.get('xbrl_validator_args', 'scraped_dir')

# Arguments for the XBRL unpacker
unpacker_source_dir = config.get('xbrl_unpacker_args',
                                 'xbrl_unpacker_file_source_dir')
unpacker_destination_dir = config.get('xbrl_unpacker_args',
                                      'xbrl_unpacker_file_destination_dir')

# Arguments for the XBRL parser
xbrl_unpacked_data = config.get('xbrl_parser_args', 'xbrl_parser_data_dir')
xbrl_processed_csv = config.get('xbrl_parser_args',
                                'xbrl_parser_processed_csv_dir')
xbrl_parser_bq_location = config.get('xbrl_parser_args',
                                     'xbrl_parser_bq_location')
xbrl_tag_frequencies = config.get('xbrl_parser_args',
                                  'xbrl_parser_tag_frequencies')
xbrl_tag_list = config.get('xbrl_parser_args', 'xbrl_parser_tag_list')
xbrl_parser_process_year = config.get('xbrl_parser_args',
                                      'xbrl_parser_process_year')
xbrl_parser_process_quarter = config.get('xbrl_parser_args',
                                         'xbrl_parser_process_quarter')
xbrl_parser_custom_input = config.get('xbrl_parser_args',
                                      'xbrl_parser_custom_input')

# Arguments for xbrl appender
xbrl_file_appender_indir = config.get('xbrl_file_appender_args',
                                      'xbrl_file_appender_indir')
xbrl_file_appender_outdir = config.get('xbrl_file_appender_args',
                                       'xbrl_file_appender_outdir')
xbrl_file_appender_year = config.get('xbrl_file_appender_args',
                                     'xbrl_file_appender_year')
xbrl_file_appender_quarter = config.get('xbrl_file_appender_args',
                                        'xbrl_file_appender_quarter')

# Arguments for xbrl csv cleaner
xbrl_csv_cleaner_indir = config.get('xbrl_csv_cleaner_args',
                                    'xbrl_csv_cleaner_indir')
xbrl_csv_cleaner_outdir = config.get('xbrl_csv_cleaner_args',
                                     'xbrl_csv_cleaner_outdir')

# Arguments for xbrl melt to pivot table

# Arguments for xbrl subsets

# Arguments for the filing_fetcher scraper
# filed_accounts_scraped_dir = config.get('pdf_web_scraper_args',
#                                         'filed_accounts_scraped_dir')
# filed_accounts_scraper = config.get('pdf_web_scraper_args',
#                                     'filed_accounts_scraper')

# Arguments for pdfs_to_images

# Arguments for train_classifier_model

# Arguments for binary_classifier

# Arguments for binary_classifier_accuracy

# Arguments for ocr_functions

# Arguments for nlp_functions

# Arguments for merge_xbrl_to_pdf_data


def main():
    print("-" * 50)

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

        # print("XBRL web scraper running...")
        # print("Scraping XBRL data to:", scraped_dir)
        # print("Running crawler from:", xbrl_scraper)
        # chdir(xbrl_scraper)
        # print(getcwd())
        # cmdlinestr = "scrapy crawl xbrl_scraper"
        # popen(cmdlinestr).read()

    # Validate xbrl data
    if xbrl_web_scraper_validator == str(True):
        validator = XbrlValidatorMethods(fs)
        print("Validating xbrl web scraped data...")
        validator.validate_compressed_files(validator_scraped_dir)

    # Execute module xbrl_unpacker
    if xbrl_unpacker == str(True):
        print("XBRL unpacker running...")
        print("Unpacking zip files...")
        print("Reading from directory: ", unpacker_source_dir)
        print("Writing to directory: ", unpacker_destination_dir)
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
                                    2)

    # Execute module xbrl_csv_cleaner
    if xbrl_csv_cleaner == str(True):

        print("XBRL CSV cleaner running...")
        XbrlCSVCleaner.clean_parsed_files(xbrl_csv_cleaner_indir,
                                          xbrl_csv_cleaner_outdir)

    # Append XBRL data on an annual or quarterly basis
    if xbrl_file_appender == str(True):
        appender = XbrlCsvAppender(fs)
        print("XBRL appender running...")
        appender.merge_files_by_year(xbrl_file_appender_indir,
                                     xbrl_file_appender_outdir,
                                     xbrl_file_appender_year,
                                     xbrl_file_appender_quarter)
    """
    # Execute PDF web scraper
    if pdf_web_scraper == str(True):
        print("PDF web scraper running...")
        print("Scraping filed accounts as PDF data to:",
         filed_accounts_scraped_dir)
        print("Running crawler from:", filed_accounts_scraper)
        chdir(filed_accounts_scraper)
        print(getcwd())
        paper_filing_cmdlinestr = "scrapy crawl latest_paper_filing"
        popen(paper_filing_cmdlinestr).read()

    # Convert PDF files to images
    if pdfs_to_images == str(True):
        print("Converting all PDFs to images...")

    # Train the Classifier model
    if train_classifier_model == str(True):
        print("Training classifier model...")

    # Execute binary Classifier
    if binary_classifier == str(True):
        print("Executing binary classifier...")

    # Execute OCR
    if ocr_functions == str(True):
        print("Running all OCR functions...")
        # instance to class
        
    # Execute NLP
    if nlp_functions == str(True):
        print("Running all NLP functions...")

    # Merge xbrl and PDF file data
    if merge_xbrl_to_pdf_data == str(True):
        print("Merging XBRL and PDF data...")

 
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--input_imgs", required = True,
        help = "path to a directory containing images to be used as input data")
    ap.add_argument("-c", "--cascade_file", required = True,
        help = "path to cascade classifier.")
    ap.add_argument("-s", "--scale_factor", required = True,
        help = "cascade classifier scale factor.")
    ap.add_argument("-m", "--min_neighbors", required = True,
        help = "cascade classifier min neighbors.")
    ap.add_argument("-x", "--cascade_width", required = True,
        help = "cascade classifier starting sample width.")
    ap.add_argument("-y", "--cascade_height", required = True,
        help = "cascade classifier starting sample height.")
    ap.add_argument("-o", "--classifier_output_dir", required = True,
        help = "path to classifier output.")
    ap.add_argument("-p", "--processed_images_dir", required = True,
        help = "path to images processed by the classifier.")
    ap.add_argument("-v", "--show_classifier_output", required = False,
        help = "display the output of the classifier to the user.")
    args = vars(ap.parse_args())

    detector = cv2.CascadeClassifier(args["cascade_file"])

    # load the input image and convert it to grayscale
    for image in listdir(args["input_imgs"]):
        img = Classifier.classifier_input(join(args["input_imgs"], image))
        grey = Classifier.imgs_to_grey(img)

        rects = Classifier.ensemble(detector,
                           grey,
                           float(args["scale_factor"]),
                           int(args["min_neighbors"]),
                           int(args["cascade_width"]),
                           int(args["cascade_height"]))

        print("\n[INFO] Found " 
        + str(Classifier.count_classifier_output(rects)) 
        + " companies house stamps.")

        Classifier.classifier_output(rects, img, args["classifier_output_dir"])

        Classifier.classifier_process(img, 
        join(args["processed_images_dir"], image))

        if args["show_classifier_output"] == "True": 
            Classifier.display_classifier_output(image, img)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
        else:
            pass
    """


if __name__ == "__main__":
    process_start = time.time()

    main()

    print("-" * 50)
    print("Process Complete")
    # print("The time taken to process an image is: ",
    #       "{}".format((time.time() - process_start) / 60, 2),
    #       " minutes")
