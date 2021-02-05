from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
from datetime import datetime
from dateutil import parser
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from google.cloud import bigquery
from google.oauth2 import service_account
from functools import partial
import pandas as pd
import os
import csv
import time
import sys
import math
import time
import multiprocessing as mp
import numpy as np
import gcsfs
import pytz
import psutil
import gc



class XbrlParser:
    """ This is a class for parsing the XBRL data."""

    def __init__(self, fs):
        """
        Constructs all the necessary attributes for the XbrlParser object of
        which there are none.
        """
        self.__init__
        self.fs = fs

    # Table of variables and values that indicate consolidated status
    consolidation_var_table = {
        "includedinconsolidationsubsidiary": True,
        "investmententityrequiredto\
        applyexceptionfromconsolidationtruefalse": True,
        "subsidiaryunconsolidatedtruefalse": False,
        "descriptionreasonwhyentityhasnot\
        preparedconsolidatedfinancialstatements": "exist",
        "consolidationpolicy": "exist"
    }

    @staticmethod
    def clean_value(string):
        """
        Take a value that is stored as a string, clean it and convert to
        numeric. If it's just a dash, it is taken to mean zero.

        Arguments:
            string: string to be cleaned and converted (str)
        Returns:
            string: cleaned string converted to numeric (int)
        Raises:
            None
        """
        if string.strip() == "-":
            return 0.0
        try:
            return float(string.strip().replace(",", "").replace(" ", ""))
        except:
            pass

        return string

    @staticmethod
    def retrieve_from_context(soup, contextref):
        """
        Used where an element of the document contained no data, only a
        reference to a context element.
        Finds the relevant context element and retrieves the relevant data.

        Arguments:
            soup:       BeautifulSoup souped html/xml object (BeautifulSoup object)
            contextref: id of the context element to be raided
        Returns:
            contents: relevant data from the context (string)
        """
        try:
            context = soup.find("xbrli:context", id=contextref)
            contents = context.find("xbrldi:explicitmember").get_text()\
                .split(":")[-1].strip()
        except:
            contents = ""

        return contents

    @staticmethod
    def retrieve_accounting_standard(soup):
        """
        Gets the account reporting standard in use in a document by hunting
        down the link to the schema reference sheet that always appears to
        be in the document, and extracting the format and standard date from
        the string of the url itself.
        WARNING - That means that there's a lot of implicit hardcoded info
        on the way these links are formatted and referenced within this
        function.  Might need changing someday.

        Arguments:
            soup: BeautifulSoup souped html/xml object (BeautifulSoup object)
        Returns:
            standard:   The standard for the object (string)
            date:       The date for the object (string)
            original_url: The original url of the object (string)
        Raises:
            None
        """
        # Find the relevant link by its unique attribute
        link_obj = soup.find("link:schemaref")

        # If we didn't find anything it's an xml doc using a different
        # element name:
        if link_obj == None:
            link_obj = soup.find("schemaref")

        # extract the name of the .xsd schema file, which contains format
        # and date information
        text = link_obj['xlink:href'].split("/")[-1].split(".")[0]

        # Split the extracted text into format and date, return values
        standard, date, original_url = \
            text[:-10].strip("-"), text[-10:], link_obj['xlink:href']

        return standard, date, original_url

    @staticmethod
    def retrieve_unit(soup, each):
        """
        Gets the reporting unit by trying to chase a unitref to
        its source, alternatively uses element attribute unitref
        if it's not a reference to another element.

        Arguments:
            soup:   BeautifulSoup souped html/xml object (BeautifulSoup object)
            each:   element of BeautifulSoup souped object
        Returns:
            unit_str: the unit of the element (string)
        Raises:
            None
        """
        # If not, try to discover the unit string in the soup object
        try:
            unit_str = soup.find(id=each['unitref']).get_text()
        except:
            # Or if not, in the attributes of the element
            try:
                unit_str = each.attrs['unitref']
            except:
                return "NA"

        return unit_str.strip()

    @staticmethod
    def retrieve_date(soup, each):
        """
        Gets the reporting date by trying to chase a contextref
        to its source and extract its period, alternatively uses
        element attribute contextref if it's not a reference
        to another element.

        Arguments:
            soup:   BeautifulSoup souped html/xml object (BeautifulSoup object)
            each:   element of BeautifulSoup souped object
        Returns:
            date_val: The reporting date of the object (date)
        Raises:
            None
        """
        # Try to find a date tag within the contextref element, starting with
        # the most specific tags, and starting with those for ixbrl docs as
        # it's the most common file.
        date_tag_list = ["xbrli:enddate",
                         "xbrli:instant",
                         "xbrli:period",
                         "enddate",
                         "instant",
                         "period"]

        for tag in date_tag_list:
            try:
                date_str = each['contextref']
                date_val = parser.parse(soup.find(id=each['contextref']).
                                        find(tag).get_text()).date()\
                    .isoformat()

                return date_val
            except:
                pass

        try:
            date_str = each.attrs['contextref']
            date_val = parser.parse(each.attrs['contextref']).date().\
                isoformat()
            return date_val
        except:
            pass

        return "NA"

    @staticmethod
    def parse_element(soup, element):
        """
        For a discovered XBRL tagged element, go through, retrieve its name
        and value and associated metadata.

        Arguments:
            soup:     BeautifulSoup object of accounts document (BeautifulSoup object)
            element:  soup object of discovered tagged element
        Returns:
            element_dict: A dictionary containing the elements name value and
                          metadata (dict)
        Raises:
            None
        """
        if "contextref" not in element.attrs:
            return {}

        element_dict = []

        # Basic name and value
        try:
            # Method for XBRLi docs first
            element_dict['name'] = element.attrs['name'].lower().split(":")[-1]
        except:
            # Method for XBRL docs second
            element_dict['name'] = element.name.lower().split(":")[-1]

        element_dict['value'] = element.get_text()
        element_dict['unit'] = XbrlParser.retrieve_unit(soup, element)
        element_dict['date'] = XbrlParser.retrieve_date(soup, element)

        # If there's no value retrieved, try raiding the associated context
        # data
        if element_dict['value'] == "":
            element_dict['value'] = XbrlParser.retrieve_from_context(
                soup, element.attrs['contextref'])

        # If the value has a defined unit (eg a currency) convert to numeric
        if element_dict['unit'] != "NA":
            element_dict['value'] = XbrlParser.clean_value(
                element_dict['value'])

        # Retrieve sign of element if exists
        try:
            element_dict['sign'] = element.attrs['sign']

            # if it's negative, convert the value then and there
            if element_dict['sign'].strip() == "-":
                element_dict['value'] = 0.0 - element_dict['value']
        except:
            pass

        return element_dict

    @staticmethod
    def parse_elements(element_set, soup):
        """
        For a set of discovered elements within a document, try to parse
        them. Only keep valid results (test is whether field "name" exists).

        Arguments:
            element_set:    BeautifulSoup iterable search result object (list of BeautifulSoup objects)
            soup:           BeautifulSoup object of accounts document (BeautifulSoup object)
        Returns:
            elements:   A list of dicts corresponding to the elements of
                        element_set (list)
        Raises:
            None
        """
        element_dict = {'name': [], 'value': [], 'unit': [],
                         'date': []}
        i = 0
        for each in element_set:
            if "contextref" not in each.attrs:
                continue

            # Basic name and value
            try:
                # Method for XBRLi docs first
                element_dict['name'].append(each.attrs['name'].lower().split(":")[-1])
            except:
                # Method for XBRL docs second
                element_dict['name'].append(each.name.lower().split(":")[-1])

            element_dict['value'].append(each.get_text())
            element_dict['unit'].append(XbrlParser.retrieve_unit(soup, each))
            element_dict['date'].append(XbrlParser.retrieve_date(soup, each))

            # If there's no value retrieved, try raiding the associated context
            # data
            if element_dict['value'][i] == "":
                element_dict['value'][i] = XbrlParser.retrieve_from_context(
                    soup, each.attrs['contextref'])

            # If the value has a defined unit (eg a currency) convert to numeric
            if element_dict['unit'][i] != "NA":
                element_dict['value'][i] = XbrlParser.clean_value(
                    element_dict['value'][i])

            # Retrieve sign of element if exists
            try:
                sign = each.attrs['sign']

                # if it's negative, convert the value then and there
                if sign.strip() == "-":
                    element_dict['value'][i] = 0.0 - element_dict['value'][i]
            except:
                pass

            i+=1
        return element_dict

    @staticmethod
    def summarise_by_sum(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        a summary variable relating to the financial state of the enterprise
        by summing all those named that exist.

        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and sum (of all) if they exist
        Returns (as a dict):
            total_assets:   the totals of the given values
            units:          the units corresponding to the given sum
        """
        # Convert elements to pandas df
        df = pd.DataFrame(doc['elements'])

        # Subset to most recent (latest dated)
        df = df[df['date'] == doc['doc_balancesheetdate']]

        total_assets = 0.0
        unit = "NA"

        # Find the total assets by summing components
        for each in variable_names:
            # Fault-tolerant, will skip whatever isn't numeric
            try:
                total_assets = total_assets + df[df['name'] == each]\
                    .iloc[0]['value']
                # Retrieve reporting unit if exists
                unit = df[df['name'] == each].iloc[0]['unit']
            except:
                pass

        return {"total_assets": total_assets, "unit": unit}

    @staticmethod
    def summarise_by_priority(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        a summary variable relating to the financial state of the enterprise
        by looking for each named, in order.

        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and check if they exist
        Returns (as a dict):
            primary_assets: total assets from given variables
            unit:           units for corresponding assets
        Raises:
            None
        """
        # Convert elements to pandas df
        df = pd.DataFrame(doc['elements'])

        # Subset to most recent (latest dated)
        df = df[df['date'] == doc['doc_balancesheetdate']]

        primary_assets = 0.0
        unit = "NA"

        # Find the net asset/liability variable by hunting names in order
        for each in variable_names:
            try:
                # Fault tolerant, will skip whatever isn't numeric
                primary_assets = df[df['name'] == each].iloc[0]['value']
                # Retrieve reporting unit if it exists
                unit = df[df['name'] == each].iloc[0]['unit']
                break
            except:
                pass

        return {"primary_assets": primary_assets, "unit": unit}

    @staticmethod
    def summarise_set(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        summary variables relating to the financial state of the enterprise
        by returning all those named that exist.
        
        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and return if they exist.
        Returns:
            results: a dictionary of all the values for each in variable_names
                     (dict)
        Raises:
            None
        """
        results = {}

        # Convert elements to pandas df
        df = pd.DataFrame(doc['elements'])

        # Subset to most recent (latest dated)
        df = df[df['date'] == doc['doc_balancesheetdate']]

        # Find all the variables of interest should they exist
        for each in variable_names:
            try:
                results[each] = df[df['name'] == each].iloc[0]['value']
            except:
                pass

        # Send the variables back to be appended
        return results

    @staticmethod
    def scrape_elements(soup, filepath):
        """
        Parses an XBRL (xml) company accounts file for all labelled content and
        extracts the content (and metadata eg; unitref) of each element found
        to a dictionary.

        Arguments:
            soup:        BeautifulSoup object of accounts document (BeautifulSoup object)
            filepath:    A filepath (str)
        Returns:
             elements:  A list of dictionaries containing meta data for each
                        element (list)
        Raises:
            None
        """
        # Try multiple methods of retrieving data, I think only the first is
        # now needed though.  The rest will be removed after testing this
        # but should not affect execution speed.
        try:
            element_set = soup.find_all()
            elements = XbrlParser.parse_elements(element_set, soup)
        except:
            # if fails parsing create dummy entry elements so entry still
            # exists in dictionary
            elements = {'name': 'NA', 'value': 'NA', 'unit': 'NA',
                         'date': 'NA', 'sign': 'NA'}
            pass

        return elements

    @staticmethod
    def flatten_dict(doc):
        """
        Takes in a list of dictionaries and combines them into a
        single dictionary - assumes dictionaries all have the same keys.

        Argument:
            doc: a list of dictionaries (list)
        Returns:
            doc_dict: a dictionary formed by combing the list of dictionaries
                      (dict)
        Raises:
            None
        """
        # combines list of dictionaries into one dictionary based on common
        # keys
        doc_dict = {}
        for k in doc[0].keys():
            doc_dict[k] = [d[k] for d in doc]

        return doc_dict

    @staticmethod
    def flatten_data(doc, bq_export, temp_exports= "data/temp_exports"):
        """
        Takes the data returned by flatten dict, with its tree-like
        structure and reorganises it into a long-thin format table structure
        suitable for SQL applications.

        Argument:
            doc: a list of dictionaries (list)
        Returns:
            df_elements: A dataframe containing all data from doc (dataframe)
        Raises:
            None
        """
        doc2 = doc.copy()

        # Check if the temp_exports folder is present
        if not (os.path.isdir(temp_exports)):
            os.mkdir(temp_exports)

        # Check if temp file is already present and remove
        try:
            os.remove(temp_exports + "/df_elements.csv")
        except:
            pass

        #define lenth of dict and initial time
        T = len(doc2)
        t0 = time.time()

        # loop over each file and create a separate dataframe
        # for each set (elements) of parsed tags, appending result to list
        rc = 0
        df_list = []
        for i in range(T):
            # Turn each elements dict into a dataframe
            df_element_export = pd.DataFrame.from_dict(doc2[i])

            # Remove the 'sign' column if it is present
            try:
                df_element_export = df_element_export.drop('sign', axis=1)
            except:
                None

            # Remove unwanted characters
            unwanted_chars = ['  ', '"', '\n']
            #print(df_element_export.value.str, flush=True)
            for char in unwanted_chars:
                df_element_export["value"] = df_element_export["value"].str\
                    .replace(char, '')
            #print(df_element_export.value)
            # Change the order of the columns
            wanted_cols = ['date', 'name', 'unit', 'value', 'doc_name',
                           'doc_type',
                           'doc_upload_date', 'arc_name', 'parsed',
                           'doc_balancesheetdate',
                           'doc_companieshouseregisterednumber',
                           'doc_standard_type',
                           'doc_standard_date', 'doc_standard_link', ]

            df_element_export = df_element_export[wanted_cols]
            df_element_export = df_element_export.convert_dtypes()

            df_element_export['doc_upload_date'] = pd.to_datetime(
                df_element_export['doc_upload_date'],
                errors="coerce")
            df_element_export['date'] \
                = pd.to_datetime(df_element_export['date'],
                                 format="%Y-%m-%d",
                                 errors="coerce")
            df_element_export['doc_balancesheetdate'] \
                = pd.to_datetime(df_element_export['doc_balancesheetdate'],
                                 format="%Y-%m-%d",
                                 errors="coerce")
            df_element_export['doc_standard_date'] \
                = pd.to_datetime(df_element_export['doc_standard_date'],
                                 format="%Y-%m-%d",
                                 errors="coerce")
            df_list.append(df_element_export)
            rc += df_element_export.shape[0]
            del df_element_export
            # if i % 100 == 0:
            #     print("%2.2f %% have been processed" % ((i / T) * 100))

        df_batch = pd.concat(df_list)
        print("Batch df contains {} rows".format(df_batch.shape[0]))
        XbrlParser.append_to_bq(df_batch, bq_export)
        del df_list, df_batch, doc2, doc
        df_batch = pd.DataFrame()
        doc2, doc = [], []
        gc.collect()

        return rc




    def process_account(self, filepath):
        """
        Scrape all of the relevant information from an iXBRL (html) file,
        upload the elements and some metadata to a mongodb.

        Arguments:
            filepath: complete filepath from drive root (str)
        Returns:
            doc: dictionary of all data from the relevant file (dict)
        Raises:
            None
        """
        doc = {}

        # Some metadata, doc name, upload date/time, archive file it came from
        doc['doc_name'] = filepath.split("/")[-1]
        doc['doc_type'] = filepath.split(".")[-1].lower()
        doc['doc_upload_date'] = str(datetime.now())
        doc['arc_name'] = filepath.split("/")[-2]

        # Complicated ones
        sheet_date = filepath.split("/")[-1].split(".")[0].split("_")[-1]
        doc['doc_balancesheetdate'] = datetime.strptime(sheet_date, "%Y%m%d")\
            .date().isoformat()

        doc['doc_companieshouseregisterednumber'] = filepath.split("/")[-1]\
            .split(".")[0].split("_")[-2]

        # loop over multi-threading here - imports data and parses on separate
        # threads
        try:
            file = self.fs.open(filepath)
            soup = BS(file, "lxml")
        except:
            print("Failed to open: " + filepath)
            return 1

        # Get metadata about the accounting standard used
        try:
            doc['doc_standard_type'],\
                doc['doc_standard_date'],\
                doc['doc_standard_link'] = XbrlParser\
                .retrieve_accounting_standard(soup)
            doc['parsed'] = True
        except:
            doc['doc_standard_type'],\
                doc['doc_standard_date'],\
                doc['doc_standard_link'] = (0, 0, 0)
            doc['parsed'] = False

        # Fetch all the marked elements of the document
        try:
            doc.update(XbrlParser.scrape_elements(soup, filepath))
        except Exception as e:
            doc['parsed'] = False
            doc['Error'] = e
        try:
            return doc
        except Exception as e:
            return e

    @staticmethod
    def create_month_list(quarter):
        """
        Create a list of the names of the months (as strings) corresponding
        to the specified quarter.

        Arguments:
            quarter:    quarter of the year between 1 and 4 (as a string) or
                        None to generate a list for the year
        Returns:
            month_list: list of the corresponding months (as strings) (list)
        Raises:
            none
        """
        if quarter == "1":
            month_list = ['January', 'February', 'March']
        elif quarter == "2":
            month_list = ['April', 'May', 'June']
        elif quarter == "3":
            month_list = ['July', 'August', 'September']
        elif quarter == "4":
            month_list = ['October', 'November', 'December']
        else:
            month_list = ['January', 'February', 'March', 'April',
                          'May', 'June', 'July', 'August',
                          'September', 'October', 'November', 'December']
            if quarter != "None":
                print("Invalid quarter specified...\
                processing one year of data!")

        return month_list

    @staticmethod
    def create_directory_list(months, filepath, year, custom_input="None"):
        """
        Creates a list of file paths for accounts which are dated in the months
        specified in a list of months.

        Arguments:
            months:         A list of strings of the months to find files for
                            (list)
            filepath:       String of the directory containing the unpacked
                            files (str)
            year:           The year of which to find accounts from (str)
            custom_input:   Used to set a specific folder of accounts
        Returns:
            directory_list: list containing strings of the file paths of all
                            accounts in the relevant year (list)
        Raises:
            None
        """
        # Create a list of directories from each month present in the month
        # list
        directory_list = []
        if custom_input == "None":
            for month in months:
                directory_list.append(filepath
                                      + "/Accounts_Monthly_Data-"
                                      + month
                                      + year)

        # If a custom list has been specified as a comma separated string, use
        # this instead
        else:
            folder_list = custom_input.split(",")
            for folder in folder_list:
                directory_list.append(filepath + "/" + folder)

        return directory_list

    def parse_directory(self, directory, processed_path, num_processes=1):
        """
        Takes a directory, parses all files contained there and saves them as
        csv files in a specified directory.

        Arguments:
            directory: A directory (path) to be processed (str)
            processed_path: String of the path where processed files should be
                            saved (str)
            num_processes:  The number of cores to use in multiprocessing (int)
        Returns:
            None
        Raises:
            None
        """
        extractor = XbrlExtraction(self.fs)

        # Get all the filenames from the example folder
        files, folder_month, folder_year = extractor.get_filepaths(directory)

        print(len(files))
        
        # Here you can splice/truncate the number of files you want to process
        # for testing
        # files = files[0:10000]

        # TO BE COMMENTED OUT AFTER TESTING
        print(folder_month, folder_year)

        # Code needed to split files by the number of cores before passing in
        # as an argument
        chunk_len = math.ceil(len(files) / num_processes)
        files = [files[i:i + chunk_len] for i in
                 range(0, len(files), chunk_len)]

        # define number of processors
        pool = mp.Pool(processes=num_processes)
        # Finally, build a table of all variables from all example (digital)
        # documents splitting the load between cpu cores = num_processes
        # This can take a while (hopefully not anymore!!!)

        table_export = processed_path + "." + folder_month + "-" + folder_year

        self.mk_bq_table(table_export)

        build_month_table_partial = partial(self.build_month_table, table_export)
        fails = pool.map(build_month_table_partial, files)

        pool.close()
        pool.join()

        # # print(fails)
        # # combine resultant list of lists
        # # print("Combining lists...")
        # fails = [item for sublist in fails for item in sublist]

        # combine data and convert into dataframe

        # fails = self.build_month_table(table_export, files)
        print(fails)
        # self.flatten_data(r, table_export)


        # Output all unique tags to a txt file

        # extractor.retrieve_list_of_tags(
        #     results,
        #     "name",
        #     xbrl_tag_list,
        #     folder_month,
        #     folder_year
        # )
        #
        # # Output all unique tags and their relative frequencies to a txt file
        # extractor.get_tag_counts(
        #     results,
        #     "name",
        #     xbrl_tag_frequencies,
        #     folder_month,
        #     folder_year
        # )

        # print(results.shape)

    def parse_files(self, quarter, year, unpacked_files,
                    custom_input, processed_files, num_cores):
        """
        Parses a set of accounts for a given time period and saves as a csv in
        a specified location.

        Arguments:
            quarter:            quarter of the given year to process files from (int)
            year:               year to process files from (int)
            unpacked_files:     path of directory where files to be processed
                                are stored (string)
            processed_files:    path of directory where the files the resulting
                                files will be saved (string)
            num_cores:          number of cores to use with mutliprocessing
                                module (int)
            custom_input:       Used to set a specific folder of accounts
        Returns:
            None
        Raises:
            None
        """
        # Construct both the list of months and list of corresponding
        # directories
        month_list = self.create_month_list(quarter)
        directory_list = self.create_directory_list(month_list,
                                                          unpacked_files,
                                                          year,
                                                          custom_input)
        # Parse each directory
        for directory in directory_list:
            print("Parsing " + directory + "...")
            self.parse_directory(directory, processed_files, num_cores)

    def build_month_table(self, bq_export,list_of_files):
        """
        Function which parses, sequentially, a list of xbrl/ html files,
        converting each parsed file into a dictionary and appending to a list.

        Arguments:
            list of files: list of filepaths, each coresponding to a xbrl/html file (list)

        Returns:
            results:       list of dictionaries, each containing the parsed content of
                           a xbrl/html file (list)
        Raises:
            None
        """

        process_start = time.time()

        # Empty table awaiting results
        results = []
        fails = []
        #
        # big_results = []

        start_memory = psutil.virtual_memory().percent
        file_threshold = 500
        print("Start memory usuage: ", start_memory)
        COUNT = 0
        file_count = 0
        row_count = 0
        batch_count = 0
        # For every file
        for file in list_of_files:
            # try:
                COUNT += 1
                file_count += 1
                # Read the file and parse
                doc = self.process_account(file)
                # flatten the elements dict into single dict
                #doc['elements'] = XbrlParser.flatten_dict(doc['elements'])

                # append results to table
                # for i in range(len(results)**3):
                results.append(doc)
                #
                # big_results += [doc]*(len(big_results)+1)

                if (file_count > file_threshold) \
                        or COUNT == len(list_of_files):
                    file_count = 0
                    row_count += XbrlParser.flatten_data(results, bq_export)
                    XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT,
                                               len(list_of_files), row_count,
                                               batch_count,
                                               psutil.virtual_memory().percent,
                                               uploading=True,
                                               bar_length=50,
                                               width=20)
                    batch_count += 1
                    del results
                    results = []
                    # big_results = []
                    gc.collect()
                    # t = 0
                    # while psutil.virtual_memory().percent > start_memory + 10:
                    #     sys.stdout.write("\r Waiting, memory at {0}%".format(
                    #         psutil.virtual_memory().percent
                    #     ))
                    #     sys.stdout.flush()
                    #     t+=1
                    #     time.sleep(6)
                    #     if t > 100:
                    #         break
                XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT,
                                           len(list_of_files), row_count,
                                           batch_count,
                                           psutil.virtual_memory().percent,
                                           bar_length=50,
                                           width=20)
            # except:
            #     if print_fails:
            #         print(file, "has failed to parse")
            #     fails.append(file)

        print(
            "Average time to process an XBRL file: \x1b[31m{:0f}\x1b[0m".format(
                (time.time() - process_start) / 60, 2), "minutes")

        return fails

    @staticmethod
    def append_to_bq(df, table):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            schema = [
                bigquery.SchemaField("doc_companieshouseregisterednumber", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("parsed", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("doc_balancesheetdate", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("doc_upload_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("doc_standard_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("unit", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("value", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("arc_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_standard_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_standard_link", bigquery.enums.SqlTypeNames.STRING)
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        job = client.load_table_from_dataframe(
            df, table, job_config=job_config
            )  # Make an API request.
        job.result()  # Wait for the job to complete.
        job = 0
        del job

    @staticmethod
    def mk_bq_table(bq_location, schema="parsed_data_schema.txt"):
        client = bigquery.Client()

        client.delete_table(bq_location, not_found_ok=True)
        bq_string = "bq mk --table " + bq_location + " " + schema
        os.popen(bq_string).read()
