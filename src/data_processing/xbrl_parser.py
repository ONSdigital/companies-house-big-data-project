from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
from datetime import datetime
from dateutil import parser
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
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
                element_dict['name'].append(
                    each.attrs['name'].lower().split(":")[-1])
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

            i += 1
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
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError(
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError(
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError(
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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
        # Check arguments are of correct types
        if not all(isinstance(el, dict) for el in doc):
            raise TypeError(
                "'doc' argument must be a list of dictionaries"
            )

        # combines list of dictionaries into one dictionary based on common
        # keys
        doc_dict = {}
        for k in doc[0].keys():
            doc_dict[k] = [d[k] for d in doc]

        return doc_dict

    @staticmethod
    def flatten_data(doc, bq_export):
        """
        Takes the data returned by flatten dict, with its tree-like
        structure and reorganises it into a long-thin format table structure
        suitable for SQL applications.

        Argument:
            doc:        a list of dictionaries (list)
            bq_export:  BigQuery table to upload results to (str)
        Returns:
            df_elements: A dataframe containing all data from doc (dataframe)
        Raises:
            None
        """
        # Check arguments are of correct types
        if not all(isinstance(el, dict) for el in doc):
            raise TypeError(
                "'doc' argument must be a list of dictionaries"
            )

        doc2 = doc.copy()

        # Define lenth of dict and initial time
        T = len(doc2)
        t0 = time.time()

        # Set up row counter and empty list to save DataFrames
        rc = 0
        df_list = []

        # loop over each file and create a separate dataframe
        # for each set (elements) of parsed tags, appending result to list
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

            for char in unwanted_chars:
                df_element_export["value"] = df_element_export["value"].str\
                    .replace(char, '')

            # Change the order of the columns
            wanted_cols = ['date', 'name', 'unit', 'value', 'doc_name',
                           'doc_type',
                           'doc_upload_date', 'arc_name', 'parsed',
                           'doc_balancesheetdate',
                           'doc_companieshouseregisterednumber',
                           'doc_standard_type',
                           'doc_standard_date', 'doc_standard_link', ]

            # Keep only the remaining columns and set dtypes
            df_element_export = df_element_export[wanted_cols]
            df_element_export = df_element_export.convert_dtypes()

            # Set explicit data types for date columns - requirement for
            # BigQuery upload
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

            # Update row count and free up memory
            rc += df_element_export.shape[0]
            del df_element_export

        # Concatenate list of DataFrames and append to BigQuery table
        df_batch = pd.concat(df_list)
        print("\n Batch df contains {} rows".format(df_batch.shape[0]))
        XbrlParser.append_to_bq(df_batch, bq_export)

        # Clean up memory
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
        # Check arguments are of the correct type
        if not isinstance(filepath, str):
            raise TypeError(
                "'filepath' variable must be passed as a string"
            )

        # Check arguments take acceptable values
        if not self.fs.exists(filepath):
            raise ValueError(
                "The specified file path does not exist"
            )
        # Could add check for whether the file path is an iXBRL file

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

    def create_directory_list(self, months, filepath, year, custom_input="None"):
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

        # Check all arguments are of the correct types
        if not (
            isinstance(filepath, str) or
            isinstance(year, str) or
            isinstance(custom_input, str)
        ):
            raise TypeError(
                "'filepath', 'year' and 'custom_input' arguments must all be "
                "passed as strings"
            )
        if not all(isinstance(month, str) for month in months):
            raise TypeError(
                "All months in 'months' argument must be passed as strings"
            )

        # Check all arguments have acceptable values
        valid_months = ['January', 'February', 'March', 'April',
                        'May', 'June', 'July', 'August',
                        'September', 'October', 'November', 'December']
        if not all(month in valid_months for month in months):
            raise ValueError(
                "Invalid entries in 'month' argument"
            )
        if not self.fs.exists(filepath):
            raise ValueError(
                "The specified file path does not exist"
            )

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

    def parse_directory(self, directory, bq_location,
                        processed_path, num_processes=1):
        """
        Takes a directory, parses all files contained there and saves them as
        csv files in a specified directory.

        Arguments:
            directory:      A directory (path) to be processed (str)
            bq_location:    Location of BigQuery table to save results (str)
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
        # files = files[0:600]

        # TO BE COMMENTED OUT AFTER TESTING
        print(folder_month, folder_year)

        # Define the location where to export results to BigQuery
        table_export = bq_location + "." + folder_month + "-" + folder_year

        # Create a BigQuery table
        self.mk_bq_table(table_export)

        # Code needed to split files by the number of cores before passing in
        # as an argument
        chunk_len = math.ceil(len(files) / num_processes)
        files = [files[i:i + chunk_len] for i in
                 range(0, len(files), chunk_len)]

        # define number of processors
        pool = mp.Pool(processes=num_processes)

        # Create a partial function so that pool.map only takes one argument
        build_month_table_partial = partial(self.build_month_table,
                                            table_export)
        fails = pool.map(build_month_table_partial, files)

        pool.close()
        pool.join()

        # combine resultant list of lists (of failed files)
        fails = [item for sublist in fails for item in sublist]
        print(fails)

        # Retry failed files without multiprocessing
        self.build_month_table(table_export, fails)

        # Export the BigQuery table to a csv file
        self.export_csv(table_export, processed_path,
                        folder_month + "-" + folder_year + "_xbrl_data")

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
                    custom_input, bq_location, csv_dir, num_cores):
        """
        Parses a set of accounts for a given time period and saves as a csv in
        a specified location.

        Arguments:
            quarter:            quarter of the given year to process files from (int)
            year:               year to process files from (int)
            unpacked_files:     path of directory where files to be processed
                                are stored (string)
            bq_location:        Location of BigQuery table to save results (str)
            csv_dir:            GCS folder to save csv file (str)
            custom_input:       Used to set a specific folder of accounts
            num_cores:          number of cores to use with mutliprocessing
                                module (int)
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
            self.parse_directory(directory, bq_location, csv_dir, num_cores)

    def build_month_table(self, bq_export, list_of_files):
        """
        Function which parses, sequentially, a list of xbrl/ html files,
        converting each parsed file into a dictionary and appending to a list.

        Arguments:
            bq_export:      Location in BigQuery where (new) table of data
                            should be constructed.
            list_of_files:  list of filepaths (gcs uri's), each coresponding to
                            a xbrl/html file (list)

        Returns:
            results:       list of dictionaries, each containing the parsed content of
                           a xbrl/html file (list)
        Raises:
            None
        """
        # Check arguments are of the correct type
        if not all(isinstance(file, str) for file in list_of_files):
            raise TypeError(
                "All files in 'list_of_files' must be specified as strings"
            )

        # Check arguments have acceptable values
        if not all(self.fs.exists(file) for file in list_of_files):
            raise ValueError(
                "Not all file paths specified exist"
            )

        process_start = time.time()

        # Empty list awaiting results
        results = []
        fails = []

        start_memory = psutil.virtual_memory().percent

        # Set a threshold on the number of files to store in memory before
        # uploading to BigQuery
        file_threshold = 7000
        print("Start memory usuage: ", start_memory)

        # Initialise counters
        COUNT = 0
        file_count = 0
        row_count = 0
        batch_count = 0

        # For every file
        for file in list_of_files:
            COUNT += 1
            try:
                # file_count is the count per batch, COUNT is the overall file
                # count
                file_count += 1
                # Read the file and parse
                doc = self.process_account(file)

                # append results to table
                results.append(doc)

            # If we can't process the file, save it to be re done on one
            # processor
            except:
                print(file, "has failed to parse")
                fails.append(file)
                continue

            # If the number of files exceeds the threshold or we have done the
            # last file
            if (file_count > file_threshold) \
                    or COUNT == len(list_of_files):
                # Reset the file_count and add to the row_count
                file_count = 0

                # This also performs the BigQuery export
                row_count += XbrlParser.flatten_data(results, bq_export)
                XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT,
                                           len(list_of_files), row_count,
                                           batch_count,
                                           psutil.virtual_memory().percent,
                                           uploading=True,
                                           bar_length=50,
                                           width=20)
                batch_count += 1

                # Delete results and free up memory
                del results
                results = []
                gc.collect()

            XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT,
                                       len(list_of_files), row_count,
                                       batch_count,
                                       psutil.virtual_memory().percent,
                                       bar_length=50, width=20)

        print(
            "Average time to process an XBRL file: \x1b[31m{:0f}\x1b[0m".format(
                (time.time() - process_start) / 60, 2), "minutes")

        return fails

    @staticmethod
    def append_to_bq(df, table):
        """
        Function to append a given DataFrame to a BigQuery table

        Arguments:
            df:     Pandas DataFrame output with columns of the correct types
            table:  Location of BigQuery table, in form "<dataset>.<table_name>"
        Returns:
            None
        Raises:
            None

        """
        # Set up a BigQuery client
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            # Set the schema types to match those in parsed_data_schema.txt
            schema=[
                bigquery.SchemaField("doc_companieshouseregisterednumber",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("parsed",
                                     bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("doc_balancesheetdate",
                                     bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("doc_upload_date",
                                     bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("doc_standard_date",
                                     bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("name",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("unit",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("value",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_name",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_type",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("arc_name",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_standard_type",
                                     bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("doc_standard_link",
                                     bigquery.enums.SqlTypeNames.STRING)
            ],
            # Append to table (rather than overwrite)
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        # Make an API request.
        job = client.load_table_from_dataframe(
            df, table, job_config=job_config
        )
        # Wait for the job to complete.
        job.result()
        # Free memory of job
        job = 0
        del job

    @staticmethod
    def mk_bq_table(bq_location, schema="parsed_data_schema.txt"):
        """
        Function to create a BigQuery table in a specified location with a
        schema specified by a txt file.

        Arguments:
            bq_location:    Location of BigQuery table, in form
                            "<dataset>.<table_name>"
            schema:         File path of schema specified as txt file
        Returns:
            None
        Raises:
            None
        """
        # Create a BigQuery client
        client = bigquery.Client()

        # Check if table exists
        try:
            client.get_table(bq_location)
            table_exists = True
        except:
            table_exists = False

        if table_exists:
            raise ValueError("Table already exists, please remove and retry")

        # Create the table using the command line
        bq_string = "bq mk --table " + bq_location + " " + schema
        os.popen(bq_string).read()

    def export_csv(self, bq_table, gcs_location, file_name):
        """
        Takes a specified BigQuery table and saves it as a single csv file
        (creates multiple csvs that partition the table as intermidiate steps)

        Arguments:
            bq_table:       Location of BigQuery table, in form
                            "<dataset>.<table_name>"
            gcs_location:   The folder in gcs where resulting csv should be
                            saved - "gs://" prefix should NOT be included
            file_name:      The name of resulting csv file - ".csv" suffix
                            should NOT be included
        Returns:
            None
        Raises:
            None
        """
        # Create BigQuery client
        client = bigquery.Client()

        # Don't include table header (will mess up combing csvs otherwise)
        job_config = bigquery.job.ExtractJobConfig(print_header=False)

        # Extract table into multiple smaller csv files
        extract_job = client.extract_table(
            bq_table,
            "gs://" + gcs_location + "/" + file_name + "*.csv",
            location="europe-west2",
            job_config=job_config
        )
        extract_job.result()

        # Recreate the header as a single df with just the header row
        header = pd.DataFrame(columns=['date', 'name', 'unit', 'value',
                                       'doc_name', 'doc_type',
                                       'doc_upload_date', 'arc_name', 'parsed',
                                       'doc_balancesheetdate',
                                       'doc_companieshouseregisterednumber',
                                       'doc_standard_type',
                                       'doc_standard_date', 'doc_standard_link'],)
        header.to_csv("gs://" + gcs_location + "/header_" + file_name + ".csv",
                      header=True, index=False)

        # Specify the files to be combined
        split_files = [f.split("/", 1)[1] for f in self.fs.ls(gcs_location)
                       if (f.split("/")[-1]).startswith("header_" + file_name)] +\
                      [f.split("/", 1)[1] for f in self.fs.ls(gcs_location)
                       if (f.split("/")[-1]).startswith(file_name)]

        # Set up a gcs storage client and locations for things
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_location.split("/", 1)[0])
        destination = bucket.blob(gcs_location.split(
            "/", 1)[1] + "/" + file_name + ".csv")
        destination.content_type = "text/csv"

        # Combine all the specified files
        sources = [bucket.get_blob(f) for f in split_files]
        destination.compose(sources)

        # Remove the intermediate files
        self.fs.rm([f for f in self.fs.ls(gcs_location)
                    if ((f.split("/")[-1]).startswith(file_name + "0")) or
                    ((f.split("/")[-1]).startswith("header_" + file_name))
                    ])
