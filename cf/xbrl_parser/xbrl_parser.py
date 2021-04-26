from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
from datetime import datetime
from dateutil import parser
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import os
import csv
import time
import sys
import math
import numpy as np
import gcsfs
import pytz
import psutil
import gc

class XbrlParser:
    """ This is a class for parsing the XBRL data."""

    def __init__(self):
        self.__init__
        self.fs = gcsfs.GCSFileSystem(cache_timeout=0)
        self.t0 = time.time()

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

    def flatten_data(self, doc, bq_export):
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

        # Set up row counter and empty list to save DataFrames
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
                df_element_export["value"] = df_element_export["value"].astype(str)\
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
            df_element_export['doc_upload_date'] = df_element_export['doc_upload_date'].astype("str")
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
            
            self.append_to_bq(df_element_export, bq_export)

            # Free up memory
            del df_element_export

        
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
        except Exception as e:
            print("Failed to open: " + filepath)
            return ""

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
            print(e)
            return ""

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
        # if not self.fs.exists(filepath):
        #     raise ValueError(
        #         "The specified file path does not exist"
        #     )

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

    def parse_files(self, files_list, directory, table_export):
        """
        Takes a list of files and a directory, parses all files contained there 
        and exports them as a BigQuery table in a specified location.

        Arguments:
            files_list:     A list of all the files to be parsed [str]
            bq_location:    Location of BigQuery table to save results (str)
            processed_path: String of the path where processed files should be
                            saved (str)
        Returns:
            None
        Raises:
            None
        """
        # Process all the files in the list of files
        results, fails = self.combine_batch_data(files_list, directory)

        # Retry unparsed files
        if len(fails) > 0:
            # Check enough time is left
            if time.time() - self.t0 < 420:
                # Retry failed files
                results0, fails0 = self.combine_batch_data(fails, directory)
                results += results0
                fails = fails0
            print(f"{fails} files did not parse")

        # Combine the results and upload them to BigQuery
        self.flatten_data(results, table_export)

        return None

   
    def combine_batch_data(self, filenames, directory):
        """
        For each xbrl file in a given list of file names, try to process
        it and append the result to a list.
        
        Arguments
            filenames:  List of strings of the full GCS file path of the
                        xbrl files to be processed
        Returns
            results:    List of dicts containing the data from files that have
                        been successfully processed
            fails:      List of the filepaths of all xbrl files that failed to
                        be processed.
        Raises
            None
        """
        results = []
        fails = []

        # Loop over all the files listed
        for file in filenames:
            filepath = directory + file
            if self.fs.exists(filepath):
                # If the file exists try and process it
                try:
                    # Read the file and parse
                    doc = self.process_account(filepath)

                    # Will return an emtpy string if it fails
                    if len(doc) > 0: 
                        # append results to table
                        results.append(doc)
                    else:
                        print(filepath, "has failed to parse")
                        fails.append(filepath)

                # If we can't process the file, save it to be re done later
                except:
                    print(filepath, "has failed to parse")
                    fails.append(filepath)
                    continue

            else:
                print(f"{filepath} does not exist")
        
        return results, fails


    def append_to_bq(self, df, table):
        """
        Function to append a given DataFrame to a BigQuery table using
        streaming insert method.

        Arguments:
            df:     Pandas DataFrame output with columns of the correct types
            table:  Location of BigQuery table, in form "<dataset>.<table_name>"
        Returns:
            None
        Raises:
            None

        """
        # Set up a BigQuery client
        client = bigquery.Client(project="ons-companies-house-dev")

        # Convert Dataframe columns to string so they are JSON serializable
        df = df.astype(str)
        
        # Make an API request.
        errors = client.insert_rows_json(
            table, df.to_dict('records'), skip_invalid_rows=False
            )
        # Print errors if any are returned
        if len(errors) > 0:
            try:
                doc_name = df["doc_name"][0]
            except:
                doc_name = "Unkown"
            print(f"Errors from bq upload for {doc_name}: {errors}")
 