from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
from datetime import datetime
from dateutil import parser
import pandas as pd
import os

#For testing only
import csv
import time
import sys


class XbrlParser:
    """ This is a class for parsing the XBRL data."""

    def __init__(self):
        """
        Constructs all the necessary attributes for the XbrlParser object of which there
        are none.
        """
        self.__init__
        
    # Table of variables and values that indicate consolidated status
    consolidation_var_table = {
        "includedinconsolidationsubsidiary": True,
        "investmententityrequiredtoapplyexceptionfromconsolidationtruefalse": True,
        "subsidiaryunconsolidatedtruefalse": False,
        "descriptionreasonwhyentityhasnotpreparedconsolidatedfinancialstatements": "exist",
        "consolidationpolicy": "exist"
    }

    @staticmethod
    def clean_value(string):
        """
        Take a value that is stored as a string, clean it and convert to numeric.
        If it's just a dash, it is taken to mean zero.

        Returns the string argument as an int.

        Keyword arguments:
        string -- The string to be converting to a numeric value
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

        Returns a text string.

        Keyword arguments:
        soup -- BeautifulSoup souped html/xml object
        contextref -- the id of the context element to be raided
        """
        try:
            context = soup.find("xbrli:context", id=contextref)
            contents = context.find("xbrldi:explicitmember").get_text().split(":")[-1].strip()
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
        on the way these links are formatted and referenced, within this
        function.  Might need changing someday.

        Returns a 3-tuple (standard, date, original url).

        Keyword arguments:
        soup -- BeautifulSoup souped html/xml object
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
        return text[:-10].strip("-"), text[-10:], link_obj['xlink:href']

    @staticmethod
    def retrieve_unit(soup, each):
        """
        Gets the reporting unit by trying to chase a unitref to
        its source, alternatively uses element attribute unitref
        if it's not a reference to another element.

        Returns the unit

        Keyword arguments:
        soup -- BeautifulSoup souped html/xml object
        each -- element of BeautifulSoup souped object
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

        Returns the date

        Keyword arguments:
        soup -- BeautifulSoup souped html/xml object
        each -- element of BeautifulSoup souped object
        """
        # Try to find a date tag within the contextref element, starting with the most
        # specific tags, and starting with those for ixbrl docs as it's the most common file.
        date_tag_list = ["xbrli:enddate",
                         "xbrli:instant",
                         "xbrli:period",
                         "enddate",
                         "instant",
                         "period"]

        for tag in date_tag_list:
            try:
                date_str = each['contextref']
                date_val = parser.parse(soup.find(id=each['contextref']).find(tag).get_text()). \
                    date(). \
                    isoformat()
                return (date_val)
            except:
                pass

        try:
            date_str = each.attrs['contextref']
            date_val = parser.parse(each.attrs['contextref']). \
                date(). \
                isoformat()
            return (date_val)
        except:
            pass

        return "NA"

    @staticmethod
    def parse_element(soup, element):
        """
        For a discovered XBRL tagged element, go through, retrieve its name
        and value and associated metadata.

        Returns a dictionary.

        Keyword arguments:
        soup -- BeautifulSoup object of accounts document
        element -- soup object of discovered tagged element
        """
        if "contextref" not in element.attrs:
            return {}

        element_dict = {}

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

        # If there's no value retrieved, try raiding the associated context data
        if element_dict['value'] == "":
            element_dict['value'] = XbrlParser.retrieve_from_context(soup, element.attrs['contextref'])

        # If the value has a defined unit (eg a currency) convert to numeric
        if element_dict['unit'] != "NA":
            element_dict['value'] = XbrlParser.clean_value(element_dict['value'])

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
        them.  Only keep valid results (test is whether field "name"
        exists).

        Keyword arguments:
        element_set -- BeautifulSoup iterable search result object
        soup -- BeautifulSoup object of accounts document
        """
        elements = []
        for each in element_set:
            element_dict = XbrlParser.parse_element(soup, each)
            if 'name' in element_dict:
                elements.append(element_dict)

        return elements

    @staticmethod
    def summarise_by_sum(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        a summary variable relating to the financial state of the enterprise
        by summing all those named that exist.

        Returns a dictionary.

        Keyword arguments:
        doc -- an extracted document dict, with "elements" entry as created
               by the 'scrape_clean_elements' functions
        variable_names -- variables to find and sum if they exist
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
                total_assets = total_assets + df[df['name'] == each].iloc[0]['value']
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

        Returns a dictionary.

        Keyword arguments:
        doc -- an extracted document dict, with "elements" entry as created
               by the 'scrape_clean_elements' functions
        variable_names -- variables to find and check if they exist
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

        Returns a dictionary.

        Keyword arguments:
        doc -- an extracted document dict, with "elements" entry as created
               by the 'scrape_clean_elements' functions.
        variable_names -- variables to find and return if they exist.
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
        Parses an XBRL (xml) company accounts file for all labelled content and extracts the
        content (and metadata, eg; unitref) of each element found to a dictionary

        Returns a lit of dictionaries.

        Keyword arguments:
        filepath -- A filepath string
        """
        # Try multiple methods of retrieving data, I think only the first is
        # now needed though.  The rest will be removed after testing this
        # but should not affect execution speed.
        try:
            element_set = soup.find_all()
            elements = XbrlParser.parse_elements(element_set, soup)
            if len(elements) <= 5:
                raise Exception("Elements should be gte 5, was {}".format(len(elements)))
        except:
            # if fails parsing create dummy entry elements so entry still exists in dictionary
            elements = [{'name': 'NA', 'value': 'NA', 'unit': 'NA', 'date': 'NA', 'sign': 'NA'}]
            pass
        return elements

    @staticmethod
    def flatten_dict(doc):
        """
        NEED TO CHANGE TO REFLECT NEW FUNCTIONALITY - IF WORKING
        Takes in a list of dictionaries and combines them into a
        single dictionary - assumes dictionaries all have same keys

        Returns a dictionary (doc_dict).

        Keyword argument:
        doc -- a list of dictionaries
        """
        # combines list of dictionaries into one dictionary based on common keys
        doc_dict = {}
        for k in doc[0].keys():
            doc_dict[k] = [d[k] for d in doc]

        return doc_dict

    @staticmethod
    def flatten_data(doc):
        """
        Takes the data returned by flatten dict, with its tree-like
        structure and reorganises it into a long-thin format table structure
        suitable for SQL applications.

        Keyword argument:
        doc -- a list of dictionaries
        """
        doc2 = doc.copy()

       #check if temp file is already present
        try:
            os.remove("/home/dylan_purches/Documents/Data/temp_exports/df_elements.csv")
        except:
            pass

        # loop over each file and create a separate dataframe
        # for each set (elements) of parsed tags, appending result to list
        md, hd = 'w', True

        T = len(doc2)
        t0 = time.time()
        for i in range(len(doc2)):
            #Turn each elements dict into a dataframe
            df_element = pd.DataFrame.from_dict(doc2[i]['elements'])

            #Ensure each element has the same number of columns
            if 'sign' not in df_element.columns.values:
                df_element['sign'] = 'NA'

            #Add a key
            df_element['key'] = i

            #Dump the "elements" entry in the doc dict
            doc2[i].pop('elements')
            df_element_meta = pd.DataFrame(doc2[i], index =[0])
            df_element_meta['key'] = i

            #Merge the metadata with the elements
            df_element_export = df_element_meta.merge(df_element, how='left', on='key')
            del df_element_meta, df_element
            df_element_export = df_element_export.drop('key', axis= 1)

            #Append the new element to a csv file stored on the disk
            df_element_export.to_csv(
                "/home/dylan_purches/Documents/Data/temp_exports/df_elements.csv",
                mode=md,
                header=hd,
                index=None,
                sep = ",",
                quotechar= '"'
            )

            if i % 100 == 0:
                print("%3.2f have been processed"%(i/T))
            md, hd = 'a', False

        # convert the stored csv back into a pandas df and tidy up
        df_elements = pd.read_csv(
            "/home/dylan_purches/Documents/Data/temp_exports/df_elements.csv",
            index_col=None,
            header=0,
            sep=",",
            lineterminator="\n",
            quotechar='"')
        os.remove("/home/dylan_purches/Documents/Data/temp_exports/df_elements.csv")

        return df_elements


    @staticmethod
    def process_account(filepath):
        """
        Scrape all of the relevant information from an iXBRL (html) file, upload
        the elements and some metadata to a mongodb.

        Keyword argument:
        filepath -- complete filepath (string) from drive root
        """
        doc = {}

        # Some metadata, doc name, upload date/time, archive file it came from
        doc['doc_name'] = filepath.split("/")[-1]
        doc['doc_type'] = filepath.split(".")[-1].lower()
        doc['doc_upload_date'] = str(datetime.now())
        doc['arc_name'] = filepath.split("/")[-2]

        # Complicated ones
        sheet_date = filepath.split("/")[-1].split(".")[0].split("_")[-1]
        doc['doc_balancesheetdate'] = datetime.strptime(sheet_date, "%Y%m%d").date().isoformat()

        doc['doc_companieshouseregisterednumber'] = filepath.split("/")[-1].split(".")[0].split("_")[-2]

        # loop over multithreading here - imports data and parses on seperate threads
        try:
            file = open(filepath)
            soup = BS(file, "lxml")
        except:
            print("Failed to open: " + filepath)
            return 1

        # Get metadata about the accounting standard used
        try:
            doc['doc_standard_type'],\
                doc['doc_standard_date'],\
                doc['doc_standard_link'] = XbrlParser.retrieve_accounting_standard(soup)
            doc['parsed'] = True
        except:
            doc['doc_standard_type'],\
                doc['doc_standard_date'],\
                doc['doc_standard_link'] = (0, 0, 0)
            doc['parsed'] = False

        # Fetch all the marked elements of the document
        try:
            doc['elements'] = XbrlParser.scrape_elements(soup, filepath)
        except Exception as e:
            doc['parsed'] = False
            doc['Error'] = e
        try:
            return doc
        except Exception as e:
            return e
