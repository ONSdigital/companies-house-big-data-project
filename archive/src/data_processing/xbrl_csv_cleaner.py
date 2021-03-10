import pandas as pd
import csv
import os

class XbrlCSVCleaner:
    """
    Class to facilitate the cleaning of csv files outputted from the
    xbrl_parsing module.
    """

    def __init__(self):
        self.__init__

    @staticmethod
    def col_cast(df, col, col_type):
        """
        Converts a pandas dataframe column or list of columns into
        a specified type.

        Arguments:
            df:         A pandas dataframe
            col:        A list of columns to be converted from the dataframe
            col_type:   The type to convert the column to (from type specified
                        in allowed_types)
        Returns:
            df: The dataframe after the conversion has been applied
        Raises:
            None
        """

        # can include all standard types
        allowed_types = ['str', 'datetime64[ns]', 'int64', 'int32', 'bool']

        # convert col to list if string
        if type(col) is str:
            col = [col]
        else:
            pass
        # loop over inputted columns and apply type
        for c in col:
            if col_type == 'date':
                df[c] = df[c].astype('datetime64[ns]').dt.date

            elif col_type in allowed_types:
                df[c] = df[c].astype(col_type)
            df = df
        return df

    @staticmethod
    def parsed_csv_clean(import_path, export_path=''):
        """
        cleans a parsed xbrl csv file (from the xbrl_parsed_data storage)
        removing the index, removing unwanted string character from the value
        column and casting string and date(time) columns.

        Arguments:
            import_path: the filepath to the parsed xbrl csv file
            export_path: the filepath to export the cleaned csv file
        Returns:
            None
        Raises:
            None
        """

        # parameters
        unwanted_chars = ['  ', '"', '\n']
        str_cols = ['name', 'unit', 'value', 'doc_name', 'doc_type',
                    'arc_name',
                    'doc_companieshouseregisterednumber', 'doc_standard_type',
                    'doc_standard_link', ]
        date_cols = ['date', 'doc_balancesheetdate',
                     'doc_standard_date', 'doc_upload_date']

        wanted_cols = ['date', 'name', 'unit', 'value', 'doc_name', 'doc_type',
                       'doc_upload_date', 'arc_name', 'parsed',
                       'doc_balancesheetdate',
                       'doc_companieshouseregisterednumber',
                       'doc_standard_type',
                       'doc_standard_date', 'doc_standard_link', ]

        # turn all to string for now - redo when dates sorted (March 2015
        # example)
        str_cols = str_cols + date_cols

        # import data
        df = pd.read_csv(import_path, lineterminator="\n", dtype="str")

        # remove unwanted chars
        for char in unwanted_chars:
            df.value = df.value.str.replace(char, '')

        # limit to desired columns
        df = df[wanted_cols]

        # cast columns
        # df = XbrlCSVCleaner.col_cast(df, str_cols, 'str')
        df["value"] = df["value"] \
            .astype('str')
        # df = col_cast(df, date_cols, 'datetime64[ns]')

        # export cleaned dataframe
        if export_path == '':
            export_path = import_path
        # write to csv with no index, tab deliminated, and quoting all non
        # numeric data

        print('Exporting cleaned dataframe to {}'.format(export_path))
        df.to_csv(export_path, index=False, sep=",", line_terminator='\n',
                  quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

        return None

    @staticmethod
    def clean_parsed_files(import_directory, export_directory):
        """
        Cleans all .csv files in a given directory using methods in the
        XbrlCSVCleaner class and saves the processed files in a given directory

        Arguments:
            import_directory: Directory containing .csv files to be cleaned
            export_directory: Directory where cleaned files should be saved
        Returns:
            None
        Raises:
            None
        """
        #path = '/shares/xbrl_parsed_data/'
        # Generate a list of files to be cleaned
        os.chdir(import_directory)
        xbrl_files = os.listdir(import_directory)
        xbrl_files = [csv for csv in xbrl_files if csv.endswith('.csv')]

        # Clean the parsed files from the relevant list
        for file in xbrl_files:
            print('Exporting {}......'.format(file))
            XbrlCSVCleaner.parsed_csv_clean(import_directory + file,
                                            export_directory + file)
            print('Successfully exported {}!'.format(file))
        return None

if __name__ == "__main__":
    import gcsfs
    pd.set_option('display.max_colwidth', None)
    pd.set_option('max_columns', None)

    in_path = "gs://ons-companies-house-dev-test-parsed-csv-data/v2_parsed_data/2015-September_xbrl_data.csv"
    out_path = "gs://ons-companies-house-dev-test-parsed-csv-data/v2_parsed_data/2015-September_xbrl_data_cleaned.csv"

    cleaner = XbrlCSVCleaner()

    df = cleaner.parsed_csv_clean(in_path, export_path=out_path)