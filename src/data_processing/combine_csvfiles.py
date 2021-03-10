import os
import csv
import pandas as pd
from typing import List
from datetime import datetime
import gcsfs


class XbrlCsvAppender:
    """ This class combines the csvs. """

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    def combine_csv(self, files: List[str], outfile: str, separator=","):
        """
        Combines all csv files listed in files into a single csv file using

        csv.writer. Unsuitable if files do not have the same format. Faster
        than the pandas version.

        Arguments:
            files:      List of csv files specified by user (list)
            outfile:    Filepath to write combined csv file (str)
        Returns:
            None
        Raises:
            None
        """
        # Check arguments are of correct types
        if not all(isinstance(file, str) for file in files):
            raise TypeError(
                "The csv paths you have specified need to be passed as \
                strings"
            )
        if not isinstance(outfile, str):
            raise TypeError(
                "The output filepath needs to be specified as a string"
            )

        # Check file paths are valid
        if not all(os.path.exists(file) for file in files):
            raise ValueError(
                "Not all of the input csv files have valid file paths"
            )
        if not all(file[-4:] == ".csv" for file in files):
            raise ValueError(
                "Not all of the input files are csv's."
            )

        print("Processing " + str(len(files)) + " files...")
        with self.fs.open(outfile, "wt") as w:
            writer = csv.writer(w, delimiter=separator)
            for n, file in enumerate(files):
                print("Processing " + file + "...")
                with self.fs.open(file, "r") as f:
                    reader = csv.reader(f, delimiter=separator)
                    if n > 0:
                        next(reader)
                    for row in reader:
                        writer.writerow(row)
        self.fs.setxattrs(outfile, content_type="text/csv")

    def combine_csv_pd(self, files: List[str], outfile: str):
        """
        Combines all csv files listed in files into a single csv file using
        pandas.

        Arguments:
            files:     List of the file paths of the files to be combined
                       (list)
            outfile:   Filepath to write combined csv file (str)
        Returns:
            None
        Raises:
            None
        """
        # Check arguments are of correct types
        if not all(isinstance(file, str) for file in files):
            raise TypeError(
                "The csv paths you have specified need to be passed as \
                strings"
            )
        if not isinstance(outfile, str):
            raise TypeError(
                "The output filepath needs to be specified as a string"
            )

        # Check file paths are valid
        if not all(os.path.exists(file) for file in files):
            raise ValueError(
                "Not all of the input csv files have valid file paths"
            )
        if not all(file[-4:] == ".csv" for file in files):
            raise ValueError(
                "Not all of the input files are csv's."
            )

        # WARNING: Causes memory overuse for large files
        combined_csv = pd.concat([pd.read_csv("gs://"+f,
                                              index_col=None,
                                              header=0,
                                              sep=",",
                                              lineterminator="\t",
                                              quotechar='"',
                                              low_memory=False)
                                  for f in files])
        combined_csv.to_csv("gs://"+outfile, index=False)

    @staticmethod
    def _add_path(indir: str, files: str):
        """
        Returns a list with the paths appended to file names.

        Arguments:
            indir:   List of csv files specified by user (list)
            files:   Filepath to write combined csv file (str)
        Returns:
            None
        Raises:
            None
        """

        # Check arguments are of the correct types
        if not isinstance(indir, str):
            raise TypeError(
                "input directory must be specified as a string"
            )
        if not all(isinstance(file, str) for file in files):
            raise TypeError(
                "All files must be specified as a string"
            )

        # Check arguments are the correct values
        if not os.path.exists(indir):
            raise ValueError(
                "Specified input directory does not exist"
            )

        return [indir+i for i in files]

    def merge_files_by_year(self, indir: str, outdir: str,
                            year: str, quarter=""):
        """
        Finds all csv files with directory indir, groups files into
        individual years and calls combine_csv function to merge into
        single file given that the years falls within the range specified
        by the user.

        Arguments:
            indir:      Path of directory to search for csv files in (str)
            outdir:     Path of directory to write resulting files (str)
            year:       Find all files from this year (str)
            quarter:    Find all files from this quarter (int)
        Returns:
            None
        Raises:
            None
        """
        # Check arguments are of correct types
        if not (
            isinstance(indir, str) or
            isinstance(outdir, str) or
            isinstance(year, str) or
            isinstance(quarter, str)
        ):
            raise TypeError(
                "All arguments must be passed as strings"
            )

        # Check arguments have valid values
        if not(
            os.path.exists(indir) or
            os.path.exists(outdir)
        ):
            raise ValueError(
                "Invalid file path entered"
            )

        year = int(year)
        if quarter == "None":
            quarter = None
        else:
            quarter = int(quarter)

        # If the input directory is valid...
        if self.fs.exists(indir):

            # Create the output folder if it doesn't exist
            # if not os.path.exists(outdir):
            #     os.mkdir(outdir)

            # Get all csv files which contain the year specified in their
            # filenames
            files = self.fs.ls(indir)
            files = ([i for i in files if i[-4:] == '.csv'
                      and i.split("/")[-1][:4].isnumeric()
                      and int(i.split("/")[-1][:4]) == year])

            # Sort all files by the month contained in the file name
            files = sorted(files,
                           key=lambda day: datetime.strptime(
                               day.split("/")[-1].split("-")[1].split("_")[0],
                               "%B"))

            # Create a list of months based on what quarter in the year has
            # been specified
            if quarter == 1:
                quarter_list = ['January', 'February', 'March']
            elif quarter == 2:
                quarter_list = ['April', 'May', 'June']
            elif quarter == 3:
                quarter_list = ['July', 'August', 'September']
            elif quarter == 4:
                quarter_list = ['October', 'November', 'December']
            else:
                quarter_list = []

            # Filter out those files not in the specified quarter,
            # if a quarter has been specified
            if quarter != None:
                files = [f for f in files
                         if f.split("/")[-1].split("-")[1].split("_")[0]
                         in quarter_list]
                quarter_str = "q" + str(quarter)
            else:
                quarter_str = ""

            # Prepend input directory to filenames
            # files = [indir + f for f in files]

            # Combine the csvs
            # XbrlCsvAppender.combine_csv(files, outdir + str(year)
            #                                 + quarter_str + '_xbrl.csv')

            self.combine_csv(files, outdir + "/" + str(year)
                             + quarter_str + '_xbrl.csv',
                             separator="\t")
        else:
            print("Input file path does not exist")


if __name__ == '__main__':

    indir = 'ons-companies-house-dev-parsed-csv-data'
    outdir = 'ons-companies-house-dev-outputs-data/appender_test'
    year = 2010
    quarter = "None"

    fs = gcsfs.GCSFileSystem("ons-companies-house-dev",
                             token="/home/dylan_purches/Desktop/data_key.json",
                             cache_timeout=1)

    appender = XbrlCsvAppender(fs)
    appender.merge_files_by_year(indir, outdir, year, quarter)
