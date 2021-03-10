from os import listdir, walk
from os.path import isfile, exists, getsize, getmtime, join
from datetime import datetime
import zipfile
import gcsfs


class XbrlValidatorMethods:
    """This is a class that validates the XBRL data files."""

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    def validate_compressed_files(self, filepath):
        """
        Validates and prints information, including name size and date
        modified, on files in specified directory

        Arguments:
            filepath: source directory of files of interest (str)
        Returns:
            None
        Raises:
            None
        """
        # Check argument is of the correct type
        if not isinstance(filepath, str):
            raise TypeError(
                "The output filepath needs to be specified as a string"
            )
        # Check input is a valid file path
        if not exists(filepath):
            raise ValueError(
                "The file path specified does not exist"
            )

        if self.fs.exists(filepath):
            if not self.fs.isfile(filepath):

                files = self.fs.ls(filepath)
                files = [f for f in files if f.split(".")[-1] == "zip"]
                #[filepath + "/" + f for f in listdir(filepath)]

                for file in files:
                    gcs_file = self.fs.open(file, 'rb')
                    print("File: " + file)
                    print("File size: " +
                          str(gcs_file.info()["size"]) + " bytes")

                    #file_time = datetime.utcfromtimestamp(gcs_file.info()["updated"])
                    print("File modified: "
                          + gcs_file.info()["updated"])
            else:
                print("Specified filepath is not a directory!")

        else:
            print("Specified directory does not exist!")

    @staticmethod
    def validate_extracted_files(filepath_to_zip_files,
                                 filepath_to_extracted_files):
        """
        Validates that zip files in specified directory have been correctly
        extracted to another specified directory.
        To be used for validating files which have been extracted from a
        zip file.

        Arguments:
            filepath_to_zip_files:        source directory of files of interest
                                          (str)
            filepath_to_extracted_files:  destination directory of files of
                                          interest (str)
        Returns:
            None
        Raises:
            None
        """
        # Check argument is of the correct type
        if not (
                isinstance(filepath_to_zip_files, str) or
                isinstance(filepath_to_extracted_files)):
            raise TypeError(
                "File paths need to be specified as strings"
            )

        # Check input is a valid file path
        if not (
                exists(filepath_to_zip_files) or
                exists(filepath_to_extracted_files)):
            raise ValueError(
                "Specified a file path that doesn't exist"
            )

        def get_size_of_folder(start_path):
            """
            Recursive method to return the combined size of a folder and its
            sub-directories

            Arguments:
                start_path: directory to be checked (str)
            Returns:
                total_size: combined size of the directory and its
                            sub-directories in bytes (int)
            Raises:
                None
            """
            # Check argument is of the correct type
            if not isinstance(start_path, str):
                raise TypeError(
                    "File paths need to be specified as strings"
                )

            # Check input is a valid file path
            if not exists(start_path):
                raise ValueError(
                    "Specified a file path that doesn't exist"
                )
            total_size = 0
            for path, dirs, files in walk(start_path):
                for f in files:
                    fp = join(path, f)
                    total_size += getsize(fp)

            return total_size

        zip_files = [filepath_to_zip_files + "/" + f
                     for f in listdir(filepath_to_zip_files)]
        extracted_files = [filepath_to_extracted_files + "/" + f
                           for f in listdir(filepath_to_extracted_files)
                           if not isfile(f)]

        zip_files.sort()
        extracted_files.sort()

        for z, e in zip(zip_files, extracted_files):

            print("Validating zip file: " + z)
            print("Validating extracted folder: " + e)

            # Zip file -> extracted folder size comparison
            zip_size = getsize(z)
            extracted_folder_size = get_size_of_folder(e)

            print("Zip file size: " + str(zip_size) + " bytes")
            print("Extracted folder size: " + str(extracted_folder_size)
                  + " bytes")
            if extracted_folder_size >= zip_size:
                print("Extracted folder size larger than original zip file\
                - size check OK")
            else:
                print("Extracted folder size smaller than original \
                zip file - size check failed!")

            # Check to see if all files have been extracted correctly
            files_in_zip = set(zipfile.ZipFile(z).namelist())
            files_in_extracted_folder = set([f for f in listdir(e)])
            diff = files_in_zip.difference(files_in_extracted_folder)

            if not diff:
                print("All files extracted correctly from zip file")
            else:
                print("Some files not extracted correctly from zip file!:")
                print(str(diff))
